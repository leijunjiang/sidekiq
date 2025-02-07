# frozen_string_literal: true

$stdout.sync = true

require "yaml"
require "singleton"
require "optparse"
require "erb"
require "fileutils"

require "sidekiq"
require "sidekiq/launcher"
require "sidekiq/util"

module Sidekiq
  class CLI
    include Util
    include Singleton unless $TESTING

    attr_accessor :launcher
    attr_accessor :environment

    def parse(args = ARGV)
      setup_options(args)
      initialize_logger
      validate!
    end

    def jruby?
      defined?(::JRUBY_VERSION)
    end

    # Code within this method is not tested because it alters
    # global process state irreversibly.  PRs which improve the
    # test coverage of Sidekiq::CLI are welcomed.
    def run
      boot_system
      if environment == "development" && $stdout.tty? && Sidekiq.log_formatter.is_a?(Sidekiq::Logger::Formatters::Pretty)
        print_banner
      end

      self_read, self_write = IO.pipe
      sigs = %w[INT TERM TTIN TSTP]
      sigs.each do |sig|
        trap sig do
          self_write.write("#{sig}\n")
        end
      rescue ArgumentError
        puts "Signal #{sig} not supported"
      end

      logger.info "Running in #{RUBY_DESCRIPTION}"
      logger.info Sidekiq::LICENSE
      logger.info "Upgrade to Sidekiq Pro for more features and support: http://sidekiq.org" unless defined?(::Sidekiq::Pro)

      # touch the connection pool so it is created before we
      # fire startup and start multithreading.
      ver = Sidekiq.redis_info["redis_version"]
      raise "You are using Redis v#{ver}, Sidekiq requires Redis v2.8.0 or greater" if ver < "2.8"
      logger.warn "Sidekiq 6.0 requires Redis 4.0+, you are using Redis v#{ver}" if ver < "4"

      # Since the user can pass us a connection pool explicitly in the initializer, we
      # need to verify the size is large enough or else Sidekiq's performance is dramatically slowed.
      cursize = Sidekiq.redis_pool.size
      needed = Sidekiq.options[:concurrency] + 2
      raise "Your pool of #{cursize} Redis connections is too small, please increase the size to at least #{needed}" if cursize < needed

      # cache process identity
      Sidekiq.options[:identity] = identity

      # Touch middleware so it isn't lazy loaded by multiple threads, #3043
      Sidekiq.server_middleware

      # Before this point, the process is initializing with just the main thread.
      # Starting here the process will now have multiple threads running.
      fire_event(:startup, reverse: false, reraise: true)

      logger.debug { "Client Middleware: #{Sidekiq.client_middleware.map(&:klass).join(", ")}" }
      logger.debug { "Server Middleware: #{Sidekiq.server_middleware.map(&:klass).join(", ")}" }

      launch(self_read)
    end

    def launch(self_read)
      if environment == "development" && $stdout.tty?
        logger.info "Starting processing, hit Ctrl-C to stop"
      end

      @launcher = Sidekiq::Launcher.new(options)

      begin
        launcher.run

        while (readable_io = IO.select([self_read]))
          signal = readable_io.first[0].gets.strip
          handle_signal(signal)
        end
      rescue Interrupt
        logger.info "Shutting down"
        launcher.stop
        # Explicitly exit so busy Processor threads can't block
        # process shutdown.
        logger.info "Bye!"
        exit(0)
      end
    end

    def self.w
      "\e[37m"
    end

    def self.r
      "\e[31m"
    end

    def self.b
      "\e[30m"
    end

    def self.reset
      "\e[0m"
    end

    def self.banner
      %{
      #{w}         m,
      #{w}         `$b
      #{w}    .ss,  $$:         .,d$
      #{w}    `$$P,d$P'    .,md$P"'
      #{w}     ,$$$$$b#{b}/#{w}md$$$P^'
      #{w}   .d$$$$$$#{b}/#{w}$$$P'
      #{w}   $$^' `"#{b}/#{w}$$$'       #{r}____  _     _      _    _
      #{w}   $:     ,$$:      #{r} / ___|(_) __| | ___| | _(_) __ _
      #{w}   `b     :$$       #{r} \\___ \\| |/ _` |/ _ \\ |/ / |/ _` |
      #{w}          $$:        #{r} ___) | | (_| |  __/   <| | (_| |
      #{w}          $$         #{r}|____/|_|\\__,_|\\___|_|\\_\\_|\\__, |
      #{w}        .d$$          #{r}                             |_|
      #{reset}}
    end

    SIGNAL_HANDLERS = {
      # Ctrl-C in terminal
      "INT" => ->(cli) { raise Interrupt },
      # TERM is the signal that Sidekiq must exit.
      # Heroku sends TERM and then waits 30 seconds for process to exit.
      "TERM" => ->(cli) { raise Interrupt },
      "TSTP" => ->(cli) {
        Sidekiq.logger.info "Received TSTP, no longer accepting new work"
        cli.launcher.quiet
      },
      "TTIN" => ->(cli) {
        Thread.list.each do |thread|
          Sidekiq.logger.warn "Thread TID-#{(thread.object_id ^ ::Process.pid).to_s(36)} #{thread.name}"
          if thread.backtrace
            Sidekiq.logger.warn thread.backtrace.join("\n")
          else
            Sidekiq.logger.warn "<no backtrace available>"
          end
        end
      },
    }

    def handle_signal(sig)
      Sidekiq.logger.debug "Got #{sig} signal"
      handy = SIGNAL_HANDLERS[sig]
      if handy
        handy.call(self)
      else
        Sidekiq.logger.info { "No signal handler for #{sig}" }
      end
    end

    private

    def print_banner
      puts "\e[31m"
      puts Sidekiq::CLI.banner
      puts "\e[0m"
    end

    def set_environment(cli_env)
      @environment = cli_env || ENV["RAILS_ENV"] || ENV["RACK_ENV"] || "development"
    end

    def symbolize_keys_deep!(hash)
      hash.keys.each do |k|
        symkey = k.respond_to?(:to_sym) ? k.to_sym : k
        hash[symkey] = hash.delete k
        symbolize_keys_deep! hash[symkey] if hash[symkey].is_a? Hash
      end
    end

    alias_method :die, :exit
    alias_method :☠, :exit

    def setup_options(args)
      # parse CLI options
      opts = parse_options(args)

      set_environment opts[:environment]

      # check config file presence
      if opts[:config_file]
        if opts[:config_file] && !File.exist?(opts[:config_file])
          raise ArgumentError, "No such file #{opts[:config_file]}"
        end
      else
        config_dir = if File.directory?(opts[:require].to_s)
          File.join(opts[:require], "config")
        else
          File.join(options[:require], "config")
        end

        %w[sidekiq.yml sidekiq.yml.erb].each do |config_file|
          path = File.join(config_dir, config_file)
          opts[:config_file] ||= path if File.exist?(path)
        end
      end

      # parse config file options
      opts = parse_config(opts[:config_file]).merge(opts) if opts[:config_file]

      # set defaults
      opts[:queues] = Array(opts[:queues]) << "default" if opts[:queues].nil? || opts[:queues].empty?
      opts[:strict] = true if opts[:strict].nil?
      opts[:concurrency] = Integer(ENV["RAILS_MAX_THREADS"]) if opts[:concurrency].nil? && ENV["RAILS_MAX_THREADS"]

      # merge with defaults
      options.merge!(opts)
    end

    def options
      Sidekiq.options
    end

    def boot_system
      ENV["RACK_ENV"] = ENV["RAILS_ENV"] = environment

      if File.directory?(options[:require])
        require "rails"
        if ::Rails::VERSION::MAJOR < 5
          raise "Sidekiq no longer supports this version of Rails"
        else
          require "sidekiq/rails"
          require File.expand_path("#{options[:require]}/config/environment.rb")
        end
        options[:tag] ||= default_tag
      else
        require options[:require]
      end
    end

    def default_tag
      dir = ::Rails.root
      name = File.basename(dir)
      prevdir = File.dirname(dir) # Capistrano release directory?
      if name.to_i != 0 && prevdir
        if File.basename(prevdir) == "releases"
          return File.basename(File.dirname(prevdir))
        end
      end
      name
    end

    def validate!
      if !File.exist?(options[:require]) ||
          (File.directory?(options[:require]) && !File.exist?("#{options[:require]}/config/application.rb"))
        logger.info "=================================================================="
        logger.info "  Please point sidekiq to a Rails 4/5 application or a Ruby file  "
        logger.info "  to load your worker classes with -r [DIR|FILE]."
        logger.info "=================================================================="
        logger.info @parser
        die(1)
      end

      [:concurrency, :timeout].each do |opt|
        raise ArgumentError, "#{opt}: #{options[opt]} is not a valid value" if options.key?(opt) && options[opt].to_i <= 0
      end
    end

    def parse_options(argv)
      opts = {}

      @parser = OptionParser.new { |o|
        o.on "-c", "--concurrency INT", "processor threads to use" do |arg|
          opts[:concurrency] = Integer(arg)
        end

        o.on "-d", "--daemon", "Daemonize process" do |arg|
          puts "WARNING: Daemonization mode was removed in Sidekiq 6.0, please use a proper process supervisor to start and manage your services"
        end

        o.on "-e", "--environment ENV", "Application environment" do |arg|
          opts[:environment] = arg
        end

        o.on "-g", "--tag TAG", "Process tag for procline" do |arg|
          opts[:tag] = arg
        end

        o.on "-q", "--queue QUEUE[,WEIGHT]", "Queues to process with optional weights" do |arg|
          queue, weight = arg.split(",")
          parse_queue opts, queue, weight
        end

        o.on "-r", "--require [PATH|DIR]", "Location of Rails application with workers or file to require" do |arg|
          opts[:require] = arg
        end

        o.on "-t", "--timeout NUM", "Shutdown timeout" do |arg|
          opts[:timeout] = Integer(arg)
        end

        o.on "-v", "--verbose", "Print more verbose output" do |arg|
          opts[:verbose] = arg
        end

        o.on "-C", "--config PATH", "path to YAML config file" do |arg|
          opts[:config_file] = arg
        end

        o.on "-L", "--logfile PATH", "path to writable logfile" do |arg|
          puts "WARNING: Logfile redirection was removed in Sidekiq 6.0, Sidekiq will only log to STDOUT"
        end

        o.on "-P", "--pidfile PATH", "path to pidfile" do |arg|
          puts "WARNING: PID file creation was removed in Sidekiq 6.0, please use a proper process supervisor to start and manage your services"
        end

        o.on "-V", "--version", "Print version and exit" do |arg|
          puts "Sidekiq #{Sidekiq::VERSION}"
          die(0)
        end
      }

      @parser.banner = "sidekiq [options]"
      @parser.on_tail "-h", "--help", "Show help" do
        logger.info @parser
        die 1
      end

      @parser.parse!(argv)

      opts
    end

    def initialize_logger
      Sidekiq.logger.level = ::Logger::DEBUG if options[:verbose]
    end

    def parse_config(path)
      opts = YAML.load(ERB.new(File.read(path)).result) || {}

      if opts.respond_to? :deep_symbolize_keys!
        opts.deep_symbolize_keys!
      else
        symbolize_keys_deep!(opts)
      end

      opts = opts.merge(opts.delete(environment.to_sym) || {})
      parse_queues(opts, opts.delete(:queues) || [])

      ns = opts.delete(:namespace)
      if ns
        # logger hasn't been initialized yet, puts is all we have.
        puts("namespace should be set in your ruby initializer, is ignored in config file")
        puts("config.redis = { :url => ..., :namespace => '#{ns}' }")
      end
      opts
    end

    def parse_queues(opts, queues_and_weights)
      queues_and_weights.each { |queue_and_weight| parse_queue(opts, *queue_and_weight) }
    end

    def parse_queue(opts, queue, weight = nil)
      opts[:queues] ||= []
      raise ArgumentError, "queues: #{queue} cannot be defined twice" if opts[:queues].include?(queue)
      [weight.to_i, 1].max.times { opts[:queues] << queue }
      opts[:strict] = false if weight.to_i > 0
    end
  end
end
