# frozen_string_literal: true

require "sidekiq"

module Sidekiq
  class BasicFetch
    # We want the fetch operation to timeout every few seconds so the thread
    # can check if the process is shutting down.
    TIMEOUT = 2

    UnitOfWork = Struct.new(:queue, :job) {
      def acknowledge
        # nothing to do
      end

      def queue_name
        queue.sub(/.*queue:/, "")
      end

      def requeue
        ## @done = true only after terminate and kill action
        Sidekiq.redis do |conn|
          conn.rpush("queue:#{queue_name}", job)
        end
      end
    }

    def initialize(options)
      @strictly_ordered_queues = !!options[:strict]
      @queues = options[:queues].map { |q| "queue:#{q}" }
      if @strictly_ordered_queues
        @queues = @queues.uniq
        @queues << TIMEOUT
      end
      @pq_queues = options[:pq_queues].map { |q| "pq_queue:#{q}" }
      if @strictly_ordered_queues
        @pq_queues = @pq_queues.uniq
        @pq_queues << TIMEOUT
      end
    end

    def pq_retrieve_work
      #work = Sidekiq.redis { |conn| conn.brpop(*queues_cmd) }
      p 'it is inside a retrieving work method'

      p 'it is retrieving pq work'
      # treatment for priority queues
      p "@pq_queues = #{@pq_queues}"
      p "pq_queues_cmd = #{pq_queues_cmd}"

      queue, job = Sidekiq.redis do |conn|
        conn.bzpopmin(*pq_queues_cmd) 
      end
      work = [queue, job]
      p "fetched queue = #{queue}"
      p "fetched job = #{job}"

      parsed_job = Sidekiq.load_json(job)
      client_id = parsed_job["client_id"]

      Sidekiq.redis do |conn|
        conn.zincrby('user_count', -1, client_id.to_s)
        if conn.zscore('user_count',client_id.to_s) <= 0.0
          conn.zrem('user_count',client_id.to_s)
        end
      end
      p 'it has retrieved pq work'

      UnitOfWork.new(*work) if work
    end


    def retrieve_work
      #work = Sidekiq.redis { |conn| conn.brpop(*queues_cmd) }
      p 'it is inside a retrieving work method'
      false_or_true = [true, false].sample
      if false_or_true
        p 'it is retrieving pq work'
        p "@queues = #{@queues}"
        # treatment for priority queues
        pq_queues_cmd = queues_cmd[1]
        queue, job = Sidekiq.redis do |conn|
          conn.bzpopmin(*pq_queues_cmd) 
        end
        work = [queue, job]

        parsed_job = Sidekiq.load_json(job)
        client_id = parsed_job["client_id"]

        Sidekiq.redis do |conn|
          conn.zincrby('user_count', -1, client_id.to_s)
          if conn.zscore('user_count',client_id.to_s) <= 0.0
            conn.zrem('user_count',client_id.to_s)
          end
        end
        p 'it has retrieved pq work'
      else
        # for normal queues
        normal_queues_cmd = queues_cmd[0]
        work = Sidekiq.redis do |conn|
         conn.brpop(*normal_queues_cmd) 
        end
      end
      UnitOfWork.new(*work) if work
    end

    # Creating the Redis#brpop command takes into account any
    # configured queue weights. By default Redis#brpop returns
    # data from the first queue that has pending elements. We
    # recreate the queue command each time we invoke Redis#brpop
    # to honor weights and avoid queue starvation.
    def queues_cmd
      if @strictly_ordered_queues
        @queues
      else
        queues = @queues.shuffle.uniq
        queues << TIMEOUT
        queues
      end
    end

    def pq_queues_cmd
      if @strictly_ordered_queues
        @pq_queues
      else
        queues = @pq_queues.shuffle.uniq
        queues << TIMEOUT
        queues
      end
    end

    # By leaving this as a class method, it can be pluggable and used by the Manager actor. Making it
    # an instance method will make it async to the Fetcher actor
    def self.bulk_requeue(inprogress, options)
      return if inprogress.empty?

      Sidekiq.logger.debug { "Re-queueing terminated jobs" }
      jobs_to_requeue = {}
      inprogress.each do |unit_of_work|
        jobs_to_requeue[unit_of_work.queue_name] ||= []
        jobs_to_requeue[unit_of_work.queue_name] << unit_of_work.job
      end

      Sidekiq.redis do |conn|
        conn.pipelined do
          jobs_to_requeue.each do |queue, jobs|
            conn.rpush("queue:#{queue}", jobs)
          end
        end
      end
      Sidekiq.logger.info("Pushed #{inprogress.size} jobs back to Redis")
    rescue => ex
      Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{ex.message}")
    end
  end
end
