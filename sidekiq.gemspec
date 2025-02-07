require_relative "lib/sidekiq/version"

Gem::Specification.new do |gem|
  gem.authors       = ["Mike Perham"]
  gem.email         = ["mperham@gmail.com"]
  gem.summary       = "Simple, efficient background processing for Ruby"
  gem.description   = "Simple, efficient background processing for Ruby."
  gem.homepage      = "http://sidekiq.org"
  gem.license       = "LGPL-3.0"

  gem.executables   = ["sidekiq"]
  gem.files         = `git ls-files | grep -Ev '^(test|myapp|examples)'`.split("\n")
  gem.name          = "sidekiq"
  gem.version       = "5.2.8"
  gem.required_ruby_version = ">= 2.5.0"

  gem.add_dependency "redis", ">= 4.1.0"
  gem.add_dependency "connection_pool", ">= 2.2.2"
  gem.add_dependency "rack", ">= 2.0.0"
  gem.add_dependency "rack-protection", ">= 2.0.0"
end
