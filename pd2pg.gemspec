# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.name          = 'pd2pg'
  gem.version       = '1.0.0'
  gem.authors       = ['Stripe']
  gem.email         = ['oss@stripe.com']
  gem.description   = "Shovel data from PagerDuty's API to a Postgres database"
  gem.summary       = 'PagerDuty to Postgres shovel'
  gem.homepage      = 'https://github.com/stripe/pd2pg'
  gem.files         = `git ls-files`.split($/)
  gem.executables   = ['pd2pg']
  gem.test_files    = []

  gem.require_paths = ['lib']

  gem.add_dependency "excon", ">= 0.71.0"
  gem.add_dependency 'pg', '~> 0.18'
  gem.add_dependency 'sequel', '~> 4'
end
