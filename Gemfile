source 'http://rubygems.org'

gem 'sequel'

platforms :ruby do
  gem 'mysql2'
  gem 'pg'
end

platforms :jruby do
  gem 'jdbc-mysql'
  gem 'jdbc-postgres'
end

group :test do
  gem 'rspec'
  gem 'rake'

  platforms :ruby do
    gem 'cane'
    gem 'simplecov'
  end
end
