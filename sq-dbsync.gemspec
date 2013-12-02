# -*- encoding: utf-8 -*-
require File.expand_path('../lib/sq/dbsync/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Xavier Shay", "Damon McCormick"]
  gem.email         = ["xavier@squareup.com", "damon@squareup.com"]
  gem.description   =
    %q{Column based, timestamp replication of MySQL and Postgres databases.}
  gem.summary       = %q{
    Column based, timestamp replication of MySQL and Postgres databases. Uses
    Ruby for the glue code but pushes the heavy lifting on to the database.
  }
  gem.homepage      = "http://github.com/square/sq-dbsync"

  gem.executables   = []
  gem.files         = Dir.glob("{spec,lib}/**/*.rb") + %w(
                        README.md
                        HISTORY.md
                        LICENSE
                        sq-dbsync.gemspec
                      )
  gem.test_files    = Dir.glob("spec/**/*.rb")
  gem.name          = "sq-dbsync"
  gem.require_paths = ["lib"]
  gem.version       = Sq::Dbsync::VERSION
  gem.has_rdoc      = false
  gem.add_development_dependency 'rspec', '~> 2.0'
  gem.add_development_dependency 'rake'
  gem.add_development_dependency 'simplecov'
  gem.add_development_dependency 'cane'
  gem.add_dependency 'sequel'
end
