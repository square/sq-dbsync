require 'sequel/no_core_ext'

Sequel.default_timezone = :utc

require 'sq/dbsync/database/mysql'
require 'sq/dbsync/database/postgres'

module Sq::Dbsync::Database
  # Factory class to abstract selection of a decorator to faciliate databases
  # other than MySQL.
  class Connection
    def self.create(opts, direction)
      case opts[:brand]
      when 'mysql'
        Sq::Dbsync::Database::Mysql.new(opts, direction)
      when 'postgresql'
        Sq::Dbsync::Database::Postgres.new(opts, direction)
      else
        raise "Unsupported database: #{opts.inspect}"
      end
    end
  end
end
