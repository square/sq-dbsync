require 'sequel/no_core_ext'

Sequel.default_timezone = :utc

require 'sq/dbsync/database/mysql'
require 'sq/dbsync/database/postgres'

module Sq::Dbsync::Database
  # Factory class to abstract selection of a decorator to faciliate databases
  # other than MySQL.
  class Connection
    def self.create(opts, source_or_target)
      case opts[:brand]
      when 'mysql'
        db = Sq::Dbsync::Database::Mysql.new(
          Sequel.connect(opts), source_or_target
        )
        db.charset = opts[:charset] if opts[:charset]
        db
      when 'postgresql'
        Sq::Dbsync::Database::Postgres.new(
          Sequel.connect(opts), source_or_target
        )
      else
        raise "Unsupported database: #{opts.inspect}"
      end
    end
  end
end
