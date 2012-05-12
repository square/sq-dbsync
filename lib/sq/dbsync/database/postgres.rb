require 'delegate'
require 'tempfile'

require 'sq/dbsync/database/common'

module Sq::Dbsync::Database

  # Decorator around a Sequel database object, providing some non-standard
  # extensions required for effective extraction from Postgres.
  class Postgres < Delegator

    include Sq::Dbsync::Database::Common

    def initialize(db)
      super
      @db = db
    end

    def inspect; "#<Database::Postgres #{opts[:database]}>"; end


    def extract_to_file(table_name, columns, file_name)
      extract_sql_to_file("SELECT %s FROM %s" % [
        columns.join(', '),
        table_name
      ], file_name)
    end

    def hash_schema(table_name)
      ensure_connection

      result = schema(table_name).each do |col, metadata|
        if metadata[:db_type] == 'text'
          # A hack because MySQL can't index text columns
          metadata[:db_type] = 'varchar(255)'
        end

        if metadata[:db_type] == 'timestamp without time zone'
          metadata[:db_type] = 'datetime'
        end
      end

      Hash[result]
    end

    protected

    attr_reader :db

    def extract_sql_to_file(sql, file_name)
      sql = "COPY (#{sql}) TO STDOUT"
      file = sql_to_file(sql)

      cmd = "set -o pipefail; "
      cmd += "psql --no-align --tuples-only -F '\t'"
      cmd += " -U %s" % opts[:user]     if opts[:user]
      cmd += " -h %s" % opts[:host]     if opts[:host]
      cmd += " -p %i" % opts[:port]     if opts[:port]
      cmd += " %s"    % opts.fetch(:database)
      cmd += " -f %s" % file.path

      cmd += " > %s"  % file_name

      execute!(cmd)
    ensure
      file.close! if file
    end

    def sql_to_file(sql)
      file = Tempfile.new('extract_sql_to_file')
      file.write(sql)
      file.flush
      file
    end
  end
end
