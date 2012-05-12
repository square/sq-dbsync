require 'delegate'
require 'csv'
require 'sq/dbsync/database/common'

module Sq::Dbsync::Database

  # Thrown when a known temporary database error is detected.
  class TransientError < RuntimeError; end

  # Thrown when a command run via a sub-shell rather than Sequel fails.
  class ExtractError < RuntimeError; end

  # Decorator around a Sequel database object, providing some non-standard
  # extensions required for effective ETL with MySQL.
  class Mysql < Delegator

    include Common

    def initialize(db)
      super
      @db = db
    end

    def inspect; "#<Database::Mysql #{opts[:database]}>"; end

    def extract_to_file(table_name, columns, file_name)
      extract_sql_to_file("SELECT %s FROM %s" % [
        columns.join(', '),
        table_name
      ], file_name)
    end

    def load_from_file(table_name, columns, file_name)
      ensure_connection
      db.run "LOAD DATA INFILE '%s' IGNORE INTO TABLE %s (%s)" % [
        file_name,
        table_name,
        columns.join(', ')
      ]
    end

    def load_incrementally_from_file(table_name, columns, file_name)
      ensure_connection
      db.run "LOAD DATA INFILE '%s' REPLACE INTO TABLE %s (%s)" % [
        file_name,
        table_name,
        columns.join(', ')
      ]
    rescue Sequel::DatabaseError => e
      transient_regex =
        /Lock wait timeout exceeded|Deadlock found when trying to get lock/

      if e.message =~ transient_regex
        raise TransientError, e.message, e.backtrace
      else
        raise
      end
    end

    def consistency_check(table_name, t)
      ensure_connection
      db[table_name].
        filter("created_at BETWEEN ? AND ?", t - 60*60, t).
        count
    end

    # Overriden because the Sequel implementation does not work with partial
    # permissions on a table. See:
    # https://github.com/jeremyevans/sequel/issues/422
    def table_exists?(table_name)
      begin
        !!db.schema(table_name, reload: true)
      rescue Sequel::DatabaseError
        false
      end
    end

    def drop_table(table_name)
      db.drop_table(table_name)
    end

    def switch_table(to_replace, new_table)
      ensure_connection

      to_replace = to_replace.to_s

      renames = []
      drops   = []

      if table_exists?(to_replace)
        renames << [to_replace, 'old_' + to_replace]
        drops << 'old_' + to_replace
      end
      renames << [new_table, to_replace]

      db.run <<-SQL
        RENAME TABLE #{renames.map {|tables| "%s TO %s" % tables }.join(', ')}
      SQL

      drops.each { |table| drop_table(table) }
    end

    protected

    attr_reader :db

    def extract_sql_to_file(sql, file_name)
      cmd = "set -o pipefail; mysql --skip-column-names"
      cmd += " -u %s"   % opts[:user]     if opts[:user]
      cmd += " -p%s"    % opts[:password] if opts[:password]
      cmd += " -h %s"   % opts[:host]     if opts[:host]
      cmd += " -P %i"   % opts[:port]     if opts[:port]
      cmd += " %s"      % opts.fetch(:database)
      cmd += " -e '%s'" % escape_shell(sql)

      # This option prevents mysql from buffering results in memory before
      # outputting them, allowing us to stream large tables correctly.
      cmd += " --quick"

      cmd += " | sed 's/NULL/\\\\\\N/g'"
      cmd += " > %s" % file_name

      execute!(cmd)
     end

  end
end
