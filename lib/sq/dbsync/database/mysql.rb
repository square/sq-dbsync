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
    # 2 days is chosen as an arbitrary buffer
    AUX_TIME_BUFFER = 60 * 60 * 24 * 2 # 2 days
    LOAD_SQL = "LOAD DATA INFILE '%s' %s INTO TABLE %s %s (%s)"

    include Common

    def load_from_file(table_name, columns, file_name)
      ensure_connection
      db.run(LOAD_SQL % [
        file_name, 'IGNORE', table_name, character_set, escape_columns(columns)
      ])
    end

    def set_lock_timeout(seconds)
      db.run lock_timeout_sql(seconds)
    end

    def load_incrementally_from_file(table_name, columns, file_name)
      ensure_connection
      # Very low lock wait timeout, since we don't want loads to be blocked
      # waiting for long queries.
      set_lock_timeout(10)
      db.run(LOAD_SQL % [
        file_name, 'REPLACE', table_name, character_set, escape_columns(columns)
      ])
    rescue Sequel::DatabaseError => e
      transient_regex =
        /Lock wait timeout exceeded|Deadlock found when trying to get lock/

      if e.message =~ transient_regex
        raise TransientError, e.message, e.backtrace
      else
        raise
      end
    end

    # Deletes recent rows based on timestamp, but also allows filtering by an
    # auxilary timestamp column for the case where the primary one is not
    # indexed on the target (such as the DFR reports, where imported_at is not
    # indexed, but reporting date is).
    def delete_recent(plan, since)
      ensure_connection
      query = db[plan.table_name].
        filter("#{plan.timestamp} > ?", since)

      if plan.aux_timestamp_column
        query = query.filter(
          "#{plan.aux_timestamp_column} > ?",
          since - AUX_TIME_BUFFER
        )
      end

      query.delete
    end

    def consistency_check(table_name, t)
      ensure_connection
      db[table_name].
        filter("created_at BETWEEN ? AND ?", t - 60*60, t).
        count
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

    def extract_sql_to_file(sql, file_name)
      file = sql_to_file(connection_settings + sql)
      cmd = "set -o pipefail; mysql --skip-column-names"
      cmd += " -u %s"   % opts[:user]     if opts[:user]
      cmd += " -p%s"    % opts[:password] if opts[:password]
      cmd += " -h %s"   % opts[:host]     if opts[:host]
      cmd += " -P %i"   % opts[:port]     if opts[:port]

      if opts[:ssl]
        cmd += " --ssl-ca %s --ssl-cert %s --ssl-key %s" % [
          opts[:ssl][:ca], opts[:ssl][:cert], opts[:ssl][:key]
        ]
      end

      cmd += " --default-character-set %s" % charset if charset

      cmd += " %s"      % opts.fetch(:database)

      # This option prevents mysql from buffering results in memory before
      # outputting them, allowing us to stream large tables correctly.
      cmd += " --quick"

      cmd += " < #{file.path}"
      cmd += " | sed 's/NULL/\\\\\\N/g'"
      cmd += " > %s" % file_name

      execute!(cmd)
    end

    def escape_columns(columns)
      columns.map {|x| "`#{x}`" }.join(', ')
    end

    def connection_settings
      lock_timeout_sql(10)
    end

    def character_set; charset ? " character set #{charset}" : "" end

    def lock_timeout_sql(seconds)
      "SET SESSION innodb_lock_wait_timeout = %i;" % seconds
    end
  end
end
