require 'tempfile'

module Sq::Dbsync::Database
  module Common

    def extract_to_file(table_name, columns, file_name)
      extract_sql_to_file("SELECT %s FROM %s" % [
        columns.join(', '),
        table_name
      ], file_name)
    end

    def extract_incrementally_to_file(table_name,
                                     columns,
                                     file_name,
                                     last_row_at,
                                     overlap)
      table_name = table_name.to_sym
      db_columns = db.schema(table_name).map(&:first)
      timestamp = timestamp_column(columns)

      query = self[table_name].select(*columns)
      if last_row_at
        query = query.filter("#{timestamp} > ?", last_row_at - overlap)
      end

      extract_sql_to_file(query.sql, file_name)
    end

    def hash_schema(table_name)
      ensure_connection
      Hash[schema(table_name)]
    end

    def timestamp_column(columns)
      if columns.include?(:updated_at)
        :updated_at
      elsif columns.include?(:created_at)
        :created_at
      else # TODO: Raise on unknown
        :imported_at
      end
    end

    def name
      self['SELECT database()'].first.fetch(:'database()')
    end

    # Since we go so long without using connections (during a batch load), they
    # go stale and raise DatabaseDisconnectError when we try to use them. This
    # method ensures that the connection is fresh even after a long time
    # between drinks.
    def ensure_connection
      db.disconnect
    end

    def __getobj__
      db
    end

    def __setobj__(db)
      @db = db
    end

    protected

    def execute!(cmd)
      # psql doesn't return a non-zero error code when executing commands from
      # a file. The best way I can come up with is to raise if anything is
      # present on stderr.
      errors_file = TempfileFactory.make('extract_sql_to_file_errors')

      cmd = %{bash -c "#{cmd.gsub(/"/, '\\"')}"}

      result = run_shell(cmd, errors_file)

      unless result.exitstatus == 0 && File.size(errors_file.path) == 0
        raise(ExtractError, "Command failed: #{cmd}")
      end
    ensure
      errors_file.close! if errors_file
    end

    def sql_to_file(sql)
      TempfileFactory.make_with_content('extract_sql_to_file', sql)
    end

    private

    def run_shell(cmd, errors_file)
      if RUBY_PLATFORM == 'java'
        IO.popen4(cmd) {|_, _, _, stderr|
          errors_file.write(stderr.read)
          errors_file.flush
        }
        $?
      else
        pid = Process.spawn(cmd, STDERR => errors_file.path)
        Process.waitpid2(pid)[1]
      end
    end

    # This is a bit janky since it needs to escape SQL inside a string inside
    # double quotes inside a shell command.
    #
    # TODO: Is this still used?
    def escape_shell(sql)
      sql.gsub('`', '').gsub("'", '"')
    end

  end
end
