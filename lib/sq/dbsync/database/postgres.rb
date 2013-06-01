require 'delegate'
require 'tempfile'

require 'sq/dbsync/database/common'

module Sq::Dbsync::Database

  # Decorator around a Sequel database object, providing some non-standard
  # extensions required for effective extraction from Postgres.
  class Postgres < Delegator
    CASTS = {
        "text" => "varchar(255)",
        "character varying(255)" => "varchar(255)",

        # 255 is an arbitrary choice here. The one example we have
        # only has data 32 characters long in it.
        "character varying"      => "varchar(255)",

        # Arbitrarily chosen precision. The default numeric type in mysql is
        # (10, 0), which is perhaps the most useless default I could imagine.
        "numeric" => "numeric(12,6)",
        "boolean" => "char(1)",

        # mysql has no single-column representation for timestamp with time zone
        "timestamp with time zone" => "datetime",
        "time without time zone" => "time",
        "timestamp without time zone" => "datetime",
      }

    include Sq::Dbsync::Database::Common

    def set_lock_timeout(seconds)
      # Unimplemented
    end

    def hash_schema(plan)
      type_casts = plan.type_casts || {}
      ensure_connection

      table_name = source? ? plan.source_table_name : plan.table_name
      result = schema(table_name).each do |col, metadata|
        metadata[:source_db_type] ||= metadata[:db_type]
        metadata[:db_type] = cast_psql_to_mysql(metadata[:db_type])
      end

      Hash[result]
    end

    protected

    def cast_psql_to_mysql(db_type)
      CASTS.fetch(db_type, db_type)
    end

    # 'with time zone' pg fields have to be selected as
    # "column-name"::timestamp or they end up with a +XX tz offset on
    # them, which isn't the correct format for timestamp fields (which
    # is what they get turned into for portability.)
    def customize_sql(sql, schema)
      schema.each do |name, metadata|
        if metadata[:source_db_type].end_with? "with time zone"
          sql.sub! %r{"#{name}"}, '\0::timestamp'
        end
      end
      sql
    end

    def extract_sql_to_file(sql, file_name)
      sql = "COPY (#{sql}) TO STDOUT"
      file = sql_to_file(sql)

      cmd = "set -o pipefail; "
      cmd += "env PGTZ=utc psql --no-align --tuples-only -F '\t'"
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
  end
end
