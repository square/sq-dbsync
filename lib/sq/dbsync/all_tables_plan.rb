module Sq::Dbsync
  # Fetches all tables from the given source, retrieving tables and columns.
  # Indexes are currently ignored.
  class AllTablesPlan
    def tables(source)
      source.ensure_connection

      source.tables.map do |t|
        schema_for_table(source, t)
      end.compact
    end

  private

    def schema_for_table(source, t)
      schema = source.schema(t, reload: true)

      return unless has_primary_key?(schema)
      return unless has_timestamp?(schema)

      cols = schema.map do |col|
        col[0]
      end

      {
        source_db:  source,
        source_table_name: t,
        table_name: t,
        columns:    cols,
        indexes:    {},
        always_sync: true
      }
    rescue Sequel::DatabaseError
      # This handles a race condition where the table is deleted between us
      # selecting the list of tables and fetching the schema.
      nil
    end

    def has_primary_key?(schema)
      schema.any? do |table|
        table[1][:primary_key]
      end
    end

    def has_timestamp?(schema)
      schema.any? do |table|
        [:updated_at, :created_at].include?(table[0])
      end
    end
  end
end
