module Sq::Dbsync
  # Service class for mapping table plans to DDL.
  class SchemaMaker

    # Creates a table in the database based on the given plan. If the table
    # already exists, it will be recreated and any data will be lost.
    def self.create_table(db, table_plan)
      new(db, table_plan).run
    end

    def run
      table_plan = @table_plan

      db.create_table!(table_name,
         engine: 'InnoDB',
         charset: table_plan.charset || 'utf8'
      ) do
        extend Helpers

        add_columns!(table_plan)
        add_indexes!(table_plan)
        add_primary_key!(table_plan)
      end
    end

    protected

    def initialize(db, table_plan)
      @db = db
      @table_plan = table_plan
    end

    attr_reader :db, :table_plan

    def table_name
      table_plan.prefixed_table_name
    end

    module Helpers
      def add_columns!(table_plan)
        columns  = table_plan.columns
        db_types = table_plan.db_types || {}
        schema   = table_plan.schema
        columns.each do |column_name|
          add_column(column_name, db_types, schema)
        end
      end

      def add_indexes!(table_plan)
        indexes = table_plan.indexes || []

        indexes.each do |index_name, index_metadata|
          index_columns = index_metadata[:columns]
          unique        = index_metadata[:unique] || false
          send(:index, index_columns, name: index_name, unique: unique)
        end
      end

      def add_primary_key!(table_plan)
        columns = if table_plan.primary_key
          table_plan.primary_key
        else
          table_plan.schema.select {|col, schema|
            schema[:primary_key]
          }.map(&:first)
        end

        columns = [:id] if columns.empty?

        primary_key(columns)
      end


      def add_column(column_name, db_types, schema)
        db_type = db_types[column_name] || [schema[column_name][:db_type]]

        extra = if db_type[0] == :enum
          { elements: db_type[1] }
        else
          {}
        end

        send db_type[0], column_name, extra
      end
    end
  end
end
