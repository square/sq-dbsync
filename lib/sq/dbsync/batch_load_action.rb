require 'sq/dbsync/load_action'

module Sq::Dbsync
  # Load action to reload an entire table in full. The table will be loaded in
  # parallel to the existing one, then atomically swapped in on completion.
  class BatchLoadAction < LoadAction
    MAX_LAG = 60 * 5

    def operation; 'batch'; end

    def prepare
      return false if plan.batch_load == false

      if super
        if target.table_exists?(plan.prefixed_table_name)
          target.drop_table(plan.prefixed_table_name)
        end
        true
      end
    end

    def extract_data
      @start_time  = now.call
      @file, @last_row_at = measure(:extract) { extract_to_file(nil) }
      self
    end

    def load_data
      measure(:load) do
        TempfileFactory.split(@file, 1_000_000, logger) do |path|
          db.load_from_file(
            plan.prefixed_table_name,
            plan.columns,
            path
          )
        end
        @file.close!
      end
      self
    end

    def post_load
      while @start_time <= now.call - MAX_LAG
        @start_time = now.call
        catchup
      end

      switch_tables
      self
    end

    private

    def filter_columns
      plan.columns = resolve_columns(plan, source_columns)
    end

    def prefix
      'new_'
    end

    def catchup
      file, @last_row_at = measure(:catchup_extract) {
        extract_to_file(@last_row_at ? @last_row_at - overlap : nil)
      }
      measure(:catchup_load) do
        db.load_incrementally_from_file(
          plan.prefixed_table_name,
          plan.columns,
          file.path
        )
        file.close!
      end
    end

    def switch_tables
      measure(:switch) do
        registry.delete(plan.table_name)
        db.switch_table(
          plan.table_name,
          plan.prefixed_table_name
        )
        registry.set(plan.table_name,
          last_synced_at:       @start_time,
          last_batch_synced_at: @start_time,
          last_row_at:          @last_row_at
        )
      end
    end

  end
end
