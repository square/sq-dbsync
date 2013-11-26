require 'sq/dbsync/load_action'

module Sq::Dbsync

  # This is a terribly named class that will delete the last X days of data
  # from a table and reload it. Useful for tables that are nearly append only
  # but sometimes will update recent data (for instance, a failed import). The
  # tables are too big to regularly reload in their entirety, but reloading
  # only recent data fixes the main issues.
  class RefreshRecentLoadAction < LoadAction
    WINDOW = 60 * 60 * 24 * 2 # 2 days

    def operation; 'refresh_recent'; end

    def prepare
      return false unless plan.refresh_recent

      super
    end

    def post_load
    end

    def extract_data
      @metadata   = registry.get(plan.table_name)
      @start_time = now.call
      @since      = (
        @metadata[:last_row_at] ||
        @metadata[:last_synced_at]
      ) - WINDOW
      @file, @last_row_at = measure(:extract) { extract_to_file(@since) }
      self
    end

    def load_data
      measure(:load) do
        tname   = plan.table_name
        columns = plan.columns
        db.transaction do
          db.delete_recent(plan, @since)
          db.load_from_file(tname, columns, @file.path)
        end
      end
      @file.close!
      self
    end

    private

    def filter_columns
      plan.columns   = resolve_columns(plan, source_columns) &
        (target_columns || source_columns)
    end

    def target_columns
      # Because we may create the target table later if necessary,
      # we need to check if it *really* exists
      target_columns = if target.table_exists?(plan.table_name)
        target.hash_schema(plan).keys
      else
        nil
      end
    end

    def prefix
      ''
    end
  end
end
