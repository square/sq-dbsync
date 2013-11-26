require 'sq/dbsync/load_action'

module Sq::Dbsync

  # Load action to incrementally keep a table up-to-date by loading deltas from
  # the source system. Note that this technique is unable by itself to detect
  # deletes, but behaviour can be added to delete records based on a separate
  # audit log. See documentation for more details.
  class IncrementalLoadAction < LoadAction
    def operation; 'increment'; end

    def prepare
      if super
        if plan.always_sync
          registry.set(plan.table_name,
            last_synced_at:       EPOCH,
            last_batch_synced_at: EPOCH,
            last_row_at:          nil
          )
        end

        !!registry.get(plan.table_name)
      else
        if plan.always_sync
          registry.delete(plan.table_name)
          target.drop_table(plan.table_name)
        end
        false
      end
    end

    def extract_data
      @metadata   = registry.get(plan.table_name)
      @start_time = now.call
      since       = (
        @metadata[:last_row_at] ||
        @metadata[:last_synced_at]
      ) - overlap

      @file, @last_row_at = measure(:extract) { extract_to_file(since) }
      self
    end

    def load_data
      measure(:load) do
        db.transaction do
          db.load_incrementally_from_file(
            plan.prefixed_table_name,
            plan.columns,
            @file.path
          )

          process_deletes

          registry.update(plan.table_name, @metadata[:last_batch_synced_at],
            last_synced_at: @start_time,
            last_row_at:    @last_row_at
          )
        end
        @file.close!
      end
      self
    end

    def post_load
      self
    end

    def prefix
      ''
    end

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

    def process_deletes
      # Provided as a hook for subclasses
    end
  end
end
