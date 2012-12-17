module Sq::Dbsync

  # A key-value abstraction that is used to store metadata about loads on a
  # per-table basis.
  class TableRegistry
    def initialize(db)
      @db    = db
      @table = db[table_name]
    end

    def delete(key)
      table.filter(table_name: key.to_s).delete
    end

    # Set a value if an existing value does not already exist.
    def set(key, values)
      unless exists?(key)
        table.insert(values.merge(table_name: key.to_s))
      end
    end

    # Set a value, overriding any existing.
    def set!(key, values)
      db.transaction do
        delete(key)
        set(key, values)
      end
    end

    def update(key, lock, values)
      table.
        filter(
          table_name:           key.to_s,
          last_batch_synced_at: lock
        ).
        update(values)
    end

    def get(key)
      table.
        select(:last_synced_at, :last_row_at, :last_batch_synced_at).
        filter(table_name: key.to_s).
        first
    end

    def purge_except(keys)
      query = table
      if keys.any?
        query = query.where('table_name NOT IN ?', keys.map(&:to_s))
      end
      query.delete
    end

    def ensure_storage_exists
      db.create_table?(table_name, charset: 'utf8') do
        String   :table_name, primary_key: true
        DateTime :last_synced_at
        DateTime :last_batch_synced_at
        DateTime :last_row_at
      end
    end

    private

    attr_reader :table, :db

    def table_name
      :meta_last_sync_times
    end

    def exists?(key)
      table.filter(table_name: key.to_s).count > 0
    end
  end
end
