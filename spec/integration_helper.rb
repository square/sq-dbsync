require 'spec_helper'
require 'database_helper'

def create_source_table_with(*rows)
  # Total hack to allow source db to be passed as optional first argument.
  if rows[0].is_a?(Hash)
    source_db  = source
  else
    source_db  = rows.shift
  end
  table_name = :test_table

  source_db.create_table! table_name do
    primary_key  :id
    String       :col1
    String       :pii
    DateTime     :updated_at
    DateTime     :created_at
    DateTime     :imported_at
  end

  rows.each do |row|
    source_db[table_name].insert(row)
  end
end

def create_pg_source_table_with(*rows)
  # Total hack to allow source db to be passed as optional first argument.
  if rows[0].is_a?(Hash)
    source_db  = source
  else
    source_db  = rows.shift
  end
  table_name = :test_table

  source_db.create_table! table_name do
    primary_key  :id
    String       :col1
    String       :pii
    DateTime     :updated_at
    DateTime     :created_at
    DateTime     :imported_at
    column :ts_with_tz, 'timestamp with time zone'
  end

  rows.each do |row|
    source_db[table_name].insert(row)
  end
end

def setup_target_table(last_synced_at, name=:test_table)
  target.create_table! name do
    Integer  :id
    String   :col1
    DateTime :updated_at
    DateTime :created_at
  end

  target.add_index name, :id, :unique => true

  registry.ensure_storage_exists
  registry.set(name,
    last_synced_at:       last_synced_at,
    last_row_at:          last_synced_at,
    last_batch_synced_at: last_synced_at
  )
end
