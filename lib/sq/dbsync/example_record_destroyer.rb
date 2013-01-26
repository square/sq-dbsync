# An example class that can reconstruct deletes from an audit log.
# We use the audit table as a proxy, though this is not written to in the same
# transaction as the destroy so it may arrive some time later.
#
# A faux-table is added to the sync times metadata "record_deletes" to make
# this process resilient to replication failures in either table.
#
# This is an example implementation, you will need to modify it to suit your
# purposes.
class ExampleRecordDestroyer < Struct.new(:db,
                                          :registry,
                                          :audit_table,
                                          :other_table)
  def self.run(*args)
    new(*args).run
  end

  def run
    max = last_sync_time(audit_table)

    if max
      user_ids = extract_deletes(unprocessed_audit_logs(max))

      # This conditional should not be required, but MySQL cannot optimize the
      # impossible where clause correctly and instead scans the table.
      if user_ids.any?
        db[other_table].filter(
          user_id: user_ids
        ).delete
      end

      # last_row_at calculation isn't correct but we don't use it.
      registry.set!(meta_table,
        last_synced_at:       max,
        last_row_at:          max,
        last_batch_synced_at: nil
      )
    end
  end

  def extract_deletes(audit_logs)
    audit_logs.
      group_by {|x| x[:target_id] }.
      select {|_, xs| last_value_set(xs) == 'false' }.
      keys
  end

  def unprocessed_audit_logs(max)

    query = db[audit_table].
      select(:target_id, :new_value, :updated_at).
      filter('updated_at <= ?', max).
      filter(action_name: %w(delete))

    min = last_sync_time(meta_table)
    if min
      query = query.filter('updated_at > ?', min)
    end

    query.to_a
  end

  def last_sync_time(table)
    record = registry.get(table)

    (record || {}).fetch(:last_synced_at, nil)
  end

  # updated_at is not distinct, so use id column as a tie-break.
  def last_value_set(xs)
    xs.sort_by {|y| [y[:updated_at], y[:id]] }.last[:new_value]
  end

  def meta_table
    :"#{other_table}_deletes"
  end
end
