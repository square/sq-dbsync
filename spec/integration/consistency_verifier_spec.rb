require 'integration_helper'

require 'sq/dbsync/consistency_verifier'
require 'sq/dbsync/static_table_plan'
require 'sq/dbsync/table_registry'

describe SQD::ConsistencyVerifier do
  let(:overlap) { SQD::LoadAction.overlap }
  let(:now)     { Date.new(2012, 4, 4).to_time.utc }
  let(:source)  { test_source(:source) }
  let(:target)  { test_target }
  let(:tables) {[{
    table_name: :test_table,
    columns: [:id, :col1, :updated_at],
    consistency: true,
    source_db: source,
    indexes: {}
  }]}
  let(:registry) { SQD::TableRegistry.new(target) }
  let(:verifier) { SQD::ConsistencyVerifier.new(target, registry) }

  before do
    create_source_table_with(
      id:         1,
      col1:       'old record',
      created_at: now - overlap
    )
    setup_target_table(now)
  end

  it 'raises if counts do not match up' do
    error_string =
      "test_table had a count difference of 1; " +
      "source: #{source.name} (count: 1), " +
      "sink: #{target.name} (count: 0)"

    lambda {
      verifier.check_consistency!(tables)
    }.should raise_error(
      SQD::ConsistencyVerifier::ConsistencyError,
      error_string
    )
  end

  it 'uses last_row_at rather than last_synced_at' do
    registry.update(:test_table, now,
      last_row_at: now - 3
    )

    lambda {
      verifier.check_consistency!(tables)
    }.should_not raise_error(SQD::ConsistencyVerifier::ConsistencyError)
  end
end
