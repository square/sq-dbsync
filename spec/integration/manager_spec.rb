require 'integration_helper'

require 'sq/dbsync/manager'
require 'sq/dbsync/static_table_plan'
require 'sq/dbsync/loggers'

describe SQD::Manager do
  let(:now) { Time.now.utc }
  let(:config) {{
    sources: TEST_SOURCES,
    target:  TEST_TARGET,
    logger:  SQD::Loggers::Null.new,
    clock:   ->{ now }
  }}
  let(:manager)    { SQD::Manager.new(config, [
    [SQD::StaticTablePlan.new(plan), :source]
  ]) }
  let(:source)     { manager.sources.fetch(:source) }
  let(:alt_source) { manager.sources.fetch(:alt_source) }
  let(:target)     { manager.target }
  let(:registry)   { SQD::TableRegistry.new(target) }
  let(:plan) {[{
    table_name: :test_table,
    columns:    [:id, :updated_at]
  }] }
  let(:now) { Time.now.utc }

  before do
    create_source_table_with(
      id:         1,
      col1:       'hello',
      pii:        'don alias',
      updated_at: now - 10
    )
  end

  it 'handles duplicate table names by selecting the first one' do
    create_source_table_with(alt_source, {
      col1:       'hello',
      pii:        'don alias'
    }, {
      col1:       'hello again',
      pii:        'don alias'
    })

    manager = SQD::Manager.new(config,
      [
        [SQD::StaticTablePlan.new(plan), :source],
        [SQD::StaticTablePlan.new(plan), :alt_source]
      ]
    )
    manager.batch_nonactive

    target[:test_table].count.should == 1
  end

  it 'does not purge old tables from the database' do
    setup_target_table(now)

    manager = SQD::Manager.new(config, [])
    manager.batch_nonactive
    target.table_exists?(:test_table).should be
  end

  it 'removes old tables from the registry' do
    setup_target_table(now)

    manager = SQD::Manager.new(config, [])
    manager.increment_checkpoint
    registry.get(:test_table).should_not be

    # Dropping tables must be done manually
    target.table_exists?(:test_table).should be
  end

  it 'only batch loads the given tables, even when batch load disabled' do
    plan[0][:batch_load] = false

    manager = SQD::Manager.new(config, [
      [SQD:: StaticTablePlan.new(plan), :source],
    ])
    expect { manager.batch_nonactive([:bogus]) }.to raise_error(Sq::Dbsync::Manager::UnknownTablesError)
    target.table_exists?(:test_table).should_not be

    manager.batch_nonactive([:test_table])
    target.table_exists?(:test_table).should be
  end

  it 'does not purge tables excluded from batch load' do
    plan[0][:batch_load] = false
    setup_target_table(now)

    manager = SQD::Manager.new(config, [
      [SQD::StaticTablePlan.new(plan), :source],
    ])

    expect { manager.batch_nonactive([:bogus]) }.to raise_error(Sq::Dbsync::Manager::UnknownTablesError)
    target.table_exists?(:test_table).should be
  end

  it 'does not purge old tables when doing a partial load' do
    setup_target_table(now)

    manager = SQD::Manager.new(config, [])
    expect { manager.batch_nonactive([:bogus]) }.to raise_error(Sq::Dbsync::Manager::UnknownTablesError)

    target.table_exists?(:test_table).should be
  end
end
