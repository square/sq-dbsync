require 'acceptance_helper'

require 'sq/dbsync'

describe 'Syncing source databases to a target' do
  let(:config) {{
    sources: TEST_SOURCES,
    target:  TEST_TARGET,
    logger:  SQD::Loggers::Composite.new([logger]),
    clock:   ->{ @now }
  }}
  let(:logger)  { SQD::Loggers::NullWithCallbacks.new }
  let(:manager) {
    SQD::Manager.new(config, [[SQD::StaticTablePlan.new(plan), :source]])
  }
  let(:source)     { manager.sources.fetch(:source) }
  let(:alt_source) { manager.sources.fetch(:alt_source) }
  let(:target) { manager.target }
  let(:plan) {[{
    table_name: :test_table,
    source_table_name: :test_table,
    refresh_recent: true,
    columns:    [:id, :updated_at]
  }] }

  MINUTE = 60
  WEEK = MINUTE * 60 * 24 * 7

  before do
    @now = Time.now.utc

    setup_source_table
  end

  context 'batch loads' do
    it 'batch loads the nonactive database and switches it to active' do
      row = source[:test_table].insert(updated_at: @now)

      manager.batch_nonactive

      target[:test_table].map {|x| x[:id] }.should include(row)
      target[:meta_last_sync_times].count.should == 1
    end

    it 'catches up missed rows with an incremental update' do
      new_row_id = nil

      logger.callbacks = {
        'batch.load.test_table' => ->{
          @now += SQD::BatchLoadAction::MAX_LAG
          new_row_id = source[:test_table].insert(updated_at: @now - 1)
        }
      }

      row = source[:test_table].insert(updated_at: @now)

      manager.batch_nonactive

      target[:test_table].map {|x| x[:id] }.should include(row)
      target[:test_table].map {|x| x[:id] }.should include(new_row_id)
    end

    it 'loads from two distinct sources' do
      manager = SQD::Manager.new(config, [
        [SQD::StaticTablePlan.new(plan), :source],
        [SQD::AllTablesPlan.new,         :alt_source]
      ])

      row     =     source[:test_table    ].insert(updated_at: @now)
      alt_row = alt_source[:alt_test_table].insert(updated_at: @now)

      manager.batch_nonactive

      target[:test_table     ].map {|x| x[:id] }.should include(row)
      target[:alt_test_table ].map {|x| x[:id] }.should include(alt_row)
    end
  end

  context 'refresh recent load' do
    before do
      manager.batch_nonactive
    end

    it 'reloads all recent data' do
      deleted = 1
      to_keep = 2
      new_row = 3

      target[:test_table].insert(id: deleted, updated_at: @now)
      target[:test_table].insert(id: to_keep, updated_at: @now - WEEK)
      source[:test_table  ].insert(id: new_row, updated_at: @now - MINUTE)

      x = target[:test_table].map {|x| x[:id] }

      manager.refresh_recent

      target[:test_table].map {|x| x[:id] }.should include(new_row)
      target[:test_table].map {|x| x[:id] }.should include(to_keep)
      target[:test_table].map {|x| x[:id] }.should_not include(deleted)
    end

    describe 'when a column is provided' do
      let(:plan) {[{
        table_name: :test_table,
        refresh_recent: :reporting_date,
        source_table_name: :test_table,
        columns:    [:id, :updated_at, :reporting_date]
      }] }

      it 'adds an extra filter when a column is provided' do
        deleted = 1
        to_keep = 2
        new_row = 3
        to_keep2 = 4

        target[:test_table].insert(
          id:             deleted,
          updated_at:     @now,
          reporting_date: @now
        )
        target[:test_table].insert(
          id:             to_keep,
          updated_at:     @now - WEEK,
          reporting_date: @now
        )
        target[:test_table].insert(
          id:             to_keep2,
          updated_at:     @now,
          reporting_date: @now - WEEK
        )
        source[:test_table].insert(
          id:         new_row,
          updated_at: @now - MINUTE
        )

        x = target[:test_table].map {|x| x[:id] }
        manager.refresh_recent

        target[:test_table].map {|x| x[:id] }.should include(new_row)
        target[:test_table].map {|x| x[:id] }.should include(to_keep)
        target[:test_table].map {|x| x[:id] }.should include(to_keep2)
        target[:test_table].map {|x| x[:id] }.should_not include(deleted)
      end
    end
  end

  context 'incremental loads' do
    let(:worker) { @worker }
    before do
      manager.batch_nonactive
      @worker = background do
        manager.increment_active
      end
    end

    after do
      manager.stop!
      worker.wait_until_finished!
    end

    def background(&block)
      worker = Thread.new(&block)
      worker.instance_eval do
        def wait_until_finished!; join rescue nil; end
      end
      worker
    end

    it 'updates the active database' do
      2.times do |t|
        insert_and_verify_row
      end
    end

#     it 'does not continually retry consistent failures' do
#       pending("Hangs, stub doesn't work.")
#       target.
#         stub!(:load_incrementally_from_file).
#         and_raise(SQD::Database::ExtractError.new)
# 
#       ->{
#         worker.value
#       }.should raise_error(SQD::Database::ExtractError)
#     end

    context 'with an all tables plan' do
      let(:manager) { SQD::Manager.new(config, [[
        SQD::AllTablesPlan.new, :source
      ]]) }

      it 'adds a database that is newly in the source but not in the target' do
        source.create_table! :new_table do
          primary_key  :id
          DateTime :updated_at
        end

        insert_and_verify_row(:new_table)
      end
    end
  end

  def insert_and_verify_row(table = :test_table)
    row = source[table].insert(updated_at: Time.now.utc)
    spin_for(1) do
      target.table_exists?(table) &&
        target[table].map {|x| x[:id] }.include?(row)
    end
  end

  def run_failed_batch
    logger.callbacks = { 'batcave.test.switch_active' => ->{ raise } }

    ->{ manager.batch_nonactive }.should raise_error
  end

  def setup_source_table
    source.create_table! :test_table do
      primary_key  :id
      DateTime :reporting_date
      DateTime :updated_at
    end
    source.create_table! :test_table_2 do
      primary_key  :id
      DateTime :updated_at
    end
    alt_source.create_table! :alt_test_table do
      primary_key  :id
      DateTime :updated_at
    end
  end

  def spin_for(timeout = 1)
    result     = false
    start_time = Time.now

    while start_time + timeout > Time.now
      result = yield
      break if result
      sleep 0.001
    end

    raise "timed out" unless result
  end
end
