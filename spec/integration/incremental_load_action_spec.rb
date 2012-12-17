require 'integration_helper'

require 'sq/dbsync/database/connection'
require 'sq/dbsync/incremental_load_action'
require 'sq/dbsync/table_registry'
require 'sq/dbsync/loggers'

describe SQD::IncrementalLoadAction do
  let(:overlap)        { described_class.overlap }
  let(:now)            { Date.new(2012, 4, 4).to_time.utc }
  let(:last_synced_at) { now - 10 }
  let(:source)         { test_source(:source) }
  let(:target) { test_target }
  let(:table_plan) {{
    table_name: :test_table,
    source_table_name: :test_table,
    columns: [:id, :col1, :updated_at],
    source_db: source,
    indexes: {}
  }}
  let(:registry) { SQD::TableRegistry.new(target) }
  let(:action) { SQD::IncrementalLoadAction.new(
    target,
    table_plan,
    registry,
    SQD::Loggers::Null.new,
    ->{ now }
  )}

  shared_examples_for 'an incremental load' do
    before :each do
      create_source_table_with({
        id:         1,
        col1:       'old record',
        updated_at: last_synced_at - overlap - 1
      }, {
        id:         2,
        col1:       'new record',
        updated_at: last_synced_at - overlap + 1
      })

      setup_target_table(last_synced_at)
    end

    describe ':all columns options' do
      let(:table_plan) {{
        table_name: :test_table,
        source_table_name: :test_table,
        columns: :all,
        source_db: source,
      }}

      it 'copies all columns to target' do
        action.call

        target[:test_table].map { |row| row.values_at(:id, :col1) }.
          should == [[2, 'new record']]
      end
    end


    it 'copies null data to the target' do
      source[:test_table].update(col1: nil)

      action.call

      target[:test_table].map {|row| row[:col1] }.
        should == [nil]
    end

    it 'copies source data to the target since the last synced row' do
      registry.update(:test_table, last_synced_at,
        last_synced_at: last_synced_at + 2,
        last_row_at:    last_synced_at
      )

      action.call

      target[:test_table].map { |row| row.values_at(:id, :col1) }.
        should == [[2, 'new record']]

      metadata = registry.get(:test_table)
      metadata[:last_synced_at].to_i.should == now.to_i
      metadata[:last_row_at].to_i.should == (last_synced_at - overlap + 1).to_i
    end

    it 'should replace any records found within the overlap' do
      target[:test_table].insert(
        id:   2,
        col1: 'old record'
      )

      action.call

      target[:test_table].map { |row| row.values_at(:id, :col1) }.
        should == [[2, 'new record']]
    end

    it 'should handle table that does not exist in source but does in target' do
      source.drop_table :test_table

      action.call

      registry.get(:test_table).should_not be_nil
      target.table_exists?(:test_table).should be
    end

    it 'should handle table that does not exist in target but in source' do
      target.drop_table :test_table
      registry.delete(:test_table)

      action.call

      target.table_exists?(:test_table).should_not be
    end

    it 'should handle column that does not exist in source' do
      table_plan[:columns] += [:bogus]

      action.call

      target[:test_table].map { |row| row.values_at(:col1) }.
        should == [['new record']]
    end

    context 'always_sync = true' do
      it 'handles table that does not exist in source but does in target' do
        source.drop_table :test_table
        table_plan[:always_sync] = true

        action.call

        registry.get(:test_table).should be_nil
        target.table_exists?(:test_table).should_not be
      end

      it 'handles table that does not exist in target with always_sync' do
        table_plan[:always_sync] = true
        target.drop_table :test_table
        registry.delete(:test_table)

        action.call

        target[:test_table].map { |row| row.values_at(:id, :col1) }.
          should == [[1, 'old record'], [2, 'new record']]

        metadata = registry.get(:test_table)
        metadata[:last_synced_at].to_i.should == now.to_i
        metadata[:last_row_at].to_i.should ==
          (last_synced_at - overlap + 1).to_i
      end
    end
  end

  describe 'with MySQL source' do
    let(:source) { test_source(:source) }

    it_should_behave_like 'an incremental load'
  end

  describe 'with PG source' do
    let(:source) { test_source(:postgres) }

    it_should_behave_like 'an incremental load'
  end
end
