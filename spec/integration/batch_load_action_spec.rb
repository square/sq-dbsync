require 'integration_helper'

require 'sq/dbsync/database/connection'
require 'sq/dbsync/loggers'
require 'sq/dbsync/batch_load_action'
require 'sq/dbsync/table_registry'
require 'sq/dbsync/static_table_plan'
require 'sq/dbsync/all_tables_plan'

describe SQD::BatchLoadAction do
  let(:overlap) { described_class.overlap }
  let!(:now)    { @now = Time.now.utc }
  let(:last_synced_at) { now - 10 }
  let(:target) { test_target }
  let(:table_plan) {{
    table_name: :test_table,
    columns: [:id, :col1, :updated_at],
    source_db: source,
    indexes: index
  }}
  let(:index) {{
    index_on_col1: { columns: [:col1], unique: false }
  } }
  let(:registry) { SQD::TableRegistry.new(target) }
  let(:action) { SQD::BatchLoadAction.new(
    target,
    table_plan,
    registry,
    SQD::Loggers::Null.new,
    ->{ @now }
  ) }

  shared_examples_for 'a batch load' do
    before do
      create_source_table_with(
        id:         1,
        col1:       'hello',
        pii:        'don alias',
        updated_at: now - 10
      )
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

        target.hash_schema(:test_table).keys.should ==
          source.hash_schema(:test_table).keys
      end
    end

    it 'copies source tables to target with matching schemas' do
      start_time = now.to_f

      action.call

      verify_schema
      verify_data
      verify_metadata(start_time)
    end

    it 'handles column that does not exist in source' do
      source.alter_table :test_table do
        drop_column :id
      end

      action.call

      target[:test_table].map {|x| x.values_at(:col1)}.
        should == [['hello']]
    end

    it 'handles table that does not exist in source' do
      source.drop_table :test_table

      action.call

      target.table_exists?(:test_table).should_not be
    end

    it 'ignores duplicates when loading data' do
      source[:test_table].insert(id: 2, col1: 'hello')
      source[:test_table].insert(id: 3, col1: 'hello')

      table_plan[:indexes][:unique_index] = {columns: [:col1], unique: true}

      action.call

      target[:test_table].count.should == 1
    end

    it 'clears partial load if a new_ table already exists' do
      setup_target_table(now)
      target.switch_table(:new_test_table, :test_table)

      source[:test_table].insert(
        id: 7,
        col1: 'old',
        updated_at: now - 600
      )

      target[:new_test_table].insert(
        id:         2,
        col1:       'already loaded',
        updated_at: now - 200
      )

      action.call

      target[:test_table].all.map {|x| x[:col1] }.sort.should ==
        ['hello', 'old'].sort
    end

    it 'catches up from last_row_at' do
      action.do_prepare
      action.extract_data
      action.load_data

      source[:test_table].insert(id: 2, col1: 'new', updated_at: now)

      @now += 600

      action.post_load

      target[:test_table].all.map {|x| x[:col1] }.sort.should ==
        ['hello', 'new'].sort
    end

    def test_tables
      {
        test_table: source,
      }
    end

    def verify_schema
      test_tables.each do |table_name, source_db|
        target_table_name = table_name
        target.tables.should include(target_table_name)
        source_test_table_schema =
          source_db.schema(table_name).map do |column, hash|
            # Auto-increment is not copied, since it isn't relevant for
            # replicated tables and would be more complicated to support.
            # Primary key status is copied, however.
            hash.delete(:auto_increment)
            [column, hash]
          end

        extract_common_db_column_info = ->(e) { [
          e[0],
          {
            type:         e[1][:type],
            primary_key:  e[1][:primary_key],
            ruby_default: e[1][:ruby_default],
          }
        ] }

        if source.is_a?(SQD::Database::Postgres)
          source_test_table_schema = source_test_table_schema.map do |e|
            # Only look at some of the keys because postgres defines others
            # differently than mysql.
            extract_common_db_column_info.call(e)
          end
        end

        target.schema(target_table_name).each do |column_arr|
          if source.is_a?(SQD::Database::Postgres)
            column_arr = extract_common_db_column_info.call(column_arr)
          end
          source_test_table_schema.should include(column_arr)
        end
        target.indexes(target_table_name).should == index
      end
    end

    def verify_data
      test_tables.each do |table_name, _|
        data = target[table_name].all
        data.count.should == 1
        data = data[0]
        data.keys.length.should == 3
        data[:id].should == 1
        data[:col1].should == 'hello'
        data[:updated_at].to_i.should == (now - 10).to_i
      end
    end

    def verify_metadata(start_time)
      test_tables.each do |table_name, _|
        meta = registry.get(table_name)
        meta[:last_synced_at].should_not be_nil
        meta[:last_batch_synced_at].should_not be_nil
        meta[:last_batch_synced_at].to_i.should == start_time.to_i
        meta[:last_row_at].to_i.should == (now - 10).to_i
      end
    end
  end

  describe 'with MySQL source' do
    let(:source) { test_source(:source) }

    it_should_behave_like 'a batch load'
  end

  describe 'with PG source' do
    let(:source) { test_source(:postgres) }

    it_should_behave_like 'a batch load'
  end

end
