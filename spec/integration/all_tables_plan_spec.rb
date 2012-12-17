require 'integration_helper'

require 'sq/dbsync/all_tables_plan'
require 'sq/dbsync/database/connection'

describe SQD::AllTablesPlan do
  let(:source) { test_source(:source) }

  it 'does not return tables with no PK' do
    source.create_table :test_table do
      Integer :col1
      DateTime :updated_at
    end

    SQD::AllTablesPlan.new.tables(source).should == []
  end

  it 'does not return tables with no timestamps' do
    source.create_table :test_table do
      primary_key :id
    end

    SQD::AllTablesPlan.new.tables(source).should == []
  end

  it 'handles table dropped after select' do
    source.create_table :test_table do
      primary_key :id
      DateTime :updated_at
    end

    source.should_receive(:schema).and_raise(Sequel::DatabaseError)

    SQD::AllTablesPlan.new.tables(source).should == []
  end
end
