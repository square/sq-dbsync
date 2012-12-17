require 'integration_helper'

require 'sq/dbsync/schema_maker'
require 'ostruct'

describe SQD::SchemaMaker do
  let(:target) { test_target }

  it 'creates a table with a compound index' do
    index = {
        index_on_col1: { columns: [:col1, :col2], unique: false }
    }

    plan = {
      prefixed_table_name: :test_table,
      table_name: :test_table,
      columns: [:col1, :col2],
      indexes: index,
      schema: {
        col1: { db_type: 'varchar(255)', primary_key: true },
        col2: { db_type: 'varchar(255)', primary_key: false }
      }
    }

    described_class.create_table(target, OpenStruct.new(plan))

    target.indexes(:test_table).should == index
  end

  it 'defaults primary key to id' do
    plan = {
      prefixed_table_name: :test_table,
      table_name: :test_table,
      columns: [:id],
      schema: { id: {db_type: 'varchar(255)', primary_key: false }}
    }

    described_class.create_table(target, OpenStruct.new(plan))

    target.schema(:test_table)[0][1][:primary_key].should == true
  end

  it 'allows primary key override' do
    plan = {
      prefixed_table_name: :test_table,
      table_name: :test_table,
      columns: [:id, :col1],
      primary_key: [:id, :col1],
      schema: {
        id:   {db_type: 'varchar(255)', primary_key: false },
        col1: {db_type: 'varchar(255)', primary_key: false }
      }
    }

    described_class.create_table(target, OpenStruct.new(plan))

    target.schema(:test_table)[0][1][:primary_key].should == true
    target.schema(:test_table)[1][1][:primary_key].should == true
  end


  it 'creates a table with an enum column' do
    plan = {
      prefixed_table_name: :test_table,
      table_name: :test_table,
      columns: [:col1],
      db_types: {:col1 => [:enum, %w(a b)]},
      schema: { col1: { primary_key: true }}
    }

    described_class.create_table(target, OpenStruct.new(plan))

    target.schema(:test_table)[0][1][:db_type].should == "enum('a','b')"
  end

  it 'creates a table with non-id primary key' do
    plan = {
      prefixed_table_name: :test_table,
      table_name: :test_table,
      columns: [:col1],
      schema: { col1: { db_type: 'varchar(255)', primary_key: true }}
    }

    described_class.create_table(target, OpenStruct.new(plan))

    target.schema(:test_table)[0][1][:primary_key].should == true
  end

  it 'creates a table with a not-null column' do
    plan = {
      prefixed_table_name: :test_table,
      table_name: :test_table,
      columns: [:col1],
      db_types: {:col1 => ['int(1) not null']},
      schema: { col1: { primary_key: true }}
    }

    described_class.create_table(target, OpenStruct.new(plan))

    target.schema(:test_table)[0][1][:allow_null].should == false
  end

  it 'creates a table with a composite primary key' do
    plan = {
      prefixed_table_name: :test_table,
      table_name: :test_table,
      columns: [:a, :b],
      schema: {
        a: { db_type: 'int', primary_key: true },
        b: { db_type: 'int', primary_key: true }
      }
    }

    described_class.create_table(target, OpenStruct.new(plan))

    target.schema(:test_table)[0][1][:primary_key].should == true
    target.schema(:test_table)[1][1][:primary_key].should == true
  end
end
