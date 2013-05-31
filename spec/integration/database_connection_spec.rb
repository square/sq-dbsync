require 'integration_helper'

require 'sq/dbsync/database/connection'

shared_examples_for 'a decorated database adapter' do
  let(:path)   { @file.path }

  before { @file = Tempfile.new('bogus') }

  describe '#extract_sql_to_file' do
    it 'should raise when it fails' do
      lambda {
        db.extract_to_file('some_table', [], path)
      }.should raise_error(SQD::Database::ExtractError)
    end
  end
end

describe SQD::Database::Postgres do
  let(:source) { test_source(:postgres) }
  let(:db) { SQD::Database::Postgres.new(source, :source) }

  it_should_behave_like 'a decorated database adapter'
end

describe SQD::Database::Mysql do
  let(:source) { test_source(:source) }
  let(:db)     { SQD::Database::Mysql.new(source, :source) }

  it_should_behave_like 'a decorated database adapter'

  describe '#load_incrementally_from_file' do
    let(:path)   { @file.path }

    before { @file = Tempfile.new('bogus') }

    def source_with_exception(exception_message)
      source.stub(:run).and_raise(
        Sequel::DatabaseError.new(exception_message)
      )
    end

    it 're-raises deadlock related exceptions as TransientError' do
      source_with_exception("Deadlock found when trying to get lock")
      -> { db.load_incrementally_from_file('bogus', ['bogus'], path) }.
        should raise_error(SQD::Database::TransientError)
    end

    it 're-raises lock wait timeout exceptions as TransientError' do
      source_with_exception("Lock wait timeout exceeded")
      -> { db.load_incrementally_from_file('bogus', ['bogus'], path) }.
        should raise_error(SQD::Database::TransientError)
    end

    it 'does not translate unknown errors' do
      source_with_exception("Unknown")
      -> { db.load_incrementally_from_file('bogus', ['bogus'], path) }.
        should raise_error(Sequel::DatabaseError)
    end
  end
end
