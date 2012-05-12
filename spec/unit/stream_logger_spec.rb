require 'unit_helper'

require 'sq/dbsync/loggers'

describe SQD::Loggers::Stream do
  let(:buffer) { "" }
  let(:logger) { described_class.new(StringIO.new(buffer)) }

  it 'logs :finished when no exception is raised' do
    logger.measure(:ok) {}
    buffer.should include('finished')
  end

  it 'logs :failed when exception is raised' do
    lambda {
      logger.measure(:fail) { raise("fail") }
    }.should raise_error("fail")
    buffer.should include('failed')
  end

  it 'logs specified strings' do
    logger.log('logging is good')
    buffer.should include('logging is good')
  end
end
