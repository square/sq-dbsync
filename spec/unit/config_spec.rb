require 'unit_helper'

require 'sq/dbsync/config'

describe Sq::Dbsync::Config do
  it 'provides a default error handler' do
    described_class.make({})[:error_handler].should respond_to(:call)
  end

  it 'provides a default clock' do
    described_class.make({})[:clock].().should be_instance_of(Time)
  end

  it 'provides a default logger' do
    described_class.make({})[:logger].should \
      be_a_kind_of(Sq::Dbsync::Loggers::Abstract)
  end
end
