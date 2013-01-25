require 'unit_helper'

require 'sq/dbsync/error_handler'

describe Sq::Dbsync::ErrorHandler do
  let(:config) { {
    sources: {
      db_a: { password: 'redactme' },
      db_b: { password: 'alsome' },
      db_c: {}
    },
    target: { password: 'thistoo'},
  }}

  describe '#wrap' do
    it 'redacts message' do
      called = nil
      config[:error_handler] = ->(ex) { called = ex }
      handler = described_class.new(config)
      ->{
        handler.wrap do
          raise "redactme alsome thistoo notthis"
        end
      }.should raise_error("REDACTED REDACTED REDACTED notthis")
      called.message.should == "REDACTED REDACTED REDACTED notthis"
    end
  end

  describe '#notify_error' do
    it 'includes tag in exception message' do
      called = nil
      config[:error_handler] = ->(ex) { called = ex }
      handler = described_class.new(config)

      handler.notify_error(:test_table, RuntimeError.new('hello'))

      called.message.should include('[test_table]')
      called.message.should include('hello')
    end

    it 'redacts message' do
      called = nil
      config[:error_handler] = ->(ex) { called = ex }
      handler = described_class.new(config)

      handler.notify_error(:test_table,
        RuntimeError.new("redactme alsome thistoo notthis"))

      called.message.should include("REDACTED REDACTED REDACTED notthis")
    end
  end
end
