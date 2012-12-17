require 'unit_helper'

require 'sq/dbsync/error_handler'

describe Sq::Dbsync::ErrorHandler do
  describe '#wrap' do
    it 'redacts message' do
      called = nil
      handler = described_class.new(
        sources: {
          db_a: { password: 'redactme' },
          db_b: { password: 'alsome' },
        },
        target: { password: 'thistoo'},
        error_handler: ->(ex) { called = ex }
      )

      ->{
        handler.wrap do
          raise "redactme alsome thistoo notthis"
        end
      }.should raise_error("REDACTED REDACTED REDACTED notthis")
      called.message.should == "REDACTED REDACTED REDACTED notthis"
    end
  end
end
