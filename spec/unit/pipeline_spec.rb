require 'unit_helper'

require 'sq/dbsync/pipeline'

shared_examples_for 'a pipeline' do
  it 'passes tasks through each stage' do
    ret = SQD::Pipeline.new([3, 4],
      ->(x) { x * x },
      ->(x) { x + x }
    ).run(described_class)
    ret.should == [18, 32]
  end

  it 'returns errors' do
    ret = SQD::Pipeline.new([1],
      ->(x) { raise("fail") }
    ).run(described_class)
    ret.length.should == 1
    ret = ret[0]
    ret.should be_instance_of(SQD::Pipeline::Failure)
    ret.wrapped_exception.should be_instance_of(RuntimeError)
    ret.wrapped_exception.message.should == "fail"
    ret.task.should == 1
  end

  it 'handles errors in the middle of a pipeline' do
    ret = SQD::Pipeline.new([1, 2],
      ->(x) { x == 1 ? 10 : raise("fail") },
      ->(x) { x + 1 }
    ).run(described_class)
    ret[0].should == 11
    ret[1].should be_instance_of(SQD::Pipeline::Failure)
  end
end

describe SQD::Pipeline::ThreadedContext do
  it_should_behave_like 'a pipeline'
end

describe SQD::Pipeline::SimpleContext do
  it_should_behave_like 'a pipeline'
end
