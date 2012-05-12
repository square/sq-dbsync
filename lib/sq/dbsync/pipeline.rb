require 'sq/dbsync/pipeline/threaded_context'
require 'sq/dbsync/pipeline/simple_context'

module Sq::Dbsync

  # An inject/reduce/fold-like abstraction to pass an array through a set of
  # operations, where the result of the first operation is passed to the second
  # operation, second to the third, and so on through the set until the final
  # result is returned.  It gracefully handles any individual failure and still
  # allows other results to be computed.
  #
  # Any unhandled exception will place an instance of `Pipeline::Failure` into
  # the returned results.
  #
  # The order and timing of when stages are is undefined (for example, they may
  # be parallelized), so they should be well isolated from each other.
  #
  # Examples
  #
  #     Pipeline.new([1, 2, 3],
  #       ->(x) { x * x },
  #       ->(x) { x + x }
  #     ).run
  #     # => [2, 8, 18]
  #
  #     Pipeline.new([1, 2],
  #       ->(x) { x == 1 ? raise : x },
  #       ->(x) { x * 10 }
  #     ).run
  #     # => [Pipeline::Failure, 20]
  class Pipeline

    def initialize(tasks, *stages)
      self.tasks  = tasks
      self.stages = stages
    end

    # Run the pipeline and return the computed results.
    #
    # context - The computational context in which to run the pipeline. Must
    #           respond to `#call` and take tasks, stages, and a processing
    #           lambda as arguments. By default runs the pipeline in parallel,
    #           but an alternative `SimpleContext` is provided to run in a
    #           single thread to aid debugging and testing.
    def run(context = ThreadedContext)
      context.call(tasks, stages, ->(stage, result) {
        process(stage, result)
      })
    end

    # Used to signal failed operations in a pipeline.
    class Failure < StandardError
      # The original exception that caused this failure.
      attr_reader :wrapped_exception

      # The task that was being processed when this failure occurred.
      attr_reader :task

      def initialize(wrapped, task)
        @wrapped_exception = wrapped
        @task              = task
      end
    end

  protected
    def process(stage, task)
      if task.is_a?(Failure)
        task
      else
        begin
          stage.call(task)
        rescue => e
          Failure.new(e, task)
        end
      end
    end

    attr_accessor :tasks, :stages
  end
end
