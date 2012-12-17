require 'thread'

# See lib/sq/dbsync/pipeline.rb
class Sq::Dbsync::Pipeline

  # A computational context for passing a number of tasks through a set of
  # stages, where each stage uses resources independent of the other stages.
  # For example, stage one may be able to execute a maximum of two tasks at
  # once, and stage two may also have a maximum of two, but it is optimum
  # that a total of four tasks to be processing at any one time.
  class ThreadedContext

    # Tracer object to mark the end of a stream of tasks.
    FINISH = Object.new

    def self.call(*args, &block)
      new(*args, &block).run
    end

    def initialize(tasks, stages, process)
      self.tasks   = tasks
      self.stages  = stages
      self.process = process
      self.threads = []
    end

    def run
      initial_queue, final_queue = build_pipeline(stages, tasks.length)

      tasks.each.with_index do |task, i|
        initial_queue << [i, task]
      end

      result = ordered (0...tasks.length).map { final_queue.pop }
      flush_threads(initial_queue)
      result
    end

  protected

    attr_accessor :tasks, :stages, :process

    # Floods the queue with enough FINISH markers to guarantee that each thread
    # will see one and shut itself down.
    def flush_threads(initial_queue)
      threads.size.times { initial_queue << FINISH }
      threads.each(&:join)
    end

    def ordered(tasks)
      tasks.
        sort_by(&:first).
        map(&:last)
    end

    def concurrency(stage)
      2
    end

    def build_pipeline(stages, number_of_tasks)
      initial_queue = Queue.new
      final_queue   = stages.inject(initial_queue) do |task_queue, stage|
        spawn_workers(stage, task_queue, number_of_tasks)
      end

      [initial_queue, final_queue]
    end

    def spawn_workers(stage, task_queue, number_of_tasks)
      next_queue = Queue.new

      self.threads += in_threads(concurrency(stage)) do
        while true
          index, task = task_queue.pop
          if index == FINISH
            next_queue << FINISH
            break
          else
            next_queue << [index, process.call(stage, task)]
          end
        end
      end

      next_queue
    end

    def in_threads(n, &block)
      n.times.map do
        Thread.new(&block)
      end
    end

    attr_accessor :threads
  end
end
