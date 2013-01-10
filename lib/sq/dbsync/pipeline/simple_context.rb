# See lib/pipeline.rb
class Sq::Dbsync::Pipeline

  # A computational context that passes a number of tasks through a set of
  # stages in sequence.
  class SimpleContext
    def self.call(tasks, stages, process)
      tasks.map do |task|
        stages.inject(task) do |result, stage|
          process.call(stage, result)
        end
      end
    end
  end
end
