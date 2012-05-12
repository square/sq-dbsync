# See lib/sq/dbsync/pipeline.rb
class Sq::Dbsync::Pipeline

  # Provides atomic operations on an underlying integer value. Useful for
  # thread coordination.
  #
  # Examples
  #
  #     n = AtomicInteger.new(5)
  #     n.inc
  #     n.to_i # => 6
  class AtomicInteger
    def initialize(value = 0)
      @value = value
      @mutex = Mutex.new
    end

    def inc
      @mutex.synchronize { @value += 1 }
    end

    def to_i
      @value
    end
  end
end
