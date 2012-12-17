require 'time'
require 'socket'

# Instrumenting various aspects of the system is critical since it will take
# longer and longer as the data sources grow and it is necessary to know when
# this import time is taking too long (either replication can't keep up, or
# recovery time is too long).
module Sq::Dbsync::Loggers

  # Abstract base class for loggers. This is useful because the CompositeLogger
  # needs to delegate to a set of loggers, which requires an explicit interface
  # to communicate with. This class helps define that relationship and describe
  # the interfaces.
  class Abstract
    def measure(label, &block); end
    def log(str); end
  end

  # Writes timing information to stdout. Thread-safe in that calls to measure
  # from separate threads will execute in parallel but synchronize before
  # writing their output.
  class Stream < Abstract
    def initialize(out = $stdout)
      @mutex   = Mutex.new
      @out     = out
      @threads = {}
    end

    def measure(label, &block)
      start_time = Time.now.utc
      log_measurement(start_time, :starting, 0, label)
      ret = nil
      exception = nil
      state = :finished
      begin
        ret = block.call
      rescue => e
        state = :failed
        exception = e
        raise
      ensure
        end_time = Time.now.utc
        log_measurement(end_time, state, end_time - start_time, label)
        log(exception.message) if exception
      end
      ret
    end

    def log_measurement(time, event, duration, object)
      log([
        current_thread_name,
        event,
        "%.3f" % duration,
        object
      ].join("\t"), time)
    end

    def log(str, time = Time.now.utc)
      # Synchronize to ensure lines are not interwoven.
      mutex.synchronize { out.puts([time, str].join("\t")) }
    end

    private

    # TODO: Remove old threads from cache
    def current_thread_name
      unless threads[Thread.current]
        mutex.synchronize {
          threads[Thread.current] = "THREAD%i" % (threads.size + 1)
        }
      end

      threads[Thread.current]
    end

    attr_reader :threads, :mutex, :out
  end

  # Combines multiple loggers together.
  class Composite  < Abstract
    attr_accessor :loggers

    def initialize(loggers = nil)
      @loggers = loggers
    end

    def measure(label, &block)
      # Babushka doll! Logger inside a logger inside a logger.
      loggers.inject(block) do |block, logger|
        lambda do
          logger.measure(label) do
            block.call
          end
        end
      end.call
    end

    def log(str)
      loggers.each { |logger| logger.log(str) }
    end
  end

  # Logs metric run time to graphite.
  class Graphite < Abstract
    def initialize(opts)
      @opts   = opts
    end

    def measure(label, &block)
      start_time = Time.now.utc
      block.call
    ensure
      end_time = Time.now.utc
      record_metric(end_time.to_i, label, end_time - start_time)
    end

    def record_metric(timestamp, name, value)
      msg = "#{@opts.fetch(:prefix, 'dbsync')}.#{name} #{value} #{timestamp}\n"

      s = TCPSocket.new(@opts[:host], @opts.fetch(:port, 2003))
      s.send msg, 0
      s.close
    end
  end

  # Used in test environments where instrumentation is not required.
  class Null < Abstract
    def measure(label, &block)
      block.call
    end
  end

  # Logging is one of the few outputs of the system, this class is provided as a
  # cheap way to allow tests to hook into events. It should not be used in
  # production.
  class NullWithCallbacks < Abstract
    attr_accessor :callbacks

    def initialize(callbacks = nil)
      @callbacks = callbacks
    end

    def measure(label, &block)
      (callbacks || {}).fetch(label, ->{}).call
      block.call
    end
  end
end
