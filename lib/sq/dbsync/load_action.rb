require 'date'
require 'ostruct'

require 'sq/dbsync/schema_maker'
require 'sq/dbsync/tempfile_factory'

module Sq::Dbsync
  # An stateful action object representing the transfer of data from a source
  # table to a target. The action can be performed in full using `#call`, but
  # control can also be inverted using the `.stages` method, which allows the
  # action to be combined to run efficiently in parallel with other actions.
  #
  # This is useful because a single load taxes the source system then the target
  # system in sequence, so for maximum efficency a second load should be
  # interleaved to start taxing the source system as soon as the first finishes
  # the extract, rather than waiting for it to also finish the load. This is not
  # possible if the process is fully encapsulated as it is in `#call`.
  #
  # This is an abstract base class, see `BatchLoadAction` and
  # `IncrementalLoadAction` for example subclasses.
  class LoadAction
    EPOCH = Date.new(2000, 1, 1).to_time

    # An empty action that is used when a load needs to be noop'ed in a manner
    # that does not raise an error (i.e. expected conditions).
    class NullAction
      def extract_data; self; end
      def load_data; self; end
      def post_load; self; end
    end

    def initialize(target, plan, registry, logger, now = ->{ Time.now.utc })
      @target   = target
      @plan     = OpenStruct.new(plan)
      @registry = registry
      @logger   = logger
      @now      = now
    end

    def tag
      plan.table_name
    end

    def call
      self.class.stages.inject(self) {|x, v| v.call(x) }
    end

    def self.stages
      [
        ->(x) { x.do_prepare || NullAction.new },
        ->(x) { x.extract_data },
        ->(x) { x.load_data },
        ->(x) { x.post_load }
      ]
    end

    def do_prepare
      return unless prepare

      ensure_target_exists
      self
    end

    protected

    attr_reader :target, :plan, :registry, :logger, :now

    def prepare
      unless plan.source_db.table_exists?(plan.source_table_name)
        logger.log("%s does not exist" % plan.source_table_name)
        return false
      end
      add_schema_to_table_plan(plan)
      plan.prefixed_table_name = (prefix + plan.table_name.to_s).to_sym
      filter_columns
    end

    def ensure_target_exists
      unless target.table_exists?(plan.prefixed_table_name)
        SchemaMaker.create_table(target, plan)
      end
    end

    def add_schema_to_table_plan(x)
      x.schema ||= x.source_db.hash_schema(x.source_table_name)
      x
    end

    def resolve_columns(plan, source_columns)
      if plan.columns == :all
        source_columns
      else
        source_columns & plan.columns
      end
    end

    def extract_to_file(since)
      plan.source_db.ensure_connection
      plan.source_db.set_lock_timeout(10)

      last_row_at = plan.source_db[plan.source_table_name].
        max(([:updated_at, :created_at, :imported_at] & plan.columns)[0])

      file = make_writeable_tempfile

      plan.source_db.extract_incrementally_to_file(
        plan.source_table_name,
        plan.columns,
        file.path,
        since,
        0
      )

      [file, last_row_at]
    end

    # This functionality is provided as a work around for the postgres query
    # planner failing to use indexes correctly for MAX() on a view that uses
    # UNION under the covers.
    #
    # It is most useful under the assumption that one of the tables being
    # unioned will always contain the most recent record (true in all current
    # cases). If this is not true, you must provide a custom view that supports
    # this query with a sane plan.
    def timestamp_table(plan)
      plan.source_db[plan.timestamp_table_name || plan.source_table_name]
    end

    def db; target; end

    def measure(stage, &block)
      label = "%s.%s.%s" % [
        operation,
        stage,
        plan.table_name
      ]
      logger.measure(label) { block.call }
    end

    def overlap
      self.class.overlap
    end

    # The distance we look back in time (in seconds) prior to the most recent
    # row we have seen. This needs to comfortably more that the maximum
    # expected time for a long running transaction.
    def self.overlap
      180
    end

    def make_writeable_tempfile
      TempfileFactory.make_world_writable(plan.table_name.to_s)
    end
  end
end
