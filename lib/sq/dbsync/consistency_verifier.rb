require 'sq/dbsync/load_action' # For overlap, not ideal

module Sq::Dbsync

  # Performs a cheap check to verify that the number of records present for a
  # recent time slice are the same across source and target tables.
  #
  # This checks consistency on the current tables, not the new_ set.
  class ConsistencyVerifier
    def initialize(target, registry)
      @target   = target
      @registry = registry
    end

    def check_consistency!(tables)
      tables.each do |tplan|
        next unless tplan[:consistency]
        verify_consistency!(tplan)
      end
    end

    def verify_consistency!(tplan)
      last_row_at = registry.get(tplan[:table_name])[:last_row_at]
      return unless last_row_at

      now = registry.get(tplan[:table_name])[:last_row_at] - LoadAction.overlap

      counts = [
        tplan[:source_db],
        target
      ].map do |x|
        x.consistency_check(tplan[:table_name], now)
      end

      delta = counts.reduce(:-)

      unless delta == 0
        raise ConsistencyError.new(
          tplan[:table_name],
          delta,
          "source: #{tplan[:source_db].name} (count: #{counts[0]}), " +
          "sink: #{target.name} (count: #{counts[1]})"
        )
      end
    end

    attr_reader :target, :registry

    # Used to signal an observed error in the number of records between source
    # and target tables. There are no current known situations in which this
    # occurs, though in the past buggy handling of replication lag was normally
    # the culprit.
    #
    # If it does occur, a good first response is to set `last_sync_times` to the
    # last batch time (usually within 24 hours) which will force batcave to
    # reconsider all recent records.
    class ConsistencyError < RuntimeError
      def initialize(table_name, delta, description="")
        @table_name  = table_name
        @delta       = delta
        @description = description
      end

      def message
        output = "%s had a count difference of %i" % [@table_name, @delta]
        output = output + "; " + @description if !@description.empty?
      end
    end
  end
end
