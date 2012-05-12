require 'sq/dbsync/batch_load_action'
require 'sq/dbsync/incremental_load_action'
require 'sq/dbsync/pipeline'
require 'sq/dbsync/table_registry'
require 'sq/dbsync/consistency_verifier'
require 'sq/dbsync/database/connection'

# The manager orchestrates the high level functions of the sync, such as
# keeping the active database up-to-date, and batch loading into the
# non-active.
#
# This is the main entry point for the application.
class Sq::Dbsync::Manager
  include Sq::Dbsync

  EPOCH = Date.new(2000, 1, 1).to_time

  def initialize(config, plans)
    @config = config
    @plans  = plans
  end

  def batch_nonactive(tables = [])
    measure(:batch_total) do
      run_load(BatchLoadAction, Pipeline::ThreadedContext, tables)
    end

    measure(:purge) do
      purge_old_tables
    end if tables.empty?
  end

  def increment_active
    @running = true
    counter = 0

    loop_with_retry_on(->{ @running }, transient_exceptions) do
      increment_active_once

      counter = (counter + 1) % 100
      if counter == 1
        # No need to do this every cycle, 100 is chosen to be as good as any
        # other number. It should run on the very first cycle however so that
        # the specs will cover it.
        verifier.check_consistency!(tables_to_load)
      end
    end
  end

  def increment_active_once
    # ThreadedContext would be ideal here, but it leaks memory in JRuby. Not
    # sure why yet, but mass creation of threads seems like an obvious
    # candidate for brokenness.
    run_load(incremental_action, Pipeline::SimpleContext)

    purge_registry
  end

  def stop!
    @running = false
  end

  def target
    @target ||= Sq::Dbsync::Database::Connection.create(config[:target])
  end

  def tables_to_load
    plans_with_sources.map do |plan, source|
      plan.tables(source).map do |x|
        x.update(source_db: source)
      end
    end.reduce([], :+).uniq {|x| x[:table_name] }
  end

  def plans_with_sources
    @plans_with_sources ||= plans.map do |plan, source_name|
      [plan, sources.fetch(source_name)]
    end
  end

  def sources
    @sources ||= Hash[config[:sources].map do |name, opts|
      [name, Sq::Dbsync::Database::Connection.create(opts)]
    end]
  end

  attr_accessor :config, :plans

  private

  def run_load(action, context, tables = [])
    items = tables_to_load.map do |tplan|
      next if tables.any? && !tables.include?(tplan[:table_name])

      action.new(target, tplan, registry, logger, config[:clock])
    end.compact
    raise_if_pipeline_failure(
      Pipeline.new(items, *LoadAction.stages).run(context)
    )
  end

  def purge_old_tables
    (target.tables - expected_table_names).each {|table|
      next if table.to_s =~ /\Ameta_/

      target.drop_table(table)
    }
  end

  # This is necessary so that old tables that are no longer being synced do not
  # break our lag calculations.
  def purge_registry
    registry.purge_except(expected_table_names)
  end

  def expected_table_names
    tables_to_load.map {|x| x[:table_name] } + [
      :permissions_user_deletes,
      :freeze_deletes
    ]
  end

  def loop_with_retry_on(guard, transient_exceptions, &block)
    consecutive_fails = 0

    while guard.call
      begin
        block.call
        consecutive_fails = 0
      rescue *transient_exceptions
        consecutive_fails += 1
        raise if consecutive_fails >= 3
      end
    end
  end

  def raise_if_pipeline_failure(results)
    results.each do |result|
      if result.is_a?(Pipeline::Failure)
        raise result.wrapped_exception
      end
    end
  end

  def measure(label, &block)
#     logger.measure(label) do
      block.call
#     end
  end

  def registry
    TableRegistry.new(target)
  end

  def verifier
    @verifier ||= ConsistencyVerifier.new(target, registry)
  end

  def logger
    config[:logger]
  end

  def transient_exceptions
    [
      Database::ExtractError,
      Database::TransientError
    ]
  end

  def incremental_action
    config.fetch(:incremental_action, IncrementalLoadAction)
  end

end
