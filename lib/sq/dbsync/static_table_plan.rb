# Generates a plan given a static "spec" of tables, columns, optional data
# types, and indexes. This is useful for partial replication of tables, for
# instance when some columns contain sensitive or large information that is not
# to be replicated.
#
# Simple Example:
#
#  spec = [{
#    table_name: :users,
#    columns: [:id, :updated_at],
#    indexes: {
#      index_users_on_updated_at: {columns: [:updated_at]}
#    }
#  }]
class Sq::Dbsync::StaticTablePlan
  def initialize(spec)
    @spec = format_spec(spec)
  end

  def tables(source)
    deep_clone(@spec).map do |tplan|
      tplan.update(source_db: source)
    end
  end

  def deep_clone(object)
    Marshal.load(Marshal.dump(object))
  end

  def format_spec(spec)
    # Support copying from different relations of a Postgres DB, but to the
    # same target database in MySQL.
    spec.map do |table_def|
      unless table_def[:source_table_name]
        table_def[:source_table_name] = table_def[:table_name]
        table_def[:table_name] = table_def[:source_table_name].
          to_s.gsub('__', '_').to_sym
      end
      table_def
    end
  end
end
