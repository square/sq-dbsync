Square Dbsync
=============

An extract and load system to shunt data between databases.

It uses timestamp based replication which is fast and easy to keep running,
but has some caveats. Most notably, it does not handle deletes well (see
documentation below for details).

This was useful to us at Square because we needed partial (only select
columns), continuous replication from both MySQL and PostgreSQL databases to a
single target database with some basic ETL logic along the way. None of the
existing solutions were able to do this adequately.

At some point you will need to bite the bullet and implement a real ETL system,
but `sq-dbsync` can tide you over until you get there.

dbsync is MySQL `utf8mb4` clean: it will correctly handle four-byte
UTF8 characters like emojis. Under JRuby, you'll need to have the
server character set configured to `utf8mb4`. The specs include tests
for this; they'll fail if you run them under JRuby against MySQL with
different server character set.

Usage
-----

```
gem install sq-dbsync
```

``` Ruby
require 'sq/dbsync'

include Sq::Dbsync

# Config will typically differ per environment.
config = {
  sources: {
    db_a: {
      database: 'db_a_production',
      user:     'sqdbsync-ro',
      password: 'password',
      host:     'db-a-host',
      brand:    'mysql',
      port:     3306,
    },
    db_b: {
      database: 'db_b_production',
      user:     'sqdbsync-ro',
      password: 'password',
      host:     'db-b-host',
      brand:    'postgresl',
      port:     5432,
    }
  },
  target: {
    database: 'replica',
    user:     'sqdbsync',
    password: 'password',

    # Only localhost supported, since `LOAD DATA INFILE` is used which
    # requires a shared temp directory.
    host:     'localhost',
    brand:    'mysql',
    port:     3306,
  },

  # Optional configuration
  logger: Loggers::Stream.new,     # A graphite logger is provided, see source.
  clock: ->{ Time.now.utc },       # In test env it can be useful to fix this.
  error_handler: ->(e) { puts(e) } # Notify your exception system
}

# Write plans that specify how data is replicated.
DB_A_PLAN = [{
  table_name: :users,
  columns: [
    # You must replicate the primary key.
    :id,

    # You must replicate a timestamp column, and it should be indexed on the
    # target system.
    :updated_at,

    # Then whatever other columns you require.
    :name,
    :account_type,
    :created_at,

  ],
  indexes: {
    # Indexing it on the source system is optional
    index_users_on_updated_at: {:columns=>[:updated_at], :unique=>false},
  },

  # Basic schema transformations are supported.
  db_types: {
    :account_type => [:enum, %w(
      bronze
      silver
      gold
    )]
  }
},{
  table_name: :account_types,
  source_table_name: :user_account_types,
  columns: :all
}]

plans = [
  [StaticTablePlan.new(DB_A_PLAN), :db_a],
  [AllTablesPlan.new, :db_b]
]

manager = Manager.new(config, plans)

# Run a batch load nightly
manager.batch(ALL_TABLES)

# Run an incremental load continuously
manager.increment

# You can load a subset of tables if necessary
manager.batch([:users])
```

Documentation
-------------

### Plan Options

* `batch_load` whether or not to batch load this table in the default batch
  load. If the table is specifically requested, it will be loaded regardless of
  this setting. (default: true)
* `charset` charset to use when creating the table. Passed directly through to
  [Sequel::MySQL::Database#connect](http://sequel.rubyforge.org/rdoc-adapters/classes/Sequel/MySQL/Database.html).
  MySQL only, ignored for Postgres. (default: 'utf8')
* `columns` Either an array of columns to replicate, or `:all` indicating that
  all columns should be replicated. (required)
* `consistency` Perform a basic consistency check on the table regularly during
  the incremental load by comparing recent counts of the source and target
  tables. Make sure you have a timestamp index on both tables! This was
  particularly useful when developing the project, but honestly probably isn't
  that useful now --- I can't remember the last time I saw an error from this.
  (default: false)
* `db_types` A hash that allows you to modify the target schema from the
  source. See the example in usage section above. (default: `{}`)
* `indexes` A hash defining desired indexes on the target table. Indexes are
  *not* copied from source tables. See example in usage section above.
  (default: `{}`)
* `refresh_recent` Some table are too large to batch load regularly, but
  modifications are known to be recent. This setting will cause the last two
  days of data to be dropped an recreated as part of the nightly batch load.
  (default: false)
* `source_table_name` Allows the source and target tables to be named
  differently. (default: `table_name` configuration option)
* `timestamp_table_name` A hack to workaround the postgres query planner
  failing to use indexes correctly for `MAX()` on a view that uses `UNION`
  under the covers. If this describes your source view, and one of the
  underlying tables is guaranteed to contain the latest record you can set this
  value to that and it will be used for all timestamp related queries. If not,
  you must provide a custom view that supports a `MAX` query with a sane query
  plan. (default: nil)
* `table_name` The name of the table to be replicated. If `source_table_name`
  is specified, this option defines the name of the table in the target
  database only.
* `primary_key` Usually the primary key can be inferred from the source schema,
  but if you are replicating from a view you will need to specify it explictly
  with this option. Should be an array of symbols. (default: nil, will
  auto-detect from source schema)
* `timestamp` The column to treat as a timestamp. Must be a member of the
  `:columns` option. (default: select `updated_at` or `created_at`, in that
  order)
* `type_casts` a hash for specifying column-level type casting from postgres
  data types to mysql data types. Primarily intended for custom postgres type
  defintions that have no mysql equivalent. The hash key is the column name,
  and the value is the desired mysql column type. See the
  `Sq::Dbsync::Database::Postgres::CASTS` constant for the formant of the hash.
  Example:

```ruby
{
  :table_name => :some_table,
  :columns => [ :custom_postgres_type ],
  :type_casts => { :custom_postgres_type => "varchar(255)" }
}
```

### Handling Deletes

The incremental load has no way of detecting deleted records. The nightly batch
load will reload all tables, so there will be at most a one day turn-around on
deletes. Some tables will be too big to batch load every night however, so this
is not a great solution in that case.

If you have an "audit" table that contains enough data for you to reconstruct
deletes in other tables, then you can provide a custom subclass to the
incremental loader that will be able to run this logic.

``` ruby
class IncrementalLoadWithDeletes < Sq::Dbsync::IncrementalLoadAction
  def process_deletes
    if plan.table_name == :audit_logs
      ExampleRecordDestroyer.run(db, registry, :audit_logs, :other_table)
    end
  end
end

CONFIG = {
  # ...
  incremental_action: IncrementalLoadWithDeletes,
}
```

See `lib/sq/dbsync/example_record_destroyer` for a sample implementation.

### Database Settings

If your target database is MySQL, we recommend that you ensure it is running
under the `READ COMMITTED` isolation level. This makes it much harder for an
analyst to lock a table and block replication. (Statements like `CREATE TABLE
AS SELECT FROM ...` tend to be the culprit.)

Developing
----------

    bundle
    bundle exec rake

Compatibility
-------------

Requires 1.9. Tested on CRuby 1.9.3 and JRuby.

## Support

Make a [new github issue](https://github.com/square/sq-dbsync/issues/new).

## Contributing

Fork and patch! Before any changes are merged to master, we need you to sign an
[Individual Contributor
Agreement](https://spreadsheets.google.com/a/squareup.com/spreadsheet/viewform?formkey=dDViT2xzUHAwRkI3X3k5Z0lQM091OGc6MQ&ndplr=1)
(Google Form).
