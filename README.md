SqDbsync
========

An extract and load system to shunt data between databases.

Usage
-----

``` Ruby
include Sq::Dbsync

# TODO: Provide sane defaults for logger, clock, port numbers, JDBC
# TODO: Is brand required anymore? It's kinda weird.
# TODO: Move Application logic inside the gem, allow exception handler to be
# specified

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

  logger: Loggers::Stream.new,
  clock: ->{ Time.now.utc }
}

# Write plans that specify how data is replicated.
DB_A_PLAN = [{
  table_name: :users,
  columns: [
    :id,
    :name,
    :account_type,
    :created_at,
    :updated_at,
  ],
  indexes: {
    index_users_on_updated_at: {:columns=>[:updated_at], :unique=>false},
  },
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

# TODO: Rename these methods to something more sane, collapse batch and
# refresh.

# Run a batch load nightly
manager.batch_nonactive(ALL_TABLES)
manager.refresh_recent(ALL_TABLES)

# Run an incremental load continuously
manager.increment_nonactive

# You can load a subset of tables if necessary
manager.batch_nonactive([:users])
```

Documentation
-------------

TODO

Plan options
READ COMMITTED
Handling deletes

Developing
----------

    bundle
    bundle exec rake

