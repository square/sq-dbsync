require 'sq/dbsync/database/connection'

def db_options(opts)
  opts = {
    user:  'root',
    host:  'localhost',
    adapter: 'mysql2',
    port: opts[:adapter] == 'postgres' ? 5432 : 3306
  }.merge(opts)

  opts = opts.merge(
    type: {
      'mysql2'   => 'mysql',
      'postgres' => 'postgresql'
    }.fetch(opts.fetch(:adapter))
  )

  if RUBY_PLATFORM == 'java'
    opts.merge(
      adapter: "jdbc",
      uri: begin
        base = 'jdbc:%s://%s:%i/%s?user=%s' % [
          opts.fetch(:type),
          opts.fetch(:host),
          opts.fetch(:port),
          opts.fetch(:database),
          opts.fetch(:user)
        ]
        if opts[:password]
          base += '&password=%s' % opts[:password]
        end
        base
      end
    )
  else
    opts
  end
end

TEST_SOURCES = {
  source:     db_options(database: 'sq_dbsync_test_source'),
  alt_source: db_options(database: 'sq_dbsync_test_source_alt'),
  postgres:   db_options(
    user:     `whoami`.chomp,
    adapter:  'postgres',
    host:     'localhost',
    database: 'sq_dbsync_pg_test_source'
  )
}
TEST_TARGET = db_options(database: 'sq_dbsync_test_target')

$target = nil
def test_target
  $target ||= SQD::Database::Connection.create(TEST_TARGET)
end

$sources = {}
def test_source(name)
  $sources[name] ||= SQD::Database::Connection.create(TEST_SOURCES.fetch(name))
end

RSpec.configure do |config|
  db_specs_only = {
    example_group: {file_path: /spec\/(integration|acceptance)/}
  }

  config.before(:suite, db_specs_only) do
    (TEST_SOURCES.values + [TEST_TARGET]).each do |opts|
      db = opts.fetch(:database)

      case opts.fetch(:type)
      when 'mysql'
        `mysql -u root -e "drop database if exists #{db}"`
        `mysql -u root -e "create database #{db}"`
      when 'postgresql'
        `dropdb #{db}`
        `createdb #{db}`
      else
        raise "Unknown database: #{opts.inspect}"
      end
    end
  end

  config.before(:each, db_specs_only) do
    (TEST_SOURCES.keys.map {|x| test_source(x) } + [test_target]).each do |db|
      db.tables.each do |name|
        db.drop_table(name)
      end
    end
  end
end
