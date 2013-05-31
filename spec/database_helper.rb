require 'sq/dbsync/database/connection'

def db_options(opts)
  opts = {
    user: 'root',
    host: 'localhost',
    brand: 'mysql',
    port: opts[:brand] == 'postgresql' ? 5432 : 3306
  }.merge(opts)

  if RUBY_PLATFORM == 'java'
    opts.merge(
      adapter: "jdbc",
      uri: begin
        base = 'jdbc:%s://%s:%i/%s?user=%s' % [
          opts.fetch(:brand),
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
    {
      adapter: opts[:brand] == 'postgresql' ? 'postgres' : 'mysql2',
    }.merge(opts)
  end
end

TEST_SOURCES = {
  source:     db_options(database: 'sq_dbsync_test_source'),
  mb4_source: db_options(database: 'sq_dbsync_mb4_test_source', charset: "utf8mb4"),
  alt_source: db_options(database: 'sq_dbsync_test_source_alt'),
  postgres:   db_options(
    user:     `whoami`.chomp,
    brand:    'postgresql',
    host:     'localhost',
    database: 'sq_dbsync_pg_test_source'
  )
}
TEST_TARGET = db_options(database: 'sq_dbsync_test_target')
MB4_TEST_TARGET = db_options(database: 'sq_dbsync_test_target', charset:"utf8mb4")

$target = nil
def test_target
  $target ||= SQD::Database::Connection.create(TEST_TARGET, :target)
end

$sources = {}
def test_source(name)
  $sources[name] ||= SQD::Database::Connection.create(
    TEST_SOURCES.fetch(name), :source
  )
end

RSpec.configure do |config|
  db_specs_only = {
    example_group: {file_path: /spec\/(integration|acceptance)/}
  }

  config.before(:suite, db_specs_only) do
    (TEST_SOURCES.values + [TEST_TARGET]).each do |opts|
      db = opts.fetch(:database)

      case opts.fetch(:brand)
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
