require 'sq/dbsync/loggers'

# Helper class to provide sane defaults to user-supplied config.
class Sq::Dbsync::Config
  def self.make(hash)
    {
      clock:         ->{ Time.now.utc },
      logger:        Sq::Dbsync::Loggers::Stream.new,
      error_handler: ->(e) { $stderr.puts(e.message, e.backtrace) }
    }.merge(hash)
  end
end
