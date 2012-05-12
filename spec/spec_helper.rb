ENV['APP_ENV'] = 'test'

if ENV['COVERAGE'] != "0" && RUBY_PLATFORM != 'java'
  require 'simplecov'

  class SimpleCov::Formatter::MergedFormatter
    def format(result)
      SimpleCov::Formatter::HTMLFormatter.new.format(result)
      File.open("coverage/covered_percent", "w") do |f|
        f.puts result.source_files.covered_percent.to_i
      end
    end
  end

  SimpleCov.formatter = SimpleCov::Formatter::MergedFormatter
  SimpleCov.start do
    add_filter "/config/"
    add_filter "/spec/"
  end
end

module Sq
  module Dbsync
  end
end

SQD = Sq::Dbsync
