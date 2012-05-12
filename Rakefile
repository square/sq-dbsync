begin
  require 'rspec/core/rake_task'

  RSpec::Core::RakeTask.new(:spec) do |t|
    t.rspec_opts = '-b'
  end

  task :default => :spec
rescue LoadError
  $stderr.puts "rspec not available, spec task not provided"
end

begin
  require 'cane/rake_task'

  desc "Run cane to check quality metrics"
  Cane::RakeTask.new(:quality) do |cane|
    cane.abc_max = 12
    cane.add_threshold 'coverage/covered_percent', :>=, 95
  end

  task :default => :quality if RUBY_PLATFORM != 'java'
rescue LoadError
  warn "cane not available, quality task not provided."
end
