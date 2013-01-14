require 'tempfile'

module Sq::Dbsync
  # Provide extra functionality on top of the standard tempfile API.
  class TempfileFactory

    # ENV['TMPDIR'] is explicitly referenced here, since a change to JRuby in
    # 1.7.0 makes `Dir.tmpdir` preference non-world writable directories first,
    # of which `.` is a member. This makes it impossible to configure a world
    # writable directory solely via the environment.
    def self.make(name)
      Tempfile.new(name, ENV['TMPDIR'] || Dir.tmpdir)
    end

    def self.make_with_content(name, content)
      file = make(name)
      file.write(content)
      file.flush
      file
    end

    # A world writable file is necessary if it is being used as a communication
    # mechanism with other processes (such as MySQL `LOAD DATA INFILE`).
    def self.make_world_writable(name)
      file = make(name)
      file.chmod(0666)
      file
    end

    def self.split(file, n, &block)
      `split -l #{n} #{file.path} #{file.path}.`
      Dir[file.path + '.*'].each do |tempfile|
        block.call(tempfile)
        FileUtils.rm(tempfile)
      end
    end

  end
end
