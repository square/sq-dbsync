
module Sq::Dbsync

  # Handles redacting sensitive information for error messages, and delegating
  # response to a user-defined handler.
  class ErrorHandler
    def initialize(config)
      @config = config
      @handler = config.fetch(:error_handler, ->(ex) {})
    end

    def wrap(&block)
      begin
        with_massaged_exception(redact_passwords, &block)
      rescue => ex
        handler[ex]

        raise ex
      end
    end

    def redact_passwords
      lambda do |message|
        (
          config[:sources].values + [config[:target]]
        ).compact.inject(message) do |m, options|
          m.gsub(options[:password], 'REDACTED')
        end
      end
    end

    def with_massaged_exception(*massagers)
      yield
    rescue => ex
      message = massagers.inject(ex.message) do |a, v|
        v.call(a)
      end

      raise ex, message, ex.backtrace
    end

    private

    attr_reader :config, :handler
  end

end
