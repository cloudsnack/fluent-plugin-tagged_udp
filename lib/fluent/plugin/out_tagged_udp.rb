module Fluent
  class TaggedUdpOutput < Output

    # First, register the plugin. NAME is the name of this plugin
    # and identifies the plugin in the configuration file.
    Fluent::Plugin.register_output('tagged_udp', self)

    config_param :host, :string, :default => '127.0.0.1'
    config_param :port, :integer, :default => 1883

    require 'socket'

    # This method is called before starting.
    # 'conf' is a Hash that includes configuration parameters.
    # If the configuration is invalid, raise Fluent::ConfigError.
    def configure(conf)
      super
      @socket = UDPSocket.new
    end

    def emit(tag, es, chain)
      begin
        es.each {|time,record|
          @socket.send(
            # tag is inserted into the head of the message
            "#{record.to_json}", 0, @host, @port
          )
        }
        $log.flush
        chain.next
      rescue StandardError => e
        $log.debug "#{e.class}: #{e.message}"
      end
    end
  end
end
