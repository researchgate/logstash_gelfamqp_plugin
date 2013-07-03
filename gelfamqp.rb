require "logstash/outputs/base"
require "logstash/namespace"

# Push events to an AMQP exchange.
#
# AMQP is a messaging system. It requires you to run an AMQP server or 'broker'
# Examples of AMQP servers are [RabbitMQ](http://www.rabbitmq.com/) and
# [QPid](http://qpid.apache.org/)
class LogStash::Outputs::GelfAmqp < LogStash::Outputs::Base
    MQTYPES = [ "fanout", "direct", "topic" ]
    
    config_name "gelfamqp"
    plugin_status "beta"
    
    # Your amqp server address
    config :host, :validate => :string, :required => true
    
    # The AMQP port to connect on
    config :port, :validate => :number, :default => 5672
    
    # Your amqp username
    config :user, :validate => :string, :default => "guest"
    
    # Your amqp password
    config :password, :validate => :password, :default => "guest"
    
    # The exchange type (fanout, topic, direct)
    config :exchange_type, :validate => [ "fanout", "direct", "topic"], :required => true
    
    # The name of the exchange
    config :name, :validate => :string, :required => true
    
    # Key to route to by default. Defaults to 'logstash'
    #
    # * Routing keys are ignored on fanout exchanges.
    config :key, :validate => :string, :default => "logstash"
    
    # The vhost to use
    config :vhost, :validate => :string, :default => "/"
    
    # Is this exchange durable? (aka; Should it survive a broker restart?)
    config :durable, :validate => :boolean, :default => true
    
    # Should messages persist to disk on the AMQP broker until they are read by a
    # consumer?
    config :persistent, :validate => :boolean, :default => true
    
    # Enable or disable debugging
    config :debug, :validate => :boolean, :default => false
    
    # Enable or disable SSL
    config :ssl, :validate => :boolean, :default => false
    
    # Validate SSL certificate
    config :verify_ssl, :validate => :boolean, :default => false
    
    ############# GELF settings
    
    
    # Allow overriding of the gelf 'sender' field. This is useful if you
    # want to use something other than the event's source host as the
    # "sender" of an event. A common case for this is using the application name
    # instead of the hostname.
    config :sender, :validate => :string, :default => "%{@source_host}"
    
    # The GELF message level. Dynamic values like %{level} are permitted here;
    # useful if you want to parse the 'log level' from an event and use that
    # as the gelf level/severity.
    #
    # Values here can be integers [0..7] inclusive or any of
    # "debug", "info", "warn", "error", "fatal", "unknown" (case insensitive).
    # Single-character versions of these are also valid, "d", "i", "w", "e", "f",
    # "u"
    config :level, :validate => :array, :default => [ "%{severity}", "INFO" ]
    
    # The GELF facility. Dynamic values like %{foo} are permitted here; this
    # is useful if you need to use a value from the event as the facility name.
    config :facility, :validate => :array, :default => [ "%{facility}" , "logstash-gelf" ]
    
    # Ship metadata within event object?
    config :ship_metadata, :validate => :boolean, :default => true
    
    # The GELF custom field mappings. GELF supports arbitrary attributes as custom
    # fields. This exposes that. Exclude the `_` portion of the field name
    # e.g. `custom_fields => ['foo_field', 'some_value']
    # sets `_foo_field` = `some_value`
    config :custom_fields, :validate => :hash, :default => {}
    
    public
    def register
        require "bunny" # rubygem 'bunny'
        
        @logger.info("Registering output", :plugin => self)
        connect
        
        @level_map = {
            "debug" => 0, "d" => 0,
            "info" => 1, "i" => 1,
            "notice" => 2, "n" => 2,
            "warn" => 3, "w" => 3,
            "error" => 4, "e" => 4,
            "fatal" => 5, "f" => 5,
            "alert" => 6, "a" => 6,
            "unknown" => 1, "u" => 1,
        }
    end # def register
    
    public
    def connect
        amqpsettings = {
            :vhost => @vhost,
            :host => @host,
            :port => @port,
            :logging => @debug,
        }
        amqpsettings[:user] = @user if @user
        amqpsettings[:pass] = @password.value if @password
        amqpsettings[:ssl] = @ssl if @ssl
        amqpsettings[:verify_ssl] = @verify_ssl if @verify_ssl
        
        begin
            @logger.debug("Connecting to AMQP", :settings => amqpsettings,
                          :exchange_type => @exchange_type, :name => @name)
            @bunny = Bunny.new(amqpsettings)
            @bunny.start
            rescue => e
            if terminating?
                return
                else
                @logger.error("AMQP connection error (during connect), will reconnect",
                              :exception => e, :backtrace => e.backtrace)
                sleep(1)
                retry
            end
        end
        
        @logger.debug("Declaring exchange", :name => @name, :type => @exchange_type,
                      :durable => @durable)
        @exchange = @bunny.exchange(@name, :type => @exchange_type.to_sym, :durable => @durable)
        
        @logger.debug("Binding exchange", :name => @name, :key => @key)
    end # def connect
    
    public
    def receive(event)
        return unless output?(event)
        
        @logger.debug("Sending event", :destination => to_s, :event => event,
                      :key => key)
        key = event.sprintf(@key) if @key
        
        
        #### GELFIFY
        
        # We have to make our own hash here because GELF expects a hash
        # with a specific format.
        m = Hash.new
        if event.fields["message"]
            v = event.fields["message"]
            m["short_message"] = (v.is_a?(Array) && v.length == 1) ? v.first : v
            event.fields.delete("message")
            else
            m["short_message"] = event.message
        end
        
        m["full_message"] = (event.message)
        
        m["host"] = event.sprintf(@sender)
        
        if event.fields["File"]
            v = event.fields["File"]
            m["file"] = (v.is_a?(Array) && v.length == 1) ? v.first : v
            event.fields.delete("File")
        else
            m["file"] = event["@source_path"]
        end
        
        if event.fields["Line"]
            v = event.fields["Line"]
            m["line"] = (v.is_a?(Array) && v.length == 1) ? v.first : v
            event.fields.delete("Line")
        end
        
        # set facility as defined
        if event.fields["facility"]
            v = event.fields["facility"]
            m["facility"] = (v.is_a?(Array) && v.length == 1) ? v.first : v
            event.fields.delete("facility")
            else
            m["facility"] = event.sprintf(@facility)
        end
        
        # Probe severity array levels
        level = nil
        if event.fields["severity"]
            v = event.fields["severity"]
            level = (v.is_a?(Array)) ? v.first : v
            event.fields.delete("severity")
            elsif @level.is_a?(Array)
            @level.each do |value|
                parsed_value = event.sprintf(value)
                if parsed_value
                    level = parsed_value
                    break
                end
            end
            else
            level = event.sprintf(@level.to_s)
        end
        m["level"] = (@level_map[level.downcase] || level).to_i
        
        if @ship_metadata
            event.fields.each do |name, value|
                next if value == nil
                name = "_id" if name == "id"  # "_id" is reserved, so use "__id"
                if !value.nil?
                    if value.is_a?(Array)
                        # collapse single-element arrays, otherwise leave as array
                        m["_#{name}"] = (value.length == 1) ? value.first : value
                        else
                        # Non array values should be presented as-is
                        # https://logstash.jira.com/browse/LOGSTASH-113
                        m["_#{name}"] = value
                    end
                end
            end
        end
        
        if @custom_fields
            @custom_fields.each do |field_name, field_value|
                m["_#{field_name}"] = field_value unless field_name == 'id'
            end
        end
        
        # Allow 'INFO' 'I' or number. for 'level'
        m["timestamp"] = event.unix_timestamp.to_i
        m["version"] = "1.0"
        
        @logger.debug(["Sending GELF event", m])
        
        #### END GELFIFY
        
        
        begin
            receive_raw(m.to_json, key)
            rescue JSON::GeneratorError => e
            @logger.warn("Trouble converting event to JSON", :exception => e,
                         :event => m)
            return
        end
    end # def receive
    
    public
    def receive_raw(message, key=@key)
        begin
            if @exchange
                @logger.debug(["Publishing message", { :destination => to_s, :message => message, :key => key }])
                @exchange.publish(message, :persistent => @persistent, :key => key)
                else
                @logger.warn("Tried to send message, but not connected to amqp yet.")
            end
            rescue *[Bunny::ServerDownError, Errno::ECONNRESET] => e
            @logger.error("AMQP connection error (during publish), will reconnect: #{e}")
            connect
            retry
        end
    end
    
    public
    def to_s
        return "amqp://#{@user}@#{@host}:#{@port}#{@vhost}/#{@exchange_type}/#{@name}\##{@key}"
    end
    
    public
    def teardown
        @bunny.close rescue nil
        @bunny = nil
        @exchange = nil
        finished
    end # def teardown
end # class LogStash::Outputs::Amqp