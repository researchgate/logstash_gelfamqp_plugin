This logstash output plugin converts a logstash message to a GELF message that can be consumed by graylog2 and then pushes them to an amqp exchange.

Usage example:
--------------

   gelfamqp {
        host => "amqp.hostname"
        exchange_type => "fanout"
        name => "exchange_name"
    }