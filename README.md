# broker-nanobot

Nanobot module injector that patches core nanobot startup to register broker-backed channels.

It runs as a nanobot worker process and extends channel support for `mqtt`, `rabbitmq`, and `kafka` without changing base nanobot behavior.

## Channel configuration

Broker transport is configured from nanobot's native config file (`~/.nanobot/config.json`,
container path `/root/.nanobot/config.json`) under `channels`.

### MQTT

```json
{
  "channels": {
    "mqtt": {
      "enabled": true,
      "host": "mqtt",
      "port": 1883,
      "request_topic": "broker/requests/+/+",
      "reply_topic": "broker/replies",
      "qos": 1
    }
  }
}
```

### RabbitMQ

```json
{
  "channels": {
    "rabbitmq": {
      "enabled": true,
      "url": "amqp://guest:guest@rabbitmq:5672/",
      "request_exchange": "broker.requests",
      "request_routing_key": "#",
      "reply_exchange": "broker.replies",
      "request_queue": "broker.requests",
      "reply_queue": "broker.replies",
      "prefetch_count": 1
    }
  }
}
```

### Kafka

```json
{
  "channels": {
    "kafka": {
      "enabled": true,
      "bootstrap_servers": "redpanda:9092",
      "request_topic": "broker.requests",
      "reply_topic": "broker.replies",
      "group_id": "broker-nanobot"
    }
  }
}
```

Enable any of `channels.mqtt`, `channels.rabbitmq`, `channels.kafka` you want to run.

## Nanobot config

Set your provider/model in native nanobot config, for example:

```json
{
  "providers": {
    "openrouter": {
      "apiKey": "sk-or-..."
    }
  },
  "agents": {
    "defaults": {
      "model": "anthropic/claude-opus-4-5"
    }
  }
}
```

## Payload contract

Inbound request payloads are raw UTF-8 text on the configured request route.

For MQTT session routing, use request topics in the form:

`broker/requests/{producer_id}/{session_id}`

Replies are published to:

`broker/replies/{producer_id}/{session_id}`

For RabbitMQ session routing, publish request messages to exchange `broker.requests`
with routing key:

`{producer_id}.{session_id}`

Replies are published to exchange `broker.replies` with the same routing key.

If an inbound RabbitMQ message includes native RPC properties (`reply_to`,
`correlation_id`), reply publishing respects `reply_to` first and copies
`correlation_id` to the response.

For Kafka session routing, publish request messages to topic `broker.requests`
with key:

`{producer_id}/{session_id}`

Replies are published to `broker.replies` using the same key. If the inbound
record includes a `correlation_id` header, it is copied to the reply record.

Outbound replies are raw UTF-8 text on the configured reply route.
