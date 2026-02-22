"""Config schema extensions for broker-nanobot."""

from __future__ import annotations

from pydantic import Field

from nanobot.config.schema import Base, ChannelsConfig, Config


class MQTTConfig(Base):
    enabled: bool = False
    host: str = "mqtt"
    port: int = 1883
    username: str = ""
    password: str = ""
    request_topic: str = "broker/requests"
    reply_topic: str = "broker/replies"
    qos: int = 1


class RabbitMQConfig(Base):
    enabled: bool = False
    url: str = "amqp://guest:guest@rabbitmq:5672/"
    request_exchange: str = "broker.requests"
    request_routing_key: str = "#"
    reply_exchange: str = "broker.replies"
    request_queue: str = "broker.requests"
    reply_queue: str = "broker.replies"
    prefetch_count: int = 1


class KafkaConfig(Base):
    enabled: bool = False
    bootstrap_servers: str = "redpanda:9092"
    request_topic: str = "broker.requests"
    reply_topic: str = "broker.replies"
    group_id: str = "broker-nanobot"


class ExtendedChannelsConfig(ChannelsConfig):
    mqtt: MQTTConfig = Field(default_factory=MQTTConfig)
    rabbitmq: RabbitMQConfig = Field(default_factory=RabbitMQConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)


class ExtendedConfig(Config):
    channels: ExtendedChannelsConfig = Field(default_factory=ExtendedChannelsConfig)
