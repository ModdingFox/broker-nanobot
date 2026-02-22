"""Entry point for broker-nanobot."""

from __future__ import annotations

from loguru import logger

from broker_nanobot.channels.kafka import KafkaBrokerChannel
from broker_nanobot.channels.mqtt import MQTTBrokerChannel
from broker_nanobot.channels.rabbitmq import RabbitMQBrokerChannel
from broker_nanobot.config.schema import ExtendedConfig


def _patch_nanobot_config() -> None:
    """Patch nanobot loader/schema to use broker-extended config."""
    import nanobot.config.loader as loader
    import nanobot.config.schema as schema

    loader.Config = ExtendedConfig
    schema.Config = ExtendedConfig


def _patch_channel_manager() -> None:
    from nanobot.channels.manager import ChannelManager

    original_init_channels = ChannelManager._init_channels

    def patched_init_channels(self: ChannelManager) -> None:
        original_init_channels(self)

        if self.config.channels.mqtt.enabled:
            self.channels[MQTTBrokerChannel.name] = MQTTBrokerChannel(
                self.config.channels.mqtt,
                self.bus,
            )
            logger.info("MQTT broker channel enabled")

        if self.config.channels.rabbitmq.enabled:
            self.channels[RabbitMQBrokerChannel.name] = RabbitMQBrokerChannel(
                self.config.channels.rabbitmq,
                self.bus,
            )
            logger.info("RabbitMQ broker channel enabled")

        if self.config.channels.kafka.enabled:
            self.channels[KafkaBrokerChannel.name] = KafkaBrokerChannel(
                self.config.channels.kafka,
                self.bus,
            )
            logger.info("Kafka broker channel enabled")

    ChannelManager._init_channels = patched_init_channels  # type: ignore[method-assign]


def main() -> None:
    _patch_nanobot_config()
    _patch_channel_manager()

    from nanobot.cli.commands import app as nanobot_app

    nanobot_app()


if __name__ == "__main__":
    main()
