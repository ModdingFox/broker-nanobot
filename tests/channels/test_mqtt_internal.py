import asyncio

from broker_nanobot.channels.mqtt import MQTTBrokerChannel
from broker_nanobot.config.schema import MQTTConfig


class _FakeBus:
    def __init__(self) -> None:
        self.inbound = []

    async def publish_inbound(self, msg):
        self.inbound.append(msg)


def test_mqtt_consume_returns_when_not_initialized() -> None:
    async def run() -> None:
        mqtt = MQTTBrokerChannel(MQTTConfig(), _FakeBus())
        await mqtt._consume()

    asyncio.run(run())
