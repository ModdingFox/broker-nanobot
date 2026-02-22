import asyncio
from nanobot.bus.events import OutboundMessage

from broker_nanobot.channels import mqtt as mqtt_channel_mod
from broker_nanobot.config.schema import MQTTConfig


class FakeBus:
    def __init__(self) -> None:
        self.inbound = []

    async def publish_inbound(self, msg):
        self.inbound.append(msg)


class FakeClient:
    def __init__(self) -> None:
        self.replies: list[str] = []
        self.topics: list[str] = []

    async def publish(self, _topic: str, payload: str, qos: int = 0):
        _ = qos
        self.topics.append(_topic)
        self.replies.append(payload)


class FakeIncoming:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


def test_mqtt_channel_start_process_send_stop() -> None:
    async def run() -> None:
        bus = FakeBus()
        ch = mqtt_channel_mod.MQTTBrokerChannel(MQTTConfig(), bus)
        fake_client = FakeClient()
        ch._client = fake_client
        ch._running = True

        await ch._process_request("hello")
        assert len(bus.inbound) == 1
        assert bus.inbound[0].content == "hello"
        assert bus.inbound[0].chat_id

        msg = OutboundMessage(channel="broker", chat_id="unused", content="world")
        await ch.send(msg)
        assert fake_client.replies == ["world"]

    asyncio.run(run())


def test_mqtt_channel_send_without_transport() -> None:
    async def run() -> None:
        ch = mqtt_channel_mod.MQTTBrokerChannel(MQTTConfig(), FakeBus())
        msg = OutboundMessage(channel="broker", chat_id="missing", content="x")
        await ch.send(msg)

        fake_client = FakeClient()
        ch._client = fake_client
        await ch.send(msg)
        assert fake_client.replies == ["x"]

    asyncio.run(run())


def test_mqtt_process_request() -> None:
    async def run() -> None:
        bus = FakeBus()
        ch = mqtt_channel_mod.MQTTBrokerChannel(MQTTConfig(), bus)

        await ch._process_request("standalone")
        assert len(bus.inbound) == 1
        assert bus.inbound[0].content == "standalone"
        assert bus.inbound[0].chat_id

    asyncio.run(run())


def test_mqtt_parse_route_from_topic_and_store_reply_topic() -> None:
    async def run() -> None:
        bus = FakeBus()
        ch = mqtt_channel_mod.MQTTBrokerChannel(MQTTConfig(), bus)

        sender_id, chat_id, reply_topic = ch._parse_request_route(
            FakeIncoming("broker/requests/client-a/thread-7", b"hello")
        )

        assert sender_id == "client-a"
        assert chat_id == "client-a/thread-7"
        assert reply_topic == "broker/replies/client-a/thread-7"

        await ch._process_request(
            "hello",
            sender_id=sender_id,
            chat_id=chat_id,
            reply_topic=reply_topic,
        )

        assert len(bus.inbound) == 1
        assert bus.inbound[0].sender_id == "client-a"
        assert bus.inbound[0].chat_id == "client-a/thread-7"
        assert bus.inbound[0].metadata["reply_topic"] == "broker/replies/client-a/thread-7"

    asyncio.run(run())


def test_mqtt_send_prefers_reply_topic_from_metadata() -> None:
    async def run() -> None:
        ch = mqtt_channel_mod.MQTTBrokerChannel(MQTTConfig(), FakeBus())
        fake_client = FakeClient()
        ch._client = fake_client
        msg = OutboundMessage(
            channel="broker",
            chat_id="client-a/thread-7",
            content="world",
            metadata={"reply_topic": "broker/replies/client-a/thread-7"},
        )

        await ch.send(msg)

        assert fake_client.topics == ["broker/replies/client-a/thread-7"]
        assert fake_client.replies == ["world"]

    asyncio.run(run())
