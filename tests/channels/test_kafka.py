import asyncio

from nanobot.bus.events import OutboundMessage

from broker_nanobot.channels.kafka import KafkaBrokerChannel
from broker_nanobot.config.schema import KafkaConfig


class _FakeBus:
    def __init__(self) -> None:
        self.inbound = []

    async def publish_inbound(self, msg):
        self.inbound.append(msg)


class _FakeMessage:
    def __init__(
        self,
        value: bytes,
        key: bytes | None = None,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.value = value
        self.key = key
        self.headers = headers or []


class _FakeConsumer:
    def __init__(self, messages: list[_FakeMessage]) -> None:
        self._messages = list(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class _FakeProducer:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def send_and_wait(self, topic: str, value: bytes, key=None, headers=None) -> None:
        self.calls.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "headers": headers,
            }
        )


def test_kafka_consume_returns_when_not_initialized() -> None:
    async def run() -> None:
        kafka = KafkaBrokerChannel(KafkaConfig(), _FakeBus())
        await kafka._consume()

    asyncio.run(run())


def test_kafka_consume_uses_key_for_sender_and_chat() -> None:
    async def run() -> None:
        bus = _FakeBus()
        kafka = KafkaBrokerChannel(KafkaConfig(), bus)
        kafka._consumer = _FakeConsumer(
            [
                _FakeMessage(
                    value=b"hello",
                    key=b"client_a/session_1",
                    headers=[("correlation_id", b"req-123")],
                )
            ]
        )

        await kafka._consume()

        assert len(bus.inbound) == 1
        inbound = bus.inbound[0]
        assert inbound.sender_id == "client_a"
        assert inbound.chat_id == "client_a/session_1"
        assert inbound.content == "hello"
        assert inbound.metadata["reply_topic"] == "broker.replies"
        assert inbound.metadata["reply_key"] == "client_a/session_1"
        assert inbound.metadata["correlation_id"] == "req-123"

    asyncio.run(run())


def test_kafka_consume_falls_back_for_invalid_key() -> None:
    async def run() -> None:
        bus = _FakeBus()
        kafka = KafkaBrokerChannel(KafkaConfig(), bus)
        kafka._consumer = _FakeConsumer([_FakeMessage(value=b"hello", key=b"invalid")])

        await kafka._consume()

        assert len(bus.inbound) == 1
        inbound = bus.inbound[0]
        assert inbound.sender_id == "broker"
        assert inbound.chat_id
        assert inbound.metadata["reply_topic"] == "broker.replies"
        assert "reply_key" not in inbound.metadata

    asyncio.run(run())


def test_kafka_send_prefers_metadata_reply_topic_and_key() -> None:
    async def run() -> None:
        kafka = KafkaBrokerChannel(KafkaConfig(), _FakeBus())
        kafka._producer = _FakeProducer()

        await kafka.send(
            OutboundMessage(
                channel="broker",
                chat_id="ignored",
                content="world",
                metadata={
                    "reply_topic": "custom.replies",
                    "reply_key": "client_a/session_9",
                    "correlation_id": "req-999",
                },
            )
        )

        assert kafka._producer.calls == [
            {
                "topic": "custom.replies",
                "value": b"world",
                "key": b"client_a/session_9",
                "headers": [("correlation_id", b"req-999")],
            }
        ]

    asyncio.run(run())


def test_kafka_send_derives_reply_key_from_chat_id() -> None:
    async def run() -> None:
        kafka = KafkaBrokerChannel(KafkaConfig(), _FakeBus())
        kafka._producer = _FakeProducer()

        await kafka.send(
            OutboundMessage(
                channel="broker",
                chat_id="client_a/session_11",
                content="reply",
            )
        )

        assert kafka._producer.calls == [
            {
                "topic": "broker.replies",
                "value": b"reply",
                "key": b"client_a/session_11",
                "headers": None,
            }
        ]

    asyncio.run(run())
