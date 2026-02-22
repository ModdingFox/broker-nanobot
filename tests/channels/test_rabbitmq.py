import asyncio

from nanobot.bus.events import OutboundMessage

from broker_nanobot.channels.rabbitmq import RabbitMQBrokerChannel
from broker_nanobot.config.schema import RabbitMQConfig


class _FakeBus:
    def __init__(self) -> None:
        self.inbound = []

    async def publish_inbound(self, msg):
        self.inbound.append(msg)


class _FakeProcessCtx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeIncomingMessage:
    def __init__(
        self,
        body: bytes,
        routing_key: str = "",
        reply_to: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        self.body = body
        self.routing_key = routing_key
        self.reply_to = reply_to
        self.correlation_id = correlation_id

    def process(self, requeue_on_error: bool = True):
        _ = requeue_on_error
        return _FakeProcessCtx()


class _FakeIterator:
    def __init__(self, items):
        self._items = list(items)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


class _FakeQueue:
    def __init__(self, items):
        self._items = items

    def iterator(self):
        return _FakeIterator(self._items)


class _FakeExchange:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, str | None]] = []

    async def publish(self, message, routing_key: str) -> None:
        self.published.append((routing_key, message.body.decode(), message.correlation_id))


class _FakeChannel:
    def __init__(self) -> None:
        self.default_exchange = _FakeExchange()


def test_rabbitmq_consume_routes_only_valid_messages() -> None:
    async def run() -> None:
        bus = _FakeBus()
        ch = RabbitMQBrokerChannel(RabbitMQConfig(), bus)
        queue = _FakeQueue(
            [
                _FakeIncomingMessage(b"hello", routing_key="client_a.session_1"),
                _FakeIncomingMessage(b"broadcast", routing_key="client_a.session_1"),
            ]
        )
        await ch._consume(queue)
        assert [m.content for m in bus.inbound] == ["hello", "broadcast"]
        assert bus.inbound[0].sender_id == "client_a"
        assert bus.inbound[0].chat_id == "client_a/session_1"
        assert bus.inbound[0].metadata["reply_routing_key"] == "client_a.session_1"

    asyncio.run(run())


def test_rabbitmq_consume_returns_when_not_initialized() -> None:
    async def run() -> None:
        rabbit = RabbitMQBrokerChannel(RabbitMQConfig(), _FakeBus())
        await rabbit._consume(_FakeQueue([]))

    asyncio.run(run())


def test_rabbitmq_parse_request_route_fallback_for_invalid_key() -> None:
    bus = _FakeBus()
    ch = RabbitMQBrokerChannel(RabbitMQConfig(), bus)

    sender_id, chat_id, reply_to, correlation_id, reply_routing_key = ch._parse_request_route(
        _FakeIncomingMessage(b"hello", routing_key="invalid.key.shape")
    )

    assert sender_id == "broker"
    assert chat_id
    assert reply_to is None
    assert correlation_id is None
    assert reply_routing_key is None


def test_rabbitmq_send_uses_reply_routing_key_from_metadata() -> None:
    async def run() -> None:
        ch = RabbitMQBrokerChannel(RabbitMQConfig(), _FakeBus())
        ch._channel = _FakeChannel()
        ch._reply_exchange = _FakeExchange()

        msg = OutboundMessage(
            channel="broker",
            chat_id="ignored",
            content="world",
            metadata={"reply_routing_key": "client_a.session_1"},
        )
        await ch.send(msg)

        assert ch._reply_exchange.published == [("client_a.session_1", "world", None)]

    asyncio.run(run())


def test_rabbitmq_send_derives_reply_routing_key_from_chat_id() -> None:
    async def run() -> None:
        ch = RabbitMQBrokerChannel(RabbitMQConfig(), _FakeBus())
        ch._channel = _FakeChannel()
        ch._reply_exchange = _FakeExchange()

        msg = OutboundMessage(
            channel="broker",
            chat_id="client_a/session_2",
            content="reply",
        )
        await ch.send(msg)

        assert ch._reply_exchange.published == [("client_a.session_2", "reply", None)]

    asyncio.run(run())


def test_rabbitmq_consume_preserves_native_rpc_metadata() -> None:
    async def run() -> None:
        bus = _FakeBus()
        ch = RabbitMQBrokerChannel(RabbitMQConfig(), bus)
        queue = _FakeQueue(
            [
                _FakeIncomingMessage(
                    b"hello",
                    routing_key="client_a.session_3",
                    reply_to="amq.gen-reply-client-a",
                    correlation_id="req-123",
                )
            ]
        )
        await ch._consume(queue)

        assert len(bus.inbound) == 1
        assert bus.inbound[0].metadata["reply_to"] == "amq.gen-reply-client-a"
        assert bus.inbound[0].metadata["correlation_id"] == "req-123"
        assert bus.inbound[0].metadata["reply_routing_key"] == "client_a.session_3"

    asyncio.run(run())


def test_rabbitmq_send_prefers_reply_to_when_present() -> None:
    async def run() -> None:
        ch = RabbitMQBrokerChannel(RabbitMQConfig(), _FakeBus())
        ch._channel = _FakeChannel()
        ch._reply_exchange = _FakeExchange()

        msg = OutboundMessage(
            channel="broker",
            chat_id="client_a/session_4",
            content="rpc-reply",
            metadata={
                "reply_to": "amq.gen-reply-client-a",
                "correlation_id": "req-456",
                "reply_routing_key": "client_a.session_4",
            },
        )
        await ch.send(msg)

        assert ch._channel.default_exchange.published == [
            ("amq.gen-reply-client-a", "rpc-reply", "req-456")
        ]
        assert ch._reply_exchange.published == []

    asyncio.run(run())
