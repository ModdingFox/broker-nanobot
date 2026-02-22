"""RabbitMQ-backed nanobot broker channel."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import aio_pika
import aio_pika.abc
from loguru import logger
from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel

from broker_nanobot.config.schema import RabbitMQConfig


class RabbitMQBrokerChannel(BaseChannel):
    name: str = "rabbitmq"

    def __init__(self, config: RabbitMQConfig, bus: MessageBus) -> None:
        super().__init__(config, bus)
        self._cfg = config
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._reply_exchange: aio_pika.abc.AbstractExchange | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._running = True
        self._connection = await aio_pika.connect_robust(self._cfg.url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self._cfg.prefetch_count)

        request_exchange = await self._channel.declare_exchange(
            self._cfg.request_exchange,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        self._reply_exchange = await self._channel.declare_exchange(
            self._cfg.reply_exchange,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

        queue = await self._channel.declare_queue(self._cfg.request_queue, durable=True)
        await queue.bind(request_exchange, routing_key=self._cfg.request_routing_key)
        await self._channel.declare_queue(self._cfg.reply_queue, durable=True)
        self._task = asyncio.create_task(self._consume(queue))
        logger.info("Broker channel started with transport=rabbitmq")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        logger.info("Broker channel stopped")

    async def send(self, msg: OutboundMessage) -> None:
        if self._channel is None:
            logger.warning("Broker transport not connected; reply dropped")
            return

        reply_to = self._resolve_reply_to(msg)
        reply_routing_key = self._resolve_reply_routing_key(msg)
        correlation_id = self._resolve_correlation_id(msg)
        message = aio_pika.Message(
            body=msg.content.encode(),
            content_type="text/plain",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            correlation_id=correlation_id,
        )

        if reply_to:
            await self._channel.default_exchange.publish(message, routing_key=reply_to)
            return

        if self._reply_exchange and reply_routing_key:
            await self._reply_exchange.publish(message, routing_key=reply_routing_key)
            return

        await self._channel.default_exchange.publish(message, routing_key=self._cfg.reply_queue)

    async def _consume(self, queue: aio_pika.abc.AbstractQueue) -> None:
        async with queue.iterator() as iterator:
            async for message in iterator:
                async with message.process(requeue_on_error=True):
                    try:
                        content = message.body.decode()
                    except UnicodeDecodeError:
                        logger.warning("RabbitMQ request with invalid utf-8 payload ignored")
                        continue
                    sender_id, chat_id, reply_to, correlation_id, reply_routing_key = (
                        self._parse_request_route(message)
                    )
                    await self._process_request(
                        content,
                        sender_id=sender_id,
                        chat_id=chat_id,
                        reply_to=reply_to,
                        correlation_id=correlation_id,
                        reply_routing_key=reply_routing_key,
                    )

    def _parse_request_route(
        self, message: Any
    ) -> tuple[str, str, str | None, str | None, str | None]:
        reply_to = self._as_non_empty_str(getattr(message, "reply_to", None))
        correlation_id = self._as_non_empty_str(getattr(message, "correlation_id", None))
        routing_key = str(getattr(message, "routing_key", ""))
        parts = routing_key.split(".")
        if len(parts) == 2 and parts[0] and parts[1]:
            producer_id, session_id = parts
            chat_id = f"{producer_id}/{session_id}"
            return producer_id, chat_id, reply_to, correlation_id, routing_key

        fallback_chat_id = str(uuid.uuid4())
        return "broker", fallback_chat_id, reply_to, correlation_id, None

    @staticmethod
    def _as_non_empty_str(value: Any) -> str | None:
        return value if isinstance(value, str) and value else None

    def _resolve_reply_to(self, msg: OutboundMessage) -> str | None:
        return self._as_non_empty_str(msg.metadata.get("reply_to") if msg.metadata else None)

    def _resolve_correlation_id(self, msg: OutboundMessage) -> str | None:
        return self._as_non_empty_str(msg.metadata.get("correlation_id") if msg.metadata else None)

    def _resolve_reply_routing_key(self, msg: OutboundMessage) -> str | None:
        metadata_key = msg.metadata.get("reply_routing_key") if msg.metadata else None
        if isinstance(metadata_key, str) and metadata_key:
            return metadata_key

        if "/" in msg.chat_id:
            producer_id, session_id = msg.chat_id.split("/", 1)
            if producer_id and session_id:
                return f"{producer_id}.{session_id}"

        return None

    async def _process_request(
        self,
        content: str,
        sender_id: str | None = None,
        chat_id: str | None = None,
        reply_to: str | None = None,
        correlation_id: str | None = None,
        reply_routing_key: str | None = None,
    ) -> None:
        resolved_chat_id = chat_id or str(uuid.uuid4())
        metadata: dict[str, str] = {}
        if reply_to:
            metadata["reply_to"] = reply_to
        if correlation_id:
            metadata["correlation_id"] = correlation_id
        if reply_routing_key:
            metadata["reply_routing_key"] = reply_routing_key

        await self._handle_message(
            sender_id=sender_id or "broker",
            chat_id=resolved_chat_id,
            content=content,
            metadata=metadata or None,
        )
