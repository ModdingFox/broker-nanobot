"""Kafka-backed nanobot broker channel."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger
from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel

from broker_nanobot.config.schema import KafkaConfig


class KafkaBrokerChannel(BaseChannel):
    name: str = "kafka"

    def __init__(self, config: KafkaConfig, bus: MessageBus) -> None:
        super().__init__(config, bus)
        self._cfg = config
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._running = True
        self._producer = AIOKafkaProducer(bootstrap_servers=self._cfg.bootstrap_servers)
        await self._producer.start()
        self._consumer = AIOKafkaConsumer(
            self._cfg.request_topic,
            bootstrap_servers=self._cfg.bootstrap_servers,
            group_id=self._cfg.group_id,
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._consume())
        logger.info("Broker channel started with transport=kafka")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
        if self._producer:
            await self._producer.stop()
        logger.info("Broker channel stopped")

    async def send(self, msg: OutboundMessage) -> None:
        if self._producer is None:
            logger.warning("Broker transport not connected; reply dropped")
            return

        reply_topic = self._resolve_reply_topic(msg)
        reply_key = self._resolve_reply_key(msg)
        correlation_id = self._resolve_correlation_id(msg)
        headers = [("correlation_id", correlation_id.encode())] if correlation_id else None

        await self._producer.send_and_wait(
            reply_topic,
            value=msg.content.encode(),
            key=reply_key.encode() if reply_key else None,
            headers=headers,
        )

    async def _consume(self) -> None:
        if self._consumer is None:
            return
        async for message in self._consumer:
            sender_id, chat_id, reply_topic, reply_key, correlation_id = self._parse_request_route(
                message
            )
            try:
                content = message.value.decode()
            except UnicodeDecodeError:
                logger.warning("Kafka request with invalid utf-8 payload ignored")
                continue
            await self._process_request(
                content,
                sender_id=sender_id,
                chat_id=chat_id,
                reply_topic=reply_topic,
                reply_key=reply_key,
                correlation_id=correlation_id,
            )

    def _parse_request_route(self, message: Any) -> tuple[str, str, str, str | None, str | None]:
        key = self._decode_key(getattr(message, "key", None))
        correlation_id = self._extract_correlation_id(getattr(message, "headers", None))
        if key and "/" in key:
            producer_id, session_id = key.split("/", 1)
            if producer_id and session_id:
                chat_id = f"{producer_id}/{session_id}"
                return producer_id, chat_id, self._cfg.reply_topic, key, correlation_id

        fallback_chat_id = str(uuid.uuid4())
        return "broker", fallback_chat_id, self._cfg.reply_topic, None, correlation_id

    @staticmethod
    def _decode_key(value: Any) -> str | None:
        if isinstance(value, bytes):
            try:
                decoded = value.decode()
            except UnicodeDecodeError:
                return None
            return decoded if decoded else None
        if isinstance(value, str) and value:
            return value
        return None

    @staticmethod
    def _extract_correlation_id(headers: Any) -> str | None:
        if not isinstance(headers, list):
            return None
        for key, value in headers:
            if key != "correlation_id" or not isinstance(value, bytes):
                continue
            try:
                decoded = value.decode()
            except UnicodeDecodeError:
                return None
            return decoded if decoded else None
        return None

    def _resolve_reply_topic(self, msg: OutboundMessage) -> str:
        reply_topic = msg.metadata.get("reply_topic") if msg.metadata else None
        if isinstance(reply_topic, str) and reply_topic:
            return reply_topic
        return self._cfg.reply_topic

    def _resolve_reply_key(self, msg: OutboundMessage) -> str | None:
        reply_key = msg.metadata.get("reply_key") if msg.metadata else None
        if isinstance(reply_key, str) and reply_key:
            return reply_key

        if "/" in msg.chat_id:
            producer_id, session_id = msg.chat_id.split("/", 1)
            if producer_id and session_id:
                return f"{producer_id}/{session_id}"

        return None

    def _resolve_correlation_id(self, msg: OutboundMessage) -> str | None:
        correlation_id = msg.metadata.get("correlation_id") if msg.metadata else None
        if isinstance(correlation_id, str) and correlation_id:
            return correlation_id
        return None

    async def _process_request(
        self,
        content: str,
        sender_id: str | None = None,
        chat_id: str | None = None,
        reply_topic: str | None = None,
        reply_key: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        resolved_chat_id = chat_id or str(uuid.uuid4())
        metadata: dict[str, str] = {}
        if reply_topic:
            metadata["reply_topic"] = reply_topic
        if reply_key:
            metadata["reply_key"] = reply_key
        if correlation_id:
            metadata["correlation_id"] = correlation_id

        await self._handle_message(
            sender_id=sender_id or "broker",
            chat_id=resolved_chat_id,
            content=content,
            metadata=metadata or None,
        )
