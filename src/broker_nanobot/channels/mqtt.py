"""MQTT-backed nanobot broker channel."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any
from collections.abc import AsyncIterator

import aiomqtt
from loguru import logger
from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel

from broker_nanobot.config.schema import MQTTConfig


class MQTTBrokerChannel(BaseChannel):
    name: str = "mqtt"

    def __init__(self, config: MQTTConfig, bus: MessageBus) -> None:
        super().__init__(config, bus)
        self._cfg = config
        self._client_cm: aiomqtt.Client | None = None
        self._client: aiomqtt.Client | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._running = True
        client_id = f"broker-nanobot-{id(self)}"
        self._client_cm = aiomqtt.Client(
            hostname=self._cfg.host,
            port=self._cfg.port,
            username=self._cfg.username or None,
            password=self._cfg.password or None,
            identifier=client_id,
        )
        self._client = await self._client_cm.__aenter__()
        await self._client.subscribe(self._cfg.request_topic, qos=self._cfg.qos)
        self._task = asyncio.create_task(self._consume())
        logger.info("Broker channel started with transport=mqtt")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._client_cm:
            await self._client_cm.__aexit__(None, None, None)
        logger.info("Broker channel stopped")

    async def send(self, msg: OutboundMessage) -> None:
        if self._client is None:
            logger.warning("Broker transport not connected; reply dropped")
            return
        reply_topic = self._resolve_reply_topic(msg)
        await self._client.publish(reply_topic, msg.content, qos=self._cfg.qos)

    async def _iter_messages(self) -> AsyncIterator[Any]:
        if self._client is None:
            return
        async for message in self._client.messages:
            yield message

    async def _consume(self) -> None:
        async for message in self._iter_messages():
            sender_id, chat_id, reply_topic = self._parse_request_route(message)
            try:
                content = bytes(message.payload).decode()
            except UnicodeDecodeError:
                logger.warning("MQTT request with invalid utf-8 payload ignored")
                continue
            await self._process_request(
                content,
                sender_id=sender_id,
                chat_id=chat_id,
                reply_topic=reply_topic,
            )

    def _parse_request_route(self, message: Any) -> tuple[str, str, str]:
        topic = str(getattr(message, "topic", ""))
        parts = [part for part in topic.split("/") if part]
        if len(parts) >= 2:
            producer_id = parts[-2]
            session_id = parts[-1]
            chat_id = f"{producer_id}/{session_id}"
            reply_topic = f"{self._cfg.reply_topic}/{producer_id}/{session_id}"
            return producer_id, chat_id, reply_topic

        fallback_chat_id = str(uuid.uuid4())
        return "broker", fallback_chat_id, self._cfg.reply_topic

    def _resolve_reply_topic(self, msg: OutboundMessage) -> str:
        reply_topic = msg.metadata.get("reply_topic") if msg.metadata else None
        if isinstance(reply_topic, str) and reply_topic:
            return reply_topic

        if "/" in msg.chat_id:
            producer_id, session_id = msg.chat_id.split("/", 1)
            if producer_id and session_id:
                return f"{self._cfg.reply_topic}/{producer_id}/{session_id}"

        return self._cfg.reply_topic

    async def _process_request(
        self,
        content: str,
        sender_id: str | None = None,
        chat_id: str | None = None,
        reply_topic: str | None = None,
    ) -> None:
        resolved_chat_id = chat_id or str(uuid.uuid4())
        metadata = {"reply_topic": reply_topic} if reply_topic else None

        await self._handle_message(
            sender_id=sender_id or "broker",
            chat_id=resolved_chat_id,
            content=content,
            metadata=metadata,
        )
