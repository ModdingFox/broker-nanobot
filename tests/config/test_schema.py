from broker_nanobot.config.schema import (
    KafkaConfig,
    MQTTConfig,
    RabbitMQConfig,
)


def test_mqtt_config() -> None:
    cfg = MQTTConfig(enabled=True, host="myhost")
    assert cfg.enabled is True
    assert cfg.host == "myhost"


def test_rabbitmq_config() -> None:
    cfg = RabbitMQConfig(enabled=True, prefetch_count=5)
    assert cfg.enabled is True
    assert cfg.prefetch_count == 5


def test_kafka_config() -> None:
    cfg = KafkaConfig(enabled=True, group_id="mygroup")
    assert cfg.enabled is True
    assert cfg.group_id == "mygroup"


def test_rabbitmq_from_dict() -> None:
    raw = {
        "enabled": True,
        "url": "amqp://x:y@host/",
        "request_queue": "q1",
    }
    cfg = RabbitMQConfig.model_validate(raw)
    assert cfg.enabled is True
    assert cfg.url == "amqp://x:y@host/"
    assert cfg.request_queue == "q1"
