import sys
import types

from broker_nanobot import main as main_mod
from broker_nanobot.config.schema import ExtendedConfig


def test_patch_nanobot_config() -> None:
    import nanobot.config.loader as loader
    import nanobot.config.schema as schema

    main_mod._patch_nanobot_config()

    assert loader.Config is ExtendedConfig
    assert schema.Config is ExtendedConfig


def test_patch_channel_manager_injects_enabled_channels(monkeypatch) -> None:
    class FakeManager:
        def __init__(self):
            self.channels = {}
            self.bus = object()
            self.config = ExtendedConfig.model_validate(
                {
                    "channels": {
                        "mqtt": {"enabled": True},
                        "rabbitmq": {"enabled": True},
                        "kafka": {"enabled": False},
                    }
                }
            )
            self.init_called = False

        def _init_channels(self):
            self.init_called = True

    fake_module = types.ModuleType("nanobot.channels.manager")
    fake_module.ChannelManager = FakeManager
    monkeypatch.setitem(sys.modules, "nanobot.channels.manager", fake_module)

    main_mod._patch_channel_manager()
    mgr = FakeManager()
    mgr._init_channels()

    assert mgr.init_called is True
    assert "mqtt" in mgr.channels
    assert "rabbitmq" in mgr.channels
    assert "kafka" not in mgr.channels


def test_patch_channel_manager_no_enabled_channels(monkeypatch) -> None:
    class FakeManager:
        def __init__(self):
            self.channels = {"telegram": object()}
            self.bus = object()
            self.config = ExtendedConfig.model_validate({"channels": {}})

        def _init_channels(self):
            return None

    fake_module = types.ModuleType("nanobot.channels.manager")
    fake_module.ChannelManager = FakeManager
    monkeypatch.setitem(sys.modules, "nanobot.channels.manager", fake_module)

    main_mod._patch_channel_manager()
    mgr = FakeManager()
    mgr._init_channels()

    assert list(mgr.channels.keys()) == ["telegram"]


def test_main_invokes_cli_app(monkeypatch):
    called = []

    def fake_patch_config():
        called.append("config")

    def fake_patch_manager():
        called.append("manager")

    def fake_app():
        called.append("app")

    fake_commands = types.ModuleType("nanobot.cli.commands")
    fake_commands.app = fake_app
    monkeypatch.setitem(sys.modules, "nanobot.cli.commands", fake_commands)
    monkeypatch.setattr(main_mod, "_patch_nanobot_config", fake_patch_config)
    monkeypatch.setattr(main_mod, "_patch_channel_manager", fake_patch_manager)

    main_mod.main()

    assert called == ["config", "manager", "app"]
