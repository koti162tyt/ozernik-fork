import os
from pathlib import Path
import json
from typing import Any
from copy import deepcopy

from utilities import DataWorker

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"


# ---------- ТОКЕНЫ (только из окружения) ----------

DISCORD_TOKEN_MAIN = os.getenv("DISCORD_TOKEN_MAIN")
TELEGRAM_TOKEN_MAIN = os.getenv("TELEGRAM_TOKEN_MAIN")
TELEGRAM_TOKEN_REPEATER_1 = os.getenv("TELEGRAM_TOKEN_REPEATER_1")
TELEGRAM_TOKEN_REPEATER_2 = os.getenv("TELEGRAM_TOKEN_REPEATER_2")

_missing = [
    name for name, value in {
        "DISCORD_TOKEN_MAIN": DISCORD_TOKEN_MAIN,
        "TELEGRAM_TOKEN_MAIN": TELEGRAM_TOKEN_MAIN,
        "TELEGRAM_TOKEN_REPEATER_1": TELEGRAM_TOKEN_REPEATER_1,
        "TELEGRAM_TOKEN_REPEATER_2": TELEGRAM_TOKEN_REPEATER_2,
    }.items()
    if not value
]

if _missing:
    raise RuntimeError(
        "Не заданы переменные окружения: "
        + ", ".join(_missing)
        + "\nЗадай их в панели Render (Environment → Environment Variables) "
          "или локально через 'export' / '$env:' / 'set'."
    )


# ---------- ЛОКАЛЬНЫЙ КОНФИГ ----------

CONFIG_TEMPLATE = {
    "GUILD_ID": 1324396791965417513,
    "OWNERS_IDS": [
        512079329619083291,
        436586208450052099
    ],
    "ADMIN_ROLES_IDS": [],
    "MODULES": {}
}


class ConfigWorker(DataWorker):
    def __init__(self, path: str, setup: dict | None = None):
        super().__init__(path, setup)

    def __setattr__(self, name: str, value: Any):
        if name in {"json_path", "_data"}:
            super().__setattr__(name, value)
            return

        self._data[name] = value
        self._commit_data()

    def module_add(self, module_path: str):
        if module_path in self.MODULES:
            return

        self.MODULES[module_path] = True
        self._data["MODULES"][module_path] = True
        self._commit_data()

    def module_on(self, module_path: str):
        self.MODULES[module_path] = True
        self._data["MODULES"][module_path] = True
        self._commit_data()

    def module_off(self, module_path: str):
        self.MODULES[module_path] = False
        self._data["MODULES"][module_path] = False
        self._commit_data()


config = ConfigWorker(CONFIG_PATH, CONFIG_TEMPLATE)
