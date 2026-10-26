# main.py

import asyncio
import sys
import os
import time
import traceback
from datetime import timedelta, timezone

import aiohttp
from aiohttp import web

import discord
from colorama import Fore, Style, init
from discord.ext import commands

from config import config, DISCORD_TOKEN_MAIN

from utilities import Database, DataTypes

init()


GUILD_ID = config.GUILD_ID
MODULES_FOLDER = "modules"

MOSCOW_TZ = timezone(timedelta(hours=3))

# ---------- Render keep-alive ----------
PING_INTERVAL_SECONDS = 10 * 60       # 10 минут (< 15 мин засыпания)
PING_TIMEOUT_SECONDS = 15


class OzernikiBot(commands.Bot):

    # ---------- ИНИЦИАЛИЗАЦИЯ -----------

    def __init__(self) -> None:
        self.ready = False
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True

        super().__init__(
            command_prefix=".",
            intents=intents,
        )

        self.moscow_tz = MOSCOW_TZ
        self.modules_folder = MODULES_FOLDER

        self.db = Database()

        # Render / keep-alive state
        self._start_ts: float = time.time()
        self._web_runner: web.AppRunner | None = None
        self._ping_task: asyncio.Task | None = None

    async def setup_hook(self) -> None:
        self.discover_modules()
        await self.load_configured_extensions()

        # --- keep-alive ---
        await self._start_web_server()
        self._ping_task = asyncio.create_task(self._self_ping(), name="self-ping")
        # --- /keep-alive ---

        guild = discord.Object(id=GUILD_ID)

        real_guild = self.get_guild(GUILD_ID)
        if real_guild is None:
            print(
                Fore.YELLOW
                + f"[WARN] Бот не видит сервер {GUILD_ID}. "
                  f"Пригласи его по URL со scope 'bot applications.commands'."
                + Style.RESET_ALL
            )
        else:
            print(
                Fore.GREEN
                + f"[INFO] Сервер найден: {real_guild.name} ({real_guild.id})"
                + Style.RESET_ALL
            )

        self.tree.copy_global_to(guild=guild)

        try:
            synced = await self.tree.sync(guild=guild)
            print(
                Fore.GREEN
                + f"Slash-команды синхронизированы для сервера {GUILD_ID}: {len(synced)} шт."
                + Style.RESET_ALL
            )
        except discord.Forbidden as e:
            print(
                Fore.RED
                + f"[ERROR] Не удалось синхронизировать команды для {GUILD_ID}: {e}\n"
                  f"        Бот продолжит работу, но слэш-команды не зарегистрированы.\n"
                  f"        Проверь:\n"
                  f"          (1) бот в этом сервере;\n"
                  f"          (2) приглашён со scope 'bot applications.commands'.\n"
                  f"        Готовая ссылка-приглашение:\n"
                  f"          https://discord.com/api/oauth2/authorize"
                  f"?client_id={self.application_id}&permissions=8&scope=bot%20applications.commands"
                + Style.RESET_ALL
            )
        except Exception as e:
            print(
                Fore.RED
                + f"[ERROR] tree.sync failed: {e}"
                + Style.RESET_ALL
            )

    async def close(self) -> None:
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except (asyncio.CancelledError, Exception):
                pass

        if self._web_runner is not None:
            await self._web_runner.cleanup()
            self._web_runner = None

        await super().close()

    # ---------- RENDER KEEP-ALIVE ----------

    async def _start_web_server(self) -> None:
        """Поднимает HTTP-сервер на PORT (Render выдаёт его автоматически)."""
        app = web.Application()
        app.router.add_get("/", self._health)
        app.router.add_get("/health", self._health)

        self._web_runner = web.AppRunner(app)
        await self._web_runner.setup()

        port = int(os.getenv("PORT", "8080"))
        site = web.TCPSite(self._web_runner, "0.0.0.0", port)
        await site.start()

        public_url = os.getenv("RENDER_EXTERNAL_URL", "")
        print(
            Fore.CYAN
            + f"[web] HTTP-сервер запущен на 0.0.0.0:{port}"
            + (f" (public: {public_url})" if public_url else "")
            + Style.RESET_ALL
        )

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({
            "status": "ok",
            "bot_ready": self.ready,
            "user": str(self.user) if self.user else None,
            "uptime_seconds": int(time.time() - self._start_ts),
        })

    async def _self_ping(self) -> None:
        """Раз в 10 минут дёргает свой публичный URL, чтобы Render не усыпил контейнер."""
        url = os.getenv("RENDER_EXTERNAL_URL")
        if not url:
            print(
                Fore.YELLOW
                + "[self-ping] RENDER_EXTERNAL_URL не задан — самопинг выключен "
                  "(на Render переменная есть автоматически)."
                + Style.RESET_ALL
            )
            return

        url = url.rstrip("/") + "/health"
        timeout = aiohttp.ClientTimeout(total=PING_TIMEOUT_SECONDS)

        # небольшая задержка на старте, чтобы сервер успел подняться
        await asyncio.sleep(10)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            print(
                                Fore.CYAN
                                + f"[self-ping] {url} → {resp.status}"
                                + Style.RESET_ALL
                            )
                        else:
                            print(
                                Fore.YELLOW
                                + f"[self-ping] {url} → {resp.status}"
                                + Style.RESET_ALL
                            )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(
                        Fore.RED
                        + f"[self-ping] fail: {e!r}"
                        + Style.RESET_ALL
                    )

                await asyncio.sleep(PING_INTERVAL_SECONDS)

    # ---------- ОСТАЛЬНОЕ ----------

    def discover_modules(self) -> None:
        for folder in sorted(os.listdir(self.modules_folder)):
            folder_path = os.path.join(self.modules_folder, folder)

            if not os.path.isdir(folder_path):
                continue

            if folder.startswith("_"):
                continue

            for filename in sorted(os.listdir(folder_path)):
                if not filename.endswith(".py"):
                    continue

                if filename.startswith("_"):
                    continue

                module_path = (
                    f"{self.modules_folder}."
                    f"{folder}."
                    f"{filename[:-3]}"
                )

                config.module_add(module_path)

    async def load_configured_extensions(self) -> None:
        for index, (module_path, loaded) in enumerate(
            config.MODULES.items(),
            start=1,
        ):
            module_name = module_path.rsplit(".", maxsplit=1)[-1]

            if not loaded:
                print(
                    Fore.YELLOW
                    + f"{index}. [{module_name}] Cog is disabled."
                    + Style.RESET_ALL
                )
                continue

            try:
                await self.load_extension(module_path)

            except Exception:
                tb = traceback.format_exc()

                print(
                    Fore.RED
                    + f'{index}. [{module_name}] Error loading Cog:\n{tb}'
                    + Style.RESET_ALL
                )

            else:
                print(
                    Fore.GREEN
                    + f"{index}. [{module_name}] Cog loaded successfully!"
                    + Style.RESET_ALL
                )

    @staticmethod
    def print_error(source, error):
        tb = ''.join(
            traceback.format_exception(
                type(error),
                error,
                error.__traceback__
            )
        )

        print(f"Error in {source}:\n{tb}")

    async def on_error(self, event, *args, **kwargs) -> None:
        error = sys.exc_info()[1]
        self.print_error(event, error)

    def db_ensure_user(self, user: discord.User) -> DataTypes.Ozernik | None:
        ozernik = self.db.get_user_by_discord_id(user.id)
        if ozernik is None:
            ozernik_id = self.db.add_user(discord_id=user.id)
            ozernik = self.db.get_user(ozernik_id)
        return ozernik

    @property
    def unix_time(self) -> int:
        return time.time()

    @property
    def guild(self):
        return self.get_guild(GUILD_ID)


bot = OzernikiBot()


@bot.event
async def on_ready() -> None:
    print(f"Login: {bot.user}")
    print(f"Timezone setup: {bot.moscow_tz}")
    print("Ready!")
    bot.ready = True


async def main() -> None:
    async with bot:
        await bot.start(DISCORD_TOKEN_MAIN)


if __name__ == "__main__":
    asyncio.run(main())
