import json
import os
import traceback

import discord
import asyncio
from threading import Lock
from pathlib import Path
from typing import Any, Literal
import sqlite3
from collections.abc import MutableMapping


class JsonWorker:
    def __init__(self, path: str):
        json_path = Path(path)
        super().__setattr__("json_path", json_path)
        super().__setattr__("_data", self._open_data())

    def _open_data(self) -> dict[str, Any]:
        os.makedirs(os.path.dirname(self.json_path) or ".", exist_ok=True)

        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            with open(self.json_path, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=4)
            return {}

    def _commit_data(self) -> None:
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=4)


class DataWorker(JsonWorker):
    def __init__(self, path: str, setup: dict | None = None):
        super().__init__(path)
        if setup:
            for key, value in setup.items():
                if key not in self._data.keys():
                    setattr(self, key, value)

    def __getattr__(self, name: str) -> Any:
        return self._data.get(name)

    def __setattr__(self, name: str, value: Any):
        if name in {"json_path", "_data"}:
            super().__setattr__(name, value)
            return

        self._data[name] = value
        self._commit_data()


class NewDataWorker(JsonWorker, MutableMapping):
    def __init__(self, path: str, setup: dict | None = None):
        super().__init__(path)

        self._commit_event = asyncio.Event()
        self._commit_task = asyncio.create_task(
            self._commit_worker()
        )

        if setup:
            self._data.update({**setup, **self._data})
            self._commit()

    async def _commit_worker(self):
        try:
            while True:
                await self._commit_event.wait()
                self._commit_event.clear()

                await asyncio.to_thread(self._commit_data)

        except asyncio.CancelledError:
            raise

    def _commit(self):
        self._commit_event.set()

    async def close(self):
        self._commit_task.cancel()

        try:
            await self._commit_task
        except asyncio.CancelledError:
            pass

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value
        self._commit()

    def __delitem__(self, key):
        del self._data[key]
        self._commit()

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)


class BridgeStorage:
    """
    Storage manager for the bridge between Discord and Telegram.
    Handles JSON queues and linked message mappings.
    """

    def __init__(self,
                 link_path="modules/Bridge/LinkMess.json",
                 req_path="modules/Bridge/queues/Requests.json",
                 dtt_path="modules/Bridge/queues/DisToTel.json",
                 ttd_path="modules/Bridge/queues/TelToDis.json"):

        self.req_path = req_path
        self.link_path = link_path
        self.dtt_path = dtt_path
        self.ttd_path = ttd_path

        # Locks to ensure thread-safe access to each JSON file
        self._req_lock = Lock()
        self._link_lock = Lock()
        self._dtt_lock = Lock()
        self._ttd_lock = Lock()

    @staticmethod
    def _load_json(path: str) -> list:
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
            return []

        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, Exception):
            return []

    @staticmethod
    def _save_json(path: str, data: list):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def load_req(self) -> list:
        with self._req_lock:
            return self._load_json(self.req_path)

    def save_req(self, data: list):
        with self._req_lock:
            self._save_json(self.req_path, data)

    def add_req(self, element):
        data = self.load_req()
        data.append(element)
        self.save_req(data)

    def pop_req(self, mode):
        data = self.load_req()
        found = None

        for item in data:
            if item.get("way") == mode:
                found = item
                data.remove(item)
                break

        self.save_req(data)
        return found

    def load_link(self) -> list:
        with self._link_lock:
            return self._load_json(self.link_path)

    def save_link(self, data: list):
        with self._link_lock:
            self._save_json(self.link_path, data)

    def add_link(self, element):
        data = self.load_link()
        if len(data) >= 5000:
            data.pop(0)
        data.append(element)
        self.save_link(data)

    def clear_link(self):
        self.save_link([])

    def load_dtt(self) -> list:
        with self._dtt_lock:
            return self._load_json(self.dtt_path)

    def save_dtt(self, data: list):
        with self._dtt_lock:
            self._save_json(self.dtt_path, data)

    def add_dtt(self, element):
        data = self.load_dtt()
        data.append(element)
        self.save_dtt(data)

    def pop_dtt(self):
        data = self.load_dtt()
        if not data:
            return None
        element = data.pop(0)
        self.save_dtt(data)
        return element

    def load_ttd(self) -> list:
        with self._ttd_lock:
            return self._load_json(self.ttd_path)

    def save_ttd(self, data: list):
        with self._ttd_lock:
            self._save_json(self.ttd_path, data)

    def add_ttd(self, element):
        data = self.load_ttd()
        data.append(element)
        self.save_ttd(data)

    def pop_ttd(self):
        data = self.load_ttd()
        if not data:
            return None
        element = data.pop(0)
        self.save_ttd(data)
        return element


storage = BridgeStorage()


class DataTypes:
    class OzernikDataType:
        pass

    class Ozernik(OzernikDataType):
        def __init__(self, user_row: sqlite3.Row):
            self.id = user_row['id']
            self.alias = user_row['alias']
            self.discord_id = user_row['discord_id']
            self.discord_twink_id = user_row['discord_twink_id']
            self.telegram_id = user_row['telegram_id']
            self.telegram_bot_id = user_row['telegram_bot_id']

    class Karma(OzernikDataType):
        def __init__(self, row: sqlite3.Row):
            self.user_id = row['user_id']
            self.karma = row['karma']
            self.status = row['status']
            self.gift_karma = row['gift_karma']
            self.weekly_karma = row['weekly_karma']

    class KarmicBind(OzernikDataType):
        def __init__(self, row: sqlite3.Row):
            self.ids = (row['user1_id'], row['user2_id'])
            self.bind_karma = row['bind_karma']
            self.gift_bind_karma = row['gift_bind_karma']
            self.weekly_bind_karma = row['weekly_bind_karma']

    class ChatLink(OzernikDataType):
        def __init__(self, row: sqlite3.Row):
            self.id = row['id']
            self.discord_channel_id = row['discord_channel_id']
            self.telegram_chat_id = row['telegram_chat_id']
            self.telegram_topic_id = row['telegram_topic_id']

    class MessageLink(OzernikDataType):
        def __init__(self, row: sqlite3.Row, chat_link: "DataTypes.ChatLink"):
            self.id = row['id']
            self.user_id = row['user_id']
            self.chat_link = chat_link
            self.discord_message_id = row['discord_message_id']
            self.telegram_message_id = row['telegram_message_id']
            self.text = row['text']
            self.repeater_id = row['repeater_id']


class OzernikNotfound(Exception):
    """Озёрник не найден в Discord"""
    pass


class Database:
    def __init__(self) -> None:
        base_dir = Path(__file__).resolve().parent
        db_path = base_dir / 'database.db'

        self._con = sqlite3.connect(db_path)
        self._con.row_factory = sqlite3.Row
        self._con.execute("PRAGMA foreign_keys = ON")

        self._init_db()

    def _init_db(self) -> None:
        # Миграция ДО создания таблиц — разрешает несколько Telegram-чатов
        # на один Discord-канал (связка TG + TG + DC).
        self._migrate_chat_links()

        with self.transaction():
            self.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alias TEXT,
                    discord_id INTEGER NOT NULL UNIQUE,
                    discord_twink_id INTEGER UNIQUE,
                    telegram_id INTEGER UNIQUE,
                    telegram_bot_id INTEGER UNIQUE
                );
            """)

            self.execute("""
                CREATE TABLE IF NOT EXISTS karma (
                    user_id INTEGER PRIMARY KEY,
                    status TEXT DEFAULT NULL,
                    karma INTEGER NOT NULL DEFAULT 0,
                    gift_karma INTEGER NOT NULL DEFAULT 0,
                    weekly_karma INTEGER NOT NULL DEFAULT 0,

                    CHECK (
                        status IS NULL
                        OR status IN ('asur', 'deva')
                    ),

                    FOREIGN KEY (user_id)
                        REFERENCES users(id)
                        ON DELETE CASCADE
                );
            """)

            self.execute("""
                CREATE TABLE IF NOT EXISTS karma_logs (
                    user_id INTEGER NOT NULL,
                    added_at INTEGER NOT NULL,
                    added_karma INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT 'n/a',

                    CHECK (reason IN ('message', 'gift', 'n/a')),

                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """)

            self.execute("""
                CREATE INDEX IF NOT EXISTS idx_karma_logs_user_time
                ON karma_logs(user_id, added_at);
            """)

            self.execute("""
                CREATE TABLE IF NOT EXISTS karmic_bind (
                    user1_id INTEGER NOT NULL,
                    user2_id INTEGER NOT NULL,
                    bind_karma INTEGER NOT NULL DEFAULT 0,
                    gift_bind_karma INTEGER NOT NULL DEFAULT 0,
                    weekly_bind_karma INTEGER NOT NULL DEFAULT 0,

                    PRIMARY KEY (user1_id, user2_id),
                    CHECK (user1_id < user2_id),

                    FOREIGN KEY (user1_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (user2_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """)

            self.execute("""
                CREATE TABLE IF NOT EXISTS karmic_bind_logs (
                    user1_id INTEGER NOT NULL,
                    user2_id INTEGER NOT NULL,
                    added_at INTEGER NOT NULL,
                    added_karma INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT 'n/a',

                    CHECK (reason IN ('voice', 'reply', 'gift', 'n/a')),
                    CHECK (user1_id < user2_id),

                    FOREIGN KEY (user1_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (user2_id) REFERENCES users(id) ON DELETE CASCADE
                );
            """)

            self.execute("""
                CREATE INDEX IF NOT EXISTS idx_karmic_bind_logs_pair_time
                ON karmic_bind_logs(user1_id, user2_id, added_at);
            """)

            # chat_links — без UNIQUE(discord_channel_id), чтобы разрешить N Telegram-чатов на 1 Discord-канал.
            self.execute("""
                CREATE TABLE IF NOT EXISTS chat_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_channel_id INTEGER NOT NULL,
                    telegram_chat_id INTEGER NOT NULL,
                    telegram_topic_id INTEGER NOT NULL,
                    UNIQUE (telegram_chat_id, telegram_topic_id)
                )
            """)

            self.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    link_chat_id INTEGER NOT NULL,
                    discord_message_id INTEGER NOT NULL,
                    telegram_message_id INTEGER NOT NULL,
                    text TEXT,
                    repeater_id int,

                    FOREIGN KEY (user_id) REFERENCES users(id),
                    FOREIGN KEY (link_chat_id) REFERENCES chat_links(id)
                );
            """)

    def _migrate_chat_links(self) -> None:
        """
        Если старая схема chat_links содержит UNIQUE на discord_channel_id —
        пересоздаёт таблицу без этого ограничения, сохраняя данные.

        Старая схема (1 TG на 1 DC):
            discord_channel_id INTEGER NOT NULL UNIQUE
        Новая схема (N TG на 1 DC):
            discord_channel_id INTEGER NOT NULL,
            UNIQUE (telegram_chat_id, telegram_topic_id)
        """
        try:
            row = self.fetchone(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='chat_links'"
            )
        except sqlite3.Error:
            return

        if not row:
            return

        sql = row['sql'] or ''
        if 'discord_channel_id INTEGER NOT NULL UNIQUE' not in sql:
            return

        print("[DB] Migrating chat_links: allowing multiple Telegram chats per Discord channel...")

        old_foreign_keys = self._con.execute("PRAGMA foreign_keys").fetchone()[0]
        self._con.execute("PRAGMA foreign_keys = OFF")
        try:
            self._con.execute("BEGIN")
            self._con.execute("ALTER TABLE chat_links RENAME TO chat_links_old")
            self._con.execute("""
                CREATE TABLE chat_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    discord_channel_id INTEGER NOT NULL,
                    telegram_chat_id INTEGER NOT NULL,
                    telegram_topic_id INTEGER NOT NULL,
                    UNIQUE (telegram_chat_id, telegram_topic_id)
                )
            """)
            self._con.execute("""
                INSERT INTO chat_links (id, discord_channel_id, telegram_chat_id, telegram_topic_id)
                SELECT id, discord_channel_id, telegram_chat_id, telegram_topic_id
                FROM chat_links_old
            """)
            self._con.execute("DROP TABLE chat_links_old")
            self._con.commit()
            print("[DB] chat_links migration: OK")
        except sqlite3.Error as e:
            self._con.rollback()
            print(f"[DB] chat_links migration FAILED: {e}")
        finally:
            self._con.execute(f"PRAGMA foreign_keys = {old_foreign_keys}")

    def close_(self) -> None:
        self._con.close()

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        return self._con.execute(query, params)

    def fetchone(self, query: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        return self.execute(query, params).fetchone()

    def fetchall(self, query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        return self.execute(query, params).fetchall()

    def transaction(self):
        return self._con

    def add_user(
            self,
            discord_id: int,
            alias: str | None = None,
            telegram_id: int | None = None,
            discord_twink_id: int | None = None,
            telegram_bot_id: int | None = None,
    ) -> int:
        if discord_twink_id == discord_id:
            raise sqlite3.IntegrityError("Основной Discord ID и Discord ID твинка не могут совпадать.")

        with self.transaction():
            existing_row = self.fetchone("""
                SELECT id
                FROM users
                WHERE discord_id IN (?, ?)
                   OR discord_twink_id IN (?, ?)
                LIMIT 1
            """,
                (
                    discord_id,
                    discord_twink_id,
                    discord_id,
                    discord_twink_id,
                ),
            )

            if existing_row is not None:
                raise sqlite3.IntegrityError("Один из переданных Discord ID уже привязан к пользователю.")

            row = self.fetchone("""
                INSERT INTO users (
                    discord_id,
                    alias,
                    telegram_id,
                    discord_twink_id,
                    telegram_bot_id
                )
                VALUES (?, ?, ?, ?, ?)
                RETURNING id
            """,
                (
                    discord_id,
                    alias,
                    telegram_id,
                    discord_twink_id,
                    telegram_bot_id,
                ),
            )

        return row["id"]

    def get_user(self, user_id: int) -> DataTypes.Ozernik | None:
        row = self.fetchone("""
            SELECT * FROM users WHERE id = ?
        """, (user_id,))
        if row is None:
            return None
        return DataTypes.Ozernik(row)

    def get_user_by_discord_id(self, discord_id: int, ) -> DataTypes.Ozernik | None:
        row = self.fetchone("""
            SELECT *
            FROM users
            WHERE discord_id = ?
               OR discord_twink_id = ?
        """, (discord_id, discord_id))

        if row is None:
            return None

        return DataTypes.Ozernik(row)

    def get_user_by_telegram_id(self, telegram_id: int) -> DataTypes.Ozernik | None:
        row = self.fetchone("""
            SELECT *
            FROM users
            WHERE telegram_id = ?
        """, (telegram_id,))

        if row is None:
            return None

        return DataTypes.Ozernik(row)

    def update_user(
            self,
            user_id: int,
            discord_id: int | None = None,
            telegram_id: int | None = None,
            discord_twink_id: int | None = None,
            telegram_bot_id: int | None = None,
    ) -> DataTypes.Ozernik:
        updates = []
        values = []

        if discord_id is not None:
            updates.append("discord_id = ?")
            values.append(discord_id)

        if telegram_id is not None:
            updates.append("telegram_id = ?")
            values.append(telegram_id)

        if discord_twink_id is not None:
            updates.append("discord_twink_id = ?")
            values.append(discord_twink_id)

        if telegram_bot_id is not None:
            updates.append("telegram_bot_id = ?")
            values.append(telegram_bot_id)

        with self.transaction():
            current_row = self.fetchone("""
                SELECT *
                FROM users
                WHERE id = ?
            """, (user_id,))

            if current_row is None:
                raise ValueError(f"Пользователь с внутренним ID {user_id} не найден.")

            if not updates:
                return DataTypes.Ozernik(current_row)

            new_discord_id = (
                discord_id
                if discord_id is not None
                else current_row["discord_id"]
            )
            new_discord_twink_id = (
                discord_twink_id
                if discord_twink_id is not None
                else current_row["discord_twink_id"]
            )
            new_telegram_id = (
                telegram_id
                if telegram_id is not None
                else current_row["telegram_id"]
            )
            new_telegram_bot_id = (
                telegram_bot_id
                if telegram_bot_id is not None
                else current_row["telegram_bot_id"]
            )

            if new_discord_twink_id is not None and new_discord_id == new_discord_twink_id:
                raise sqlite3.IntegrityError("Основной Discord ID и Discord ID твинка не могут совпадать.")

            if new_telegram_id is not None and new_telegram_id == new_telegram_bot_id:
                raise sqlite3.IntegrityError("Telegram ID и Telegram Bot ID не могут совпадать.")

            existing_row = self.fetchone("""
                SELECT id
                FROM users
                WHERE id != ?
                  AND (
                        discord_id IN (?, ?)
                     OR discord_twink_id IN (?, ?)
                     OR telegram_id IN (?, ?)
                     OR telegram_bot_id IN (?, ?)
                  )
                LIMIT 1
            """, (
                user_id,
                new_discord_id, new_discord_twink_id,
                new_discord_id, new_discord_twink_id,
                new_telegram_id, new_telegram_bot_id,
                new_telegram_id, new_telegram_bot_id,
            ))

            if existing_row is not None:
                raise sqlite3.IntegrityError("Один из переданных ID уже привязан к другому пользователю.")

            values.append(user_id)

            row = self.fetchone(f"""
                UPDATE users
                SET {", ".join(updates)}
                WHERE id = ?
                RETURNING *
            """, tuple(values))

        return DataTypes.Ozernik(row)

    def delete_user(self, user_id: int) -> bool:
        with self.transaction():
            row = self.fetchone("""
                DELETE FROM users
                WHERE id = ?
                RETURNING id
            """, (user_id,))

        return row is not None


class KarmaDatabase(Database):
    def _ensure_karma_row(self, user_id: int) -> None:
        with self.transaction():
            self.execute(
                """
                INSERT INTO karma (user_id)
                VALUES (?)
                ON CONFLICT(user_id) DO NOTHING
                """,
                (user_id,),
            )

    def get_karma(self, user_id: int) -> DataTypes.Karma | None:
        self._ensure_karma_row(user_id)

        row = self.fetchone("""
            SELECT *
            FROM karma
            WHERE user_id = ?
        """, (user_id,))

        if row is None:
            return None
        return DataTypes.Karma(row)

    def add_karma(self, user_id: int, karma: int, weekly: bool = False) -> tuple[DataTypes.Karma, DataTypes.Karma]:
        self._ensure_karma_row(user_id)

        with self.transaction():
            old_row = self.fetchone("""
                SELECT *
                FROM karma
                WHERE user_id = ?
            """, (user_id,))

            new_row = self.fetchone("""
                UPDATE karma
                SET karma = karma + ?,
                    weekly_karma = weekly_karma + ?
                WHERE user_id = ?
                RETURNING *
            """, (karma, karma if weekly else 0, user_id))

        return DataTypes.Karma(old_row), DataTypes.Karma(new_row)

    def remove_karma(self, user_id: int, karma: int) -> DataTypes.Karma:
        self._ensure_karma_row(user_id)

        with self.transaction():
            row = self.fetchone("""
                UPDATE karma
                SET karma = karma - ?,
                    gift_karma = MIN(
                        gift_karma,
                        MAX(karma - ?, 0)
                    )
                WHERE user_id = ?
                RETURNING *
            """, (karma, karma, user_id))

        return DataTypes.Karma(row)

    def set_karma(self, user_id: int, karma: int) -> DataTypes.Karma:
        self._ensure_karma_row(user_id)

        with self.transaction():
            row = self.fetchone("""
                UPDATE karma
                SET karma = ?
                WHERE user_id = ?
                RETURNING *
            """, (karma, user_id))

        return DataTypes.Karma(row)

    def add_gift_karma(self, user_id: int, karma: int, weekly: bool = False) -> DataTypes.Karma:
        self._ensure_karma_row(user_id)

        with self.transaction():
            row = self.fetchone("""
                UPDATE karma
                SET karma = karma + ?,
                    gift_karma = gift_karma + ?,
                    weekly_karma = weekly_karma + ?
                WHERE user_id = ?
                RETURNING *
            """, (karma, karma, karma if weekly else 0, user_id))

        return DataTypes.Karma(row)

    def reset_weekly_karma(self) -> None:
        with self.transaction():
            self.execute("""
                UPDATE karma
                SET weekly_karma = 0
            """)

    def get_karma_rank(self, user_id: int) -> int | None:
        self._ensure_karma_row(user_id)

        row = self.fetchone("""
            SELECT rank
            FROM (
                SELECT karma.user_id,
                       ROW_NUMBER() OVER (
                           ORDER BY karma.karma DESC,
                                    karma.user_id ASC
                       ) AS rank
                FROM karma
            )
            WHERE user_id = ?
        """, (user_id,))

        if row is None:
            return None

        return row["rank"]

    def get_weekly_karma_rank(self, user_id: int) -> int | None:
        self._ensure_karma_row(user_id)

        row = self.fetchone("""
            SELECT rank
            FROM (
                SELECT karma.user_id,
                       ROW_NUMBER() OVER (
                           ORDER BY karma.weekly_karma DESC,
                                    karma.karma ASC,
                                    karma.user_id ASC
                       ) AS rank
                FROM karma
            )
            WHERE user_id = ?
        """, (user_id,))

        if row is None:
            return None

        return row["rank"]

    def get_top_karma(self) -> list[tuple[DataTypes.Ozernik, DataTypes.Karma]]:
        rows = self.fetchall("""
            SELECT users.*,
                   karma.user_id,
                   karma.status,
                   karma.karma,
                   karma.gift_karma,
                   karma.weekly_karma
            FROM users
            JOIN karma ON karma.user_id = users.id
            ORDER BY karma.karma DESC,
                     users.id DESC
        """)

        return [(DataTypes.Ozernik(row), DataTypes.Karma(row)) for row in rows]

    def get_top_weekly_karma(self) -> list[tuple[DataTypes.Ozernik, DataTypes.Karma]]:
        rows = self.fetchall("""
            SELECT users.*,
                   karma.user_id,
                   karma.status,
                   karma.karma,
                   karma.gift_karma,
                   karma.weekly_karma
            FROM users
            JOIN karma ON karma.user_id = users.id
            ORDER BY karma.weekly_karma DESC,
                     karma.karma ASC,
                     users.id ASC
        """)

        return [(DataTypes.Ozernik(row), DataTypes.Karma(row)) for row in rows]

    def _add_karma_log(self, user_id: int, added_karma: int, added_at_in_unix: int,
                       reason: Literal['message', 'gift', 'n/a'] = 'n/a') -> int:
        if reason not in ("message", "gift", "n/a"):
            raise ValueError('reason должен быть равен "message", "gift" или "n/a".')

        with self.transaction():
            row = self.fetchone("""
                INSERT INTO karma_logs (
                    user_id,
                    added_karma,
                    added_at,
                    reason
                )
                VALUES (?, ?, ?, ?)
                RETURNING id
            """, (user_id, added_karma, added_at_in_unix, reason))

        return row["id"]

    def get_karma_logs(self, start_time_in_unix: int, end_time_in_unix: int) -> list[dict[str, Any]]:
        if start_time_in_unix > end_time_in_unix:
            raise ValueError("start_time_in_unix не может быть больше end_time_in_unix.")

        rows = self.fetchall("""
            SELECT *
            FROM karma_logs
            WHERE added_at BETWEEN ? AND ?
            ORDER BY added_at ASC,
                     id ASC
        """, (start_time_in_unix, end_time_in_unix))

        return [dict(row) for row in rows]

    def get_user_karma_logs(self, user_id: int, start_time_in_unix: int,
                            end_time_in_unix: int) -> list[dict[str, Any]]:
        if start_time_in_unix > end_time_in_unix:
            raise ValueError("start_time_in_unix не может быть больше end_time_in_unix.")

        rows = self.fetchall("""
            SELECT *
            FROM karma_logs
            WHERE user_id = ?
              AND added_at BETWEEN ? AND ?
            ORDER BY added_at ASC,
                     id ASC
        """, (user_id, start_time_in_unix, end_time_in_unix))

        return [dict(row) for row in rows]

    @staticmethod
    def _normalize_pair(user1_id: int, user2_id: int) -> tuple[int, int]:
        if user1_id == user2_id:
            raise ValueError("Связь пользователя с им же самим запрещена.")

        return min(user1_id, user2_id), max(user1_id, user2_id)

    def _ensure_karmic_bind(self, user1_id: int, user2_id: int) -> tuple[int, int]:
        pair = self._normalize_pair(user1_id, user2_id)

        with self.transaction():
            self.execute("""
                INSERT INTO karmic_bind (
                    user1_id,
                    user2_id
                )
                VALUES (?, ?)
                ON CONFLICT(user1_id, user2_id) DO NOTHING
            """, pair)

    def get_karmic_bind(self, user1_id: int, user2_id: int) -> DataTypes.KarmicBind | None:
        pair = self._normalize_pair(user1_id, user2_id)

        row = self.fetchone("""
            SELECT *
            FROM karmic_bind
            WHERE user1_id = ?
              AND user2_id = ?
        """, pair)

        if row is None:
            return None
        return DataTypes.KarmicBind(row)

    def add_bind_karma(self, user1_id, user2_id, karma: int) -> DataTypes.KarmicBind:
        pair = self._normalize_pair(user1_id, user2_id)
        self._ensure_karmic_bind(*pair)

        with self.transaction():
            row = self.fetchone("""
                UPDATE karmic_bind
                SET bind_karma = bind_karma + ?
                WHERE user1_id = ?
                  AND user2_id = ?
                RETURNING *
            """, (karma, *pair))

        return DataTypes.KarmicBind(row)

    def add_gift_bind_karma(self, user1_id, user2_id, karma: int) -> DataTypes.KarmicBind:
        if karma <= 0:
            raise ValueError("karma должна быть больше нуля.")

        pair = self._normalize_pair(user1_id, user2_id)
        self._ensure_karmic_bind(*pair)

        with self.transaction():
            row = self.fetchone("""
                UPDATE karmic_bind
                SET bind_karma = bind_karma + ?,
                    gift_bind_karma = gift_bind_karma + ?
                WHERE user1_id = ?
                  AND user2_id = ?
                RETURNING *
            """, (karma, karma, *pair))

        return DataTypes.KarmicBind(row)

    def remove_bind_karma(self, user1_id, user2_id, karma: int) -> DataTypes.KarmicBind:
        if karma <= 0:
            raise ValueError("karma должна быть больше нуля.")

        pair = self._normalize_pair(user1_id, user2_id)
        self._ensure_karmic_bind(*pair)

        with self.transaction():
            row = self.fetchone("""
                UPDATE karmic_bind
                SET bind_karma = bind_karma - ?,
                    gift_bind_karma = MIN(
                        gift_bind_karma,
                        MAX(bind_karma - ?, 0)
                    )
                WHERE user1_id = ?
                  AND user2_id = ?
                RETURNING *
            """, (karma, karma, *pair))

        return DataTypes.KarmicBind(row)

    def set_bind_karma(self, user1_id, user2_id, karma: int) -> DataTypes.KarmicBind:
        if karma < 0:
            raise ValueError("karma не может быть меньше нуля.")

        pair = self._normalize_pair(user1_id, user2_id)
        self._ensure_karmic_bind(*pair)

        with self.transaction():
            row = self.fetchone("""
                UPDATE karmic_bind
                SET bind_karma = ?,
                    gift_bind_karma = MIN(
                        gift_bind_karma,
                        ?
                    )
                WHERE user1_id = ?
                  AND user2_id = ?
                RETURNING *
            """, (karma, karma, *pair))

        return DataTypes.KarmicBind(row)

    def get_user_top_karmic_binds(self, user_id) -> list[tuple[DataTypes.Ozernik, DataTypes.KarmicBind]]:
        rows = self.fetchall("""
            SELECT users.*,
                   karmic_bind.user1_id,
                   karmic_bind.user2_id,
                   karmic_bind.bind_karma,
                   karmic_bind.gift_bind_karma,
                   karmic_bind.weekly_bind_karma
            FROM karmic_bind
            JOIN users
              ON users.id = CASE
                  WHEN karmic_bind.user1_id = ?
                      THEN karmic_bind.user2_id
                  ELSE karmic_bind.user1_id
              END
            WHERE karmic_bind.user1_id = ?
               OR karmic_bind.user2_id = ?
            ORDER BY karmic_bind.bind_karma DESC,
                     users.id ASC
        """, (user_id, user_id, user_id))

        return [(DataTypes.Ozernik(row), DataTypes.KarmicBind(row)) for row in rows]

    def get_top_karmic_binds(self) -> list[tuple[DataTypes.Ozernik, DataTypes.Ozernik, DataTypes.KarmicBind]]:
        rows = self.fetchall("""
            SELECT karmic_bind.*,

                   user1.id AS user1_user_id,
                   user1.alias AS user1_alias,
                   user1.discord_id AS user1_discord_id,
                   user1.discord_twink_id AS user1_discord_twink_id,
                   user1.telegram_id AS user1_telegram_id,
                   user1.telegram_bot_id AS user1_telegram_bot_id,

                   user2.id AS user2_user_id,
                   user2.alias AS user2_alias,
                   user2.discord_id AS user2_discord_id,
                   user2.discord_twink_id AS user2_discord_twink_id,
                   user2.telegram_id AS user2_telegram_id,
                   user2.telegram_bot_id AS user2_telegram_bot_id

            FROM karmic_bind
            JOIN users AS user1
              ON user1.id = karmic_bind.user1_id
            JOIN users AS user2
              ON user2.id = karmic_bind.user2_id

            ORDER BY karmic_bind.bind_karma DESC,
                     karmic_bind.user1_id ASC,
                     karmic_bind.user2_id ASC
        """)

        result = []

        for row in rows:
            first_user = DataTypes.Ozernik({
                "id": row["user1_user_id"],
                "alias": row["user1_alias"],
                "discord_id": row["user1_discord_id"],
                "discord_twink_id": row["user1_discord_twink_id"],
                "telegram_id": row["user1_telegram_id"],
                "telegram_bot_id": row["user1_telegram_bot_id"],
            })

            second_user = DataTypes.Ozernik({
                "id": row["user2_user_id"],
                "alias": row["user2_alias"],
                "discord_id": row["user2_discord_id"],
                "discord_twink_id": row["user2_discord_twink_id"],
                "telegram_id": row["user2_telegram_id"],
                "telegram_bot_id": row["user2_telegram_bot_id"],
            })

            karmic_bind = DataTypes.KarmicBind(row)

            result.append((
                first_user,
                second_user,
                karmic_bind,
            ))

        return result

    def _add_karmic_bind_log(self, user1_id: int, user2_id: int, added_karma: int, added_at_in_unix: int,
                             reason: Literal['voice', 'reply', 'gift', 'n/a'] = 'n/a') -> int:
        if reason not in ('voice', 'reply', 'gift', 'n/a'):
            raise ValueError('reason должен быть равен "voice", "reply", "gift" или "n/a".')

        pair = self._normalize_pair(user1_id, user2_id)

        with self.transaction():
            row = self.fetchone("""
                INSERT INTO karmic_bind_logs (
                    user1_id,
                    user2_id,
                    added_karma,
                    added_at,
                    reason
                )
                VALUES (?, ?, ?, ?, ?)
                RETURNING id
            """, (*pair, added_karma, added_at_in_unix, reason))

        return row['id']

    def get_users_karmic_bind_logs(self, user1_id: int, user2_id: int, start_time_in_unix: int,
                                   end_time_in_unix: int) -> list[dict[str, Any]]:
        if start_time_in_unix > end_time_in_unix:
            raise ValueError("start_time_in_unix не может быть больше end_time_in_unix.")

        pair = self._normalize_pair(user1_id, user2_id)

        rows = self.fetchall("""
            SELECT *
            FROM karmic_bind_logs
            WHERE user1_id = ?
              AND user2_id = ?
              AND added_at >= ?
              AND added_at < ?
            ORDER BY added_at ASC,
                     id ASC
        """, (*pair, start_time_in_unix, end_time_in_unix))

        return [dict(row) for row in rows]

    def get_user_karmic_bind_logs(self, user_id: int, start_time_in_unix: int,
                                  end_time_in_unix: int) -> list[dict[str, Any]]:
        if start_time_in_unix > end_time_in_unix:
            raise ValueError("start_time_in_unix не может быть больше end_time_in_unix.")

        rows = self.fetchall("""
            SELECT *
            FROM karmic_bind_logs
            WHERE (user1_id = ? OR user2_id = ?)
              AND added_at >= ?
              AND added_at < ?
            ORDER BY added_at ASC,
                     id ASC
        """, (user_id, user_id, start_time_in_unix, end_time_in_unix))

        return [dict(row) for row in rows]

    def get_karmic_bind_logs(self, start_time_in_unix: int, end_time_in_unix: int) -> list[dict[str, Any]]:
        if start_time_in_unix > end_time_in_unix:
            raise ValueError("start_time_in_unix не может быть больше end_time_in_unix.")

        rows = self.fetchall("""
            SELECT *
            FROM karmic_bind_logs
            WHERE added_at >= ?
              AND added_at < ?
            ORDER BY added_at ASC,
                     id ASC
        """, (start_time_in_unix, end_time_in_unix))

        return [dict(row) for row in rows]


class BridgeDatabase(Database):
    def add_chat_link(self, discord_channel_id: int, telegram_chat_id: int,
                      telegram_topic_id: int = -1) -> DataTypes.ChatLink:
        with self.transaction():
            cur = self.execute("""
                INSERT INTO chat_links (
                    discord_channel_id,
                    telegram_chat_id,
                    telegram_topic_id
                )
                VALUES (?, ?, ?)
                RETURNING *
            """, (discord_channel_id, telegram_chat_id, telegram_topic_id))
            return DataTypes.ChatLink(cur.fetchone())

    def update_chat_link(self, link_id: int, discord_channel_id: int | None = None,
                         telegram_chat_id: int | None = None,
                         telegram_topic_id: int | None = None) -> DataTypes.ChatLink | None:
        fields = []
        values = []

        if discord_channel_id is not None:
            fields.append("discord_channel_id = ?")
            values.append(discord_channel_id)

        if telegram_chat_id is not None:
            fields.append("telegram_chat_id = ?")
            values.append(telegram_chat_id)

        if telegram_topic_id is not None:
            fields.append("telegram_topic_id = ?")
            values.append(telegram_topic_id)

        if not fields:
            raise ValueError("You must pass at least one field to change.")

        values.append(link_id)

        with self.transaction():
            cur = self.execute(f"""
                UPDATE chat_links
                SET {", ".join(fields)}
                WHERE id = ?
                RETURNING *
            """, values)

            row = cur.fetchone()
            return DataTypes.ChatLink(row) if row else None

    def get_chat_link(self, chat_id: int,
                      chat_type: Literal['link_id', 'discord', 'telegram'] = 'link_id',
                      thread_id: int = None) -> DataTypes.ChatLink | None:
        cur = None
        match chat_type:
            case 'link_id':
                cur = self.execute("""
                    SELECT * FROM chat_links WHERE id = ?
                """, (chat_id,))
            case 'discord':
                cur = self.execute("""
                    SELECT * FROM chat_links WHERE discord_channel_id = ?
                """, (chat_id,))
            case 'telegram':
                cur = self.execute("""
                    SELECT * FROM chat_links WHERE telegram_chat_id = ? AND telegram_topic_id = ?
                """, (chat_id, thread_id,))
            case _:
                raise TypeError(f'Invalid chat type: {chat_type}')

        row = cur.fetchone()  # noqa
        if row is None:
            return None

        return DataTypes.ChatLink(row)

    def get_chat_links_by_discord(self, discord_channel_id: int) -> list[DataTypes.ChatLink]:
        """Все Telegram-привязки данного Discord-канала (для мульти-моста)."""
        cur = self.execute(
            "SELECT * FROM chat_links WHERE discord_channel_id = ? ORDER BY id",
            (discord_channel_id,),
        )
        return [DataTypes.ChatLink(r) for r in cur.fetchall()]

    def get_linked_telegram_peers(self, link: DataTypes.ChatLink) -> list[DataTypes.ChatLink]:
        """Все ДРУГИЕ Telegram-чаты, привязанные к тому же Discord-каналу."""
        cur = self.execute(
            "SELECT * FROM chat_links WHERE discord_channel_id = ? AND id != ? ORDER BY id",
            (link.discord_channel_id, link.id),
        )
        return [DataTypes.ChatLink(r) for r in cur.fetchall()]

    def delete_chat_link(self, link_id: int) -> bool:
        with self.transaction():
            cur = self.execute("DELETE FROM chat_links WHERE id = ? RETURNING id", (link_id,))
            return cur.fetchone() is not None

    def del_chat_link(self, link_id: int) -> DataTypes.ChatLink | None:
        with self.transaction():
            cur = self.execute("""
                DELETE FROM chat_links
                WHERE id = ?
                RETURNING *
            """, (link_id,))

            row = cur.fetchone()
            return DataTypes.ChatLink(row) if row else None

    def add_message_link(
            self,
            link_chat_id: int,
            discord_message_id: int,
            telegram_message_id: int,
            user_id: int | None = None,
            text: str | None = None,
            repeater_id: str | None = None
    ) -> DataTypes.MessageLink:
        with self.transaction():
            if user_id:
                cur = self.execute("SELECT 1 FROM users WHERE id = ?", (user_id,))
                if cur.fetchone() is None:
                    raise ValueError(f"user_id={user_id} не существует")

            cur = self.execute("SELECT 1 FROM chat_links WHERE id = ?", (link_chat_id,))
            if cur.fetchone() is None:
                raise ValueError(f"link_chat_id={link_chat_id} не существует")

            cur = self.execute("""
                INSERT INTO messages (
                    user_id,
                    link_chat_id,
                    discord_message_id,
                    telegram_message_id,
                    text,
                    repeater_id
                )
                VALUES (?, ?, ?, ?, ?, ?)
                RETURNING *
            """, (
                user_id,
                link_chat_id,
                discord_message_id,
                telegram_message_id,
                text,
                repeater_id
            ))

            row = cur.fetchone()
            if row is None:
                raise RuntimeError("Не удалось добавить message link")

            chat_link = self.get_chat_link(row["link_chat_id"], "link_id")

            return DataTypes.MessageLink(row, chat_link)

    def get_message_link(self, message_id: int,
                         mess_type: Literal['discord', 'telegram'],
                         chat_link_id: int | None = None) -> DataTypes.MessageLink | None:
        cur = None
        match mess_type:
            case 'discord':
                cur = self.execute("""
                    SELECT * FROM messages WHERE discord_message_id = ?
                """, (message_id,))
            case 'telegram':
                cur = self.execute("""
                SELECT * FROM messages WHERE telegram_message_id = ? AND link_chat_id = ?
                """, (message_id, chat_link_id))

        row = cur.fetchone()  # noqa
        if row is None:
            return None

        chat_link = self.get_chat_link(row['link_chat_id'], 'link_id')

        return DataTypes.MessageLink(row, chat_link)

    def deep_get_discord_message_link(self, telegram_message_id: int,
                                      telegram_chat_id: int) -> DataTypes.MessageLink | None:
        cur = self.execute("""
                SELECT *
                FROM messages AS m
                JOIN chat_links AS c ON c.id = m.link_chat_id
                WHERE m.telegram_message_id = ?
                  AND c.telegram_chat_id = ?
                LIMIT 1
            """, (telegram_message_id, telegram_chat_id))

        row = cur.fetchone()
        if row is None:
            return None

        chat_link = self.get_chat_link(row['link_chat_id'], 'link_id')

        return DataTypes.MessageLink(row, chat_link)


class Assets:
    def __init__(self, path: Path):
        self.path = path

        self.path.mkdir(exist_ok=True)

        self.font_dir = path / 'fonts'
        self.font_dir.mkdir(exist_ok=True)

        self.background_dir = path / 'backgrounds'
        self.background_dir.mkdir(exist_ok=True)

        self.cubes_dir = path / 'cubes'
        self.cubes_dir.mkdir(exist_ok=True)

        self.sansara_dir = path / 'sansara'
        self.sansara_dir.mkdir(exist_ok=True)

    @property
    def fonts(self) -> dict[str, Path]:
        files = {}
        for file in self.font_dir.glob("*.ttf"):
            files[file.stem] = file
        return files

    @property
    def cubes(self) -> dict[str, Path]:
        files = {}
        for file in self.cubes_dir.glob("*.png"):
            files[file.stem] = file
        return files

    @property
    def sansara(self) -> dict[str, Path]:
        files = {}
        for file in self.sansara_dir.glob("*.png"):
            files[file.stem] = file
        return files

    @property
    def backgrounds(self) -> dict[str, Path]:
        files = {}
        for file in self.background_dir.glob("*.png"):
            files[file.stem] = file
        return files


assets = Assets(Path(__file__).parent / 'assets')


class Page:
    title = 'None'

    def __init__(self, navigator, author: discord.Member):
        self.navigator = navigator
        self.author = author

    def build(self):
        view = discord.ui.LayoutView(timeout=360)
        view.interaction_check = self._interaction_check

        async def on_timeout():
            await self._on_timeout(view)

        view.on_timeout = on_timeout

        container = discord.ui.Container()
        view.add_item(container)

        self.build_header(container)
        self.build_content(container)
        self.build_footer(container)

        return view

    async def _interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author.id:
            await interaction.response.send_message(
                "Это меню принадлежит другому пользователю.",
                ephemeral=True,
            )
            return False

        return True

    async def _on_timeout(self, view: discord.ui.LayoutView):
        if self.navigator.current_page != self:
            return

        for item in view.walk_children():
            if hasattr(item, "disabled"):
                item.disabled = True

        if self.navigator.message:
            await self.navigator.message.edit(view=view)

    def build_header(self, container):
        container.add_item(
            discord.ui.TextDisplay(f'# {self.title}')
        )

        container.add_item(
            discord.ui.Separator()
        )

    def build_content(self, container):
        raise NotImplementedError

    def build_footer(self, container):
        container.add_item(
            discord.ui.Separator()
        )

        row = discord.ui.ActionRow()

        if self.navigator.can_back:
            back_button = discord.ui.Button(
                label="Назад",
                style=discord.ButtonStyle.secondary
            )

            back_button.callback = self._back_callback

            row.add_item(back_button)

        for button in self.build_footer_buttons():
            row.add_item(button)

        if len(row.children):
            container.add_item(row)

    def build_footer_buttons(self):  # noqa
        return []

    async def _back_callback(self, interaction):  # noqa
        await self.navigator.back()
        await interaction.response.defer()

    def create_confirm_button(
            self,
            label: str,
            confirm_label: str,
            action,
    ) -> discord.ui.Button:

        button = discord.ui.Button(
            label=label,
            style=discord.ButtonStyle.primary,
        )

        async def first_callback(interaction: discord.Interaction):
            await interaction.response.defer()

            button.label = confirm_label
            button.style = discord.ButtonStyle.danger
            button.callback = second_callback

            await self.navigator.render()

        async def second_callback(interaction: discord.Interaction):
            await interaction.response.defer()

            button.label = label
            button.style = discord.ButtonStyle.primary
            button.callback = first_callback

            await action(interaction)

            await self.navigator.render()

        button.callback = first_callback

        return button

    async def on_open(self):
        pass

    async def on_close(self):
        pass


class Navigator:
    def __init__(self, page, **kwargs):
        self.history = []
        self.current_page = page(
            self,
            **kwargs
        )
        self.message = None

    @property
    def can_back(self):
        return bool(self.history)

    async def send(self, interaction):
        view = self.current_page.build()

        await interaction.response.send_message(view=view)

        self.message = await interaction.original_response()

    async def push(self, page, **kwargs):
        try:
            await self.current_page.on_close()

            self.history.append(self.current_page)

            self.current_page = page(
                self,
                **kwargs
            )

            await self.current_page.on_open()

            await self.render()
        except Exception as e:
            print(e)
            print(traceback.format_exc())

    async def back(self):
        if not self.history:
            return

        await self.current_page.on_close()

        self.current_page = self.history.pop()

        await self.current_page.on_open()

        await self.render()

    async def render(self):
        view = self.current_page.build()

        await self.message.edit(
            view=view
        )

    async def close(self):
        await self.current_page.on_close()
        await self.message.delete()
