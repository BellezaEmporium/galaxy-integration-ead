import asyncio
import logging
from typing import Callable, Dict, Optional

from galaxy.api.types import UserPresence
from presence import presence_from_friend_entry, friend_id_from_entry

logger = logging.getLogger(__name__)

class PresenceManager:
    def __init__(self, http_client, update_friend_presence_cb: Callable[[str, UserPresence], None], interval_seconds: int = 30) -> None:
        self._http = http_client
        self._push = update_friend_presence_cb
        self._interval = interval_seconds
        self._task: Optional[asyncio.Task] = None
        self._cache: Dict[str, UserPresence] = {}
        self._running = False
        self._disabled = False

    def start(self) -> None:
        if self._disabled or (self._task and not self._task.done()):
            return
        if not getattr(self._http, "supports_presence", lambda: False)():
            logger.info("EA social presence transport disabled; skipping presence loop")
            self._disabled = True
            return
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def get_friend_presence(self, user_id: str) -> Optional[UserPresence]:
        return self._cache.get(user_id)

    async def _run(self) -> None:
        while self._running:
            try:
                await self._refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Presence refresh failed")
                self._disabled = True
                self._running = False
                return
            await asyncio.sleep(self._interval)

    async def _refresh_once(self) -> None:
        entries = await self._http.get_friends_presence()
        for entry in entries:
            user_id = friend_id_from_entry(entry)
            presence = presence_from_friend_entry(entry)
            if not user_id or presence is None:
                continue
            if self._cache.get(user_id) != presence:
                self._cache[user_id] = presence
                self._push(user_id, presence)