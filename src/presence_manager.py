import logging
from typing import Callable, Dict, List, Optional

from galaxy.api.types import UserPresence
from presence import presence_from_rtm_entry, player_id_from_rtm_entry
from rtm_client import RtmClient

logger = logging.getLogger(__name__)


class PresenceManager:
    """
    Wraps RtmClient to provide Galaxy-compatible presence updates.

    Instead of polling, presence now arrives as real-time push events
    over EA's RTM TCP+TLS connection (rtm.tnt-ea.com:9000).
    """

    def __init__(
        self,
        http_client,
        update_friend_presence_cb: Callable[[str, UserPresence], None],
        interval_seconds: int = 30,  # kept for API compat, unused (RTM is push-based)
    ) -> None:
        self._http = http_client
        self._push = update_friend_presence_cb
        self._cache: Dict[str, UserPresence] = {}
        self._running = False
        self._disabled = False

        self._rtm = RtmClient(
            access_token_provider=lambda: getattr(http_client, "_access_token", None),
            on_presence_update=self._on_rtm_presence,
        )

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        if self._disabled or self._running:
            return
        if not getattr(self._http, "is_authenticated", lambda: False)():
            logger.info("Not authenticated; skipping RTM presence")
            self._disabled = True
            return
        self._running = True
        self._rtm.start()
        logger.info("RTM presence manager started")

    async def stop(self) -> None:
        self._running = False
        await self._rtm.stop()
        logger.info("RTM presence manager stopped")

    def set_friends(self, nucleus_ids: List[str]) -> None:
        """
        Provide the list of friend Nucleus IDs to subscribe to.
        Call this after get_friends() resolves.
        """
        self._rtm.set_friends(nucleus_ids)

    # ------------------------------------------------------------------ #
    #  Cache / query                                                       #
    # ------------------------------------------------------------------ #

    def get_friend_presence(self, user_id: str) -> Optional[UserPresence]:
        return self._cache.get(user_id)

    # ------------------------------------------------------------------ #
    #  RTM callback                                                        #
    # ------------------------------------------------------------------ #

    def _on_rtm_presence(self, player_id: str, raw: dict) -> None:
        presence = presence_from_rtm_entry(raw)
        if presence is None:
            return
        if self._cache.get(player_id) != presence:
            self._cache[player_id] = presence
            try:
                self._push(player_id, presence)
            except Exception:
                logger.exception("Failed to push presence update for %s", player_id)