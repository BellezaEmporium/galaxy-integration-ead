import logging
from typing import Optional, Dict, Any
from galaxy.api.types import UserPresence, PresenceState

logger = logging.getLogger(__name__)

_STATE_MAP = {
    "ONLINE": PresenceState.Online,
    "PLAYING": PresenceState.Online,
    "INGAME": PresenceState.Online,
    "IN_GAME": PresenceState.Online,
    "AWAY": PresenceState.Away,
    "OFFLINE": PresenceState.Offline,
    "INVISIBLE": PresenceState.Offline,
    "UNKNOWN": PresenceState.Unknown,
}

def _norm_state(value: Optional[str]) -> PresenceState:
    if not value:
        return PresenceState.Unknown
    return _STATE_MAP.get(str(value).upper(), PresenceState.Unknown)

def _as_str(value) -> Optional[str]:
    if value in (None, "", b""):
        return None
    return str(value)

def presence_from_friend_entry(entry: Dict[str, Any]) -> Optional[UserPresence]:
    try:
        presence = entry.get("presence") or entry
        state = _norm_state(
            presence.get("presenceState")
            or presence.get("status")
            or presence.get("availability")
        )
        game_id = _as_str(
            presence.get("titleId")
            or presence.get("offerId")
            or presence.get("gameId")
        )
        game_title = (
            presence.get("gameName")
            or presence.get("title")
            or presence.get("productName")
        )
        if state != PresenceState.Online:
            game_id = None
            game_title = None
        return UserPresence(
            presence_state=state,
            game_id=game_id,
            game_title=game_title or None,
        )
    except Exception:
        logger.exception("Failed to parse friend presence entry: %r", entry)
        return None

def friend_id_from_entry(entry: Dict[str, Any]) -> Optional[str]:
    return _as_str(
        entry.get("personaId")
        or entry.get("accountId")
        or entry.get("nucleusId")
        or entry.get("userId")
    )