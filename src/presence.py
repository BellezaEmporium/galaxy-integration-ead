import logging
from typing import Optional, Dict, Any
from galaxy.api.types import UserPresence, PresenceState

logger = logging.getLogger(__name__)

# Maps RTM BasicPresenceType int values (from common.proto) to Galaxy states
# NONE=0, UNKNOWN=1, OFFLINE=2, ONLINE=3, DND=4, AWAY=5, INVISIBLE=6, CUSTOM=7
_BASIC_STATE_MAP: Dict[int, PresenceState] = {
    0: PresenceState.Unknown,    # NONE_PRESENCE
    1: PresenceState.Unknown,    # UNKNOWN_PRESENCE
    2: PresenceState.Offline,    # OFFLINE
    3: PresenceState.Online,     # ONLINE
    4: PresenceState.Online,     # DND (do-not-disturb — still online)
    5: PresenceState.Away,       # AWAY
    6: PresenceState.Offline,    # INVISIBLE
    7: PresenceState.Online,     # CUSTOM
}


def presence_from_rtm_entry(entry: Dict[str, Any]) -> Optional[UserPresence]:
    """
    Build a Galaxy UserPresence from a decoded RTM PresenceV1 dict.

    Expected keys (produced by RtmClient._handle_presence):
      - basicPresenceType  (int)
      - gameId             (Optional[str])  from customRichPresenceData.gameProductId
      - gameTitle          (Optional[str])  from richPresence.game
    """
    try:
        basic = entry.get("basicPresenceType", 0)
        state = _BASIC_STATE_MAP.get(basic, PresenceState.Unknown)

        game_id    = entry.get("gameId") or None
        game_title = entry.get("gameTitle") or None

        # Only expose game info when the user is visibly online
        if state != PresenceState.Online:
            game_id = None
            game_title = None

        return UserPresence(
            presence_state=state,
            game_id=game_id,
            game_title=game_title,
        )
    except Exception:
        logger.exception("Failed to parse RTM presence entry: %r", entry)
        return None


def player_id_from_rtm_entry(entry: Dict[str, Any]) -> Optional[str]:
    pid = entry.get("playerId")
    return str(pid) if pid else None