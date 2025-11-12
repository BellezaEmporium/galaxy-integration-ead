from dataclasses import dataclass
from typing import Optional, Dict, Any, Callable, List
import base64
import logging
from galaxy.api.types import UserPresence as GalaxyUserPresence, PresenceState as GalaxyPresenceState

logger = logging.getLogger(__name__)

@dataclass
class Presence:
    account_id: Optional[str] = None
    timestamp: Optional[int] = None
    product_id: Optional[str] = None
    multiplayer_id: Optional[str] = None
    presence_status: Optional[str] = None
    rich_presence: Optional[str] = None
    game_presence: Optional[str] = None
    game_title: Optional[str] = None
    game_session_string: Optional[str] = None
    is_joinable: Optional[bool] = None
    is_joinable_invite_only: Optional[bool] = None
    raw: Optional[Dict[Any, Any]] = None

# Registry for protobuf parsers built from generated classes
_protobuf_parsers: List[Callable[[bytes], Dict[Any, Any]]] = []

def register_protobuf_parser(parser_callable: Callable[[bytes], Dict[Any, Any]]):
    """Register a callable that converts raw protobuf bytes -> dict-like object."""
    _protobuf_parsers.append(parser_callable)

def _get_nested_value(val):
    if val is None:
        return None
    if isinstance(val, dict):
        if 1 in val:
            return _get_nested_value(val[1])
        # choose first primitive
        for v in val.values():
            if isinstance(v, (str, int, float, bool)):
                return v
            nested = _get_nested_value(v)
            if nested is not None:
                return nested
        return None
    if isinstance(val, list):
        if not val:
            return None
        return _get_nested_value(val[0])
    return val

def parse_presence_map(raw: Dict[Any, Any]) -> Presence:
    """Parse a numeric-keyed map (decode_raw/protobuf output or YAML) -> Presence"""
    payload = raw.get(4) or raw.get('4') or raw

    # account id -> usually payload[1] or raw[1]
    account_raw = None
    if isinstance(payload, dict):
        account_raw = payload.get(1) or payload.get('1')
    if not account_raw:
        account_raw = raw.get(1) or raw.get('1')

    account_id = None
    if isinstance(account_raw, str):
        account_id = account_raw.split(':', 1)[0]
    else:
        try:
            account_id = str(account_raw)
        except Exception:
            account_id = None

    # timestamp: often under [3][1]
    ts = None
    try:
        if isinstance(payload, dict) and 3 in payload:
            p3 = payload.get(3)
            if isinstance(p3, dict) and 1 in p3:
                ts_val = p3.get(1)
                if isinstance(ts_val, (int, float)):
                    ts = int(ts_val)
                elif isinstance(ts_val, str) and ts_val.isdigit():
                    ts = int(ts_val)
    except Exception:
        ts = None

    attrs = {}
    # Standard '7' is list of entries {1: key, 2: {1:value}}
    if isinstance(payload, dict) and 7 in payload:
        entries = payload.get(7)
        if isinstance(entries, list):
            for item in entries:
                try:
                    key = item.get(1) or item.get('1')
                    val = _get_nested_value(item.get(2) or item.get('2'))
                    if isinstance(key, str):
                        attrs[key] = val
                except Exception:
                    logger.debug("Failed decode entry: %s", item)

    # Fallback: search recursively for ea_app.* keys
    if not attrs:
        def collect_ea_keys(obj):
            out = {}
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if isinstance(k, str) and k.startswith("ea_app."):
                        out[k] = _get_nested_value(v)
                    else:
                        out.update(collect_ea_keys(v))
            elif isinstance(obj, list):
                for item in obj:
                    out.update(collect_ea_keys(item))
            return out
        attrs = collect_ea_keys(payload) or collect_ea_keys(raw)

    return Presence(
        account_id=account_id,
        timestamp=ts,
        product_id=attrs.get('ea_app.productId'),
        multiplayer_id=attrs.get('ea_app.multiplayerId'),
        presence_status=attrs.get('ea_app.presenceStatus'),
        rich_presence=attrs.get('ea_app.richPresence'),
        game_presence=attrs.get('ea_app.gamePresence'),
        game_title=attrs.get('ea_app.gameTitle'),
        game_session_string=attrs.get('ea_app.gameSessionString'),
        is_joinable=bool(attrs.get('ea_app.isJoinable')),
        is_joinable_invite_only=bool(attrs.get('ea_app.isJoinableInviteOnly')),
        raw=raw
    )

def parse_protobuf_bytes(data: bytes) -> Optional[Dict[Any, Any]]:
    """Try registered parsers (MessageToDict calls) to convert bytes to dict-like."""
    for p in _protobuf_parsers:
        try:
            parsed = p(data)
            if parsed:
                    # If parser returned a dict wrapper that contains nested bytes as base64
                    if isinstance(parsed, dict):
                        # Look for common fields where bytes might be stored
                        for k, v in parsed.items():
                            if isinstance(v, str) and ('message' in k.lower() or 'data' in k.lower() or 'payload' in k.lower()):
                                try:
                                    nested_bytes = base64.b64decode(v)
                                    # Prefer direct numeric-map decode for nested protobuf if it contains structured fields
                                    try:
                                        numeric_map = _decode_protobuf_to_numeric_map(nested_bytes)
                                        # If numeric_map results in meaningful structure, return it
                                        if numeric_map and (1 in numeric_map or 4 in numeric_map or 7 in numeric_map):
                                            return numeric_map
                                    except Exception:
                                        pass
                                    nested_parsed = parse_protobuf_bytes(nested_bytes)
                                    if nested_parsed:
                                        return nested_parsed
                                except Exception:
                                    # not base64 or not parseable; continue
                                    pass
                    return parsed
        except Exception:
            logger.debug("Protobuf parser failed, next", exc_info=True)
    # Fallback: try a raw decode into numeric keyed map and return that
    try:
        parsed = _decode_protobuf_to_numeric_map(data)
        if parsed:
            return parsed
    except Exception:
        logger.debug("Raw protobuf decode failed", exc_info=True)

    return None


def _decode_protobuf_to_numeric_map(data: bytes) -> Dict[Any, Any]:
    """Basic protobuf wire format parser that returns a numeric-keyed map (dict).

    This is a lightweight and best-effort parser. It supports varint and
    length-delimited fields (including nested messages) which are sufficient
    for decoding many raw messages into a numeric-keyed dict.
    """
    def _read_varint(buf: bytes, offset: int):
        result = 0
        shift = 0
        pos = offset
        while pos < len(buf):
            b = buf[pos]
            pos += 1
            result |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7
        return result, pos

    def _parse_message(buf: bytes, off: int, end: int):
        out = {}
        pos = off
        while pos < end:
            key, pos = _read_varint(buf, pos)
            field_number = key >> 3
            wire_type = key & 0x7

            if wire_type == 0:  # varint
                val, pos = _read_varint(buf, pos)
                out[field_number] = val
            elif wire_type == 2:  # length-delimited
                length, pos = _read_varint(buf, pos)
                value_bytes = buf[pos:pos+length]
                # Prefer to decode as utf-8 string for readability for e.g. account ids
                try:
                    s = value_bytes.decode('utf-8')
                    # If it decodes as printable text, use it
                    if s.isprintable():
                        out[field_number] = s
                    else:
                        # otherwise attempt nested parse
                        nested = _parse_message(value_bytes, 0, len(value_bytes))
                        if nested:
                            out[field_number] = nested
                        else:
                            out[field_number] = value_bytes.hex()
                except Exception:
                    # not utf-8; attempt nested message parse
                    try:
                        nested = _parse_message(value_bytes, 0, len(value_bytes))
                        if nested:
                            out[field_number] = nested
                        else:
                            out[field_number] = value_bytes.hex()
                    except Exception:
                        out[field_number] = value_bytes.hex()
                pos += length
            else:
                # Unsupported wire type: store raw hex remainder
                out[field_number] = buf[pos:pos+1].hex()
                break
        return out

    return _parse_message(data, 0, len(data))

def parse_presence(raw_message) -> Optional[Presence]:
    """General parsing entrypoint. Accepts dict-like (already decoded) or bytes (protobuf)."""
    try:
        if isinstance(raw_message, (bytes, bytearray)):
            parsed = parse_protobuf_bytes(bytes(raw_message))
            if parsed is None:
                logger.debug("No protobuf parser matched")
                return None
            return parse_presence_map(parsed)
        elif isinstance(raw_message, dict):
            return parse_presence_map(raw_message)
        else:
            logger.debug("Unsupported presence message type: %s", type(raw_message))
            return None
    except Exception:
        logger.exception("Error parsing presence")
        return None


def presence_to_user_presence(p: Presence) -> GalaxyUserPresence:
    """Convert internal Presence model to Galaxy API UserPresence.

    Mapping rules:
    - presence_state: Online if any of presence_status/game_presence/rich_presence present, otherwise Offline
    - game_id: derived from product_id and multiplayer_id as '<product_id>:<multiplayer_id>' if both present
    - game_title: game_title or None
    - in_game_status: rich_presence or presence_status or game_presence
    - full_status: presence_status or in_game_status
    """
    # If presence is explicitly invisible, treat as offline
    if p.is_joinable is False and p.is_joinable_invite_only is False and p.presence_status is None and p.game_presence is None and p.rich_presence is None:
        # no presence info -> offline
        state = GalaxyPresenceState.Offline
    elif p.presence_status or p.game_presence or p.rich_presence:
        state = GalaxyPresenceState.Online
    else:
        state = GalaxyPresenceState.Offline

    game_id = None
    if p.product_id and p.multiplayer_id:
        # unify to a simple string; plugin may translate to OfferId if needed
        game_id = f"{p.product_id}:{p.multiplayer_id}"

    in_game_status = p.rich_presence or p.presence_status or p.game_presence or None
    full_status = p.presence_status or in_game_status or None
    # Append joinability information into the full_status so UI can present it
    if p.is_joinable:
        if full_status:
            full_status = f"{full_status} (Joinable)"
        else:
            full_status = "(Joinable)"
    elif p.is_joinable_invite_only:
        if full_status:
            full_status = f"{full_status} (Invite-only)"
        else:
            full_status = "(Invite-only)"

    return GalaxyUserPresence(
        presence_state=state,
        game_id=game_id,
        game_title=p.game_title,
        in_game_status=in_game_status,
        full_status=full_status
    )