"""Presence data parsing and conversion for Galaxy EA plugin.

Handles conversion of raw protobuf bytes or dict-like messages into structured Presence
objects, with support for registered protobuf message parsers.
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any, Callable, List
import base64
import logging

from galaxy.api.types import UserPresence as GalaxyUserPresence, PresenceState as GalaxyPresenceState

logger = logging.getLogger(__name__)

# Maximum nesting depth to prevent stack overflow on malformed data
MAX_NESTING_DEPTH = 10

# Registry for protobuf parsers built from generated classes
_protobuf_parsers: List[Callable[[bytes], Dict[Any, Any]]] = []


# ============================================================================
# Types and Constants
# ============================================================================

@dataclass
class Presence:
    """Intermediate presence data parsed from raw protobuf or dict sources."""
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


# ============================================================================
# Parser Registration
# ============================================================================

def register_protobuf_parser(parser_callable: Callable[[bytes], Dict[Any, Any]]) -> None:
    """Register a callable that converts raw protobuf bytes -> dict-like object.
    
    Args:
        parser_callable: Function that takes bytes and returns a dict, or None if it
                        cannot parse the input.
    """
    _protobuf_parsers.append(parser_callable)


# ============================================================================
# Entry Point: Main Parsing Function
# ============================================================================

def parse_presence(raw_message) -> Optional[Presence]:
    """Parse raw presence message (bytes or dict) into Presence object.
    
    This is the main entry point for presence parsing. Handles both:
    - Protobuf bytes (raw_message as bytes/bytearray)
    - Pre-decoded dicts (from other sources)
    
    Args:
        raw_message: Either bytes (protobuf) or dict (pre-decoded)
    
    Returns:
        Presence object if parsing succeeded, None on error
    """
    try:
        if isinstance(raw_message, (bytes, bytearray)):
            parsed = parse_protobuf_bytes(bytes(raw_message))
            if parsed is None:
                logger.debug("No protobuf parser could decode the message")
                return None
            return parse_presence_map(parsed)
        elif isinstance(raw_message, dict):
            return parse_presence_map(raw_message)
        else:
            logger.warning("Unsupported presence message type: %s", type(raw_message).__name__)
            return None
    except Exception as e:
        logger.exception("Unexpected error parsing presence: %s", e)
        return None


def presence_to_user_presence(p: Presence) -> GalaxyUserPresence:
    """Convert internal Presence to Galaxy API UserPresence for plugin callback.
    
    Mapping rules:
    - presence_state: Online if any presence indicators exist, Offline otherwise
    - game_id: '{product_id}:{multiplayer_id}' if both present
    - game_title: Direct mapping
    - in_game_status: rich_presence > presence_status > game_presence
    - full_status: Combines presence_status with joinability info if available
    
    Args:
        p: Parsed Presence object
    
    Returns:
        GalaxyUserPresence ready for plugin callback
    """
    # Determine online/offline state
    if p.is_joinable is False and p.is_joinable_invite_only is False and \
       p.presence_status is None and p.game_presence is None and p.rich_presence is None:
        state = GalaxyPresenceState.Offline
    elif p.presence_status or p.game_presence or p.rich_presence:
        state = GalaxyPresenceState.Online
    else:
        state = GalaxyPresenceState.Offline

    # Build game ID from product and multiplayer IDs
    game_id = None
    if p.product_id and p.multiplayer_id:
        game_id = f"{p.product_id}:{p.multiplayer_id}"

    # Build status strings
    in_game_status = p.rich_presence or p.presence_status or p.game_presence or None
    full_status = p.presence_status or in_game_status or None
    
    # Append joinability info
    if p.is_joinable:
        full_status = f"{full_status} (Joinable)" if full_status else "(Joinable)"
    elif p.is_joinable_invite_only:
        full_status = f"{full_status} (Invite-only)" if full_status else "(Invite-only)"

    return GalaxyUserPresence(
        presence_state=state,
        game_id=game_id,
        game_title=p.game_title,
        in_game_status=in_game_status,
        full_status=full_status
    )


# ============================================================================
# Protobuf Parsing: Registered Parsers + Fallback
# ============================================================================

def parse_protobuf_bytes(data: bytes) -> Optional[Dict[Any, Any]]:
    """Attempt to parse protobuf bytes using registered parsers or fallback decoder.
    
    Strategy:
    1. Try each registered parser (from generated pb2 classes)
    2. If a parser succeeds, attempt to decode nested base64 fields
    3. If no registered parser works, use fallback _decode_protobuf_to_numeric_map()
    
    Args:
        data: Raw protobuf bytes
    
    Returns:
        Decoded dict-like structure, or None if all parsers fail
    """
    # Try each registered protobuf parser
    for parser in _protobuf_parsers:
        try:
            parsed = parser(data)
            if not parsed:
                continue
            
            # If parser returned a dict, try to extract nested protobuf messages
            if isinstance(parsed, dict):
                # Look for common base64-encoded nested message fields
                for key, value in parsed.items():
                    if isinstance(value, str) and any(
                        needle in key.lower() 
                        for needle in ('message', 'data', 'payload')
                    ):
                        try:
                            nested_bytes = base64.b64decode(value)
                            # Try to decode nested as structured message first
                            numeric_map = _decode_protobuf_to_numeric_map(nested_bytes)
                            if numeric_map and any(k in numeric_map for k in (1, 4, 7)):
                                return numeric_map
                            # Otherwise try recursive protobuf parse
                            nested_parsed = parse_protobuf_bytes(nested_bytes)
                            if nested_parsed:
                                return nested_parsed
                        except Exception:
                            pass  # Not base64 or not parseable; continue
            
            return parsed
        except Exception as e:
            logger.debug(f"Registered parser failed: {e}")
            continue
    
    # Fallback: Raw protobuf decoding
    try:
        parsed = _decode_protobuf_to_numeric_map(data)
        if parsed:
            logger.debug("Decoded protobuf via fallback numeric decoder")
            return parsed
    except Exception as e:
        logger.debug(f"Fallback protobuf decode failed: {e}")
    
    return None


# ============================================================================
# Presence Map Parsing: Numeric-Keyed Dict Conversion
# ============================================================================

def parse_presence_map(raw: Dict[Any, Any], depth: int = 0) -> Presence:
    """Parse numeric-keyed dict (from protobuf decode or YAML) -> Presence object.
    
    Expects a structure like:
    {
        1: 'account_id_string',
        3: { 1: timestamp_value },
        4: { ... payload ... },
        7: [ { 1: 'ea_app.key', 2: { 1: value } }, ... ]
    }
    
    Args:
        raw: Numeric-keyed dict to parse
        depth: Current recursion depth (prevents deep recursion DoS)
    
    Returns:
        Presence object with extracted fields
    """
    if depth > MAX_NESTING_DEPTH:
        logger.warning("Max nesting depth exceeded, stopping parse")
        return Presence(raw=raw)
    
    payload = raw.get(4) or raw.get('4') or raw

    # Extract account ID (usually at payload[1] or raw[1])
    account_id = _extract_account_id(payload, raw)

    # Extract timestamp (usually at payload[3][1])
    ts = _extract_timestamp(payload)

    # Extract attributes from standard field 7 (key-value list)
    attrs = _extract_attributes_from_field_7(payload)
    
    # Fallback: recursively search for ea_app.* keys
    if not attrs:
        attrs = _collect_ea_keys(payload, depth + 1) or _collect_ea_keys(raw, depth + 1)

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


def _extract_account_id(payload: Any, raw: Any) -> Optional[str]:
    """Extract account ID from payload or raw dict."""
    account_raw = None
    if isinstance(payload, dict):
        account_raw = payload.get(1) or payload.get('1')
    if not account_raw:
        account_raw = raw.get(1) or raw.get('1') if isinstance(raw, dict) else None

    if isinstance(account_raw, str):
        # Account ID might have format "id:..." - extract just the ID part
        return account_raw.split(':', 1)[0]
    
    try:
        return str(account_raw) if account_raw else None
    except Exception:
        return None


def _extract_timestamp(payload: Any) -> Optional[int]:
    """Extract timestamp from payload[3][1] structure."""
    if not isinstance(payload, dict):
        return None
    
    try:
        p3 = payload.get(3)
        if isinstance(p3, dict) and 1 in p3:
            ts_val = p3.get(1)
            if isinstance(ts_val, (int, float)):
                return int(ts_val)
            elif isinstance(ts_val, str) and ts_val.isdigit():
                return int(ts_val)
    except Exception:
        pass
    
    return None


def _extract_attributes_from_field_7(payload: Any) -> Dict[str, Any]:
    """Extract attributes from field 7 (standard key-value list format)."""
    attrs = {}
    
    if not isinstance(payload, dict):
        return attrs
    
    entries = payload.get(7)
    if not isinstance(entries, list):
        return attrs
    
    for item in entries:
        try:
            key = item.get(1) or item.get('1')
            val = _get_nested_value(item.get(2) or item.get('2'))
            if isinstance(key, str):
                attrs[key] = val
        except Exception as e:
            logger.debug(f"Failed to decode field 7 entry: {e}")
    
    return attrs


def _collect_ea_keys(obj: Any, depth: int = 0) -> Dict[str, Any]:
    """Recursively search for 'ea_app.*' keys in nested structure.
    
    Fallback when field 7 extraction doesn't work. Searches the entire
    structure for keys starting with 'ea_app.' and extracts their values.
    
    Args:
        obj: Object to search (dict, list, or other)
        depth: Current recursion depth (to prevent deep recursion)
    
    Returns:
        Dict of ea_app.* keys found
    """
    if depth > MAX_NESTING_DEPTH:
        return {}
    
    out = {}
    
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.startswith("ea_app."):
                out[k] = _get_nested_value(v, depth + 1)
            else:
                out.update(_collect_ea_keys(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            out.update(_collect_ea_keys(item, depth + 1))
    
    return out


def _get_nested_value(val: Any, depth: int = 0) -> Any:
    """Extract a primitive value from nested dict/list structure.
    
    Handles structures like:
    - { 1: { 1: value } } -> value
    - [ { 1: value } ] -> value
    - 'string' -> 'string'
    
    Args:
        val: Value to extract from
        depth: Current recursion depth
    
    Returns:
        Extracted primitive value, or None if none found
    """
    if depth > MAX_NESTING_DEPTH:
        return None
    
    if val is None:
        return None
    
    if isinstance(val, (str, int, float, bool)):
        return val
    
    if isinstance(val, dict):
        # Try field 1 first (common structure)
        if 1 in val:
            return _get_nested_value(val[1], depth + 1)
        # Otherwise find first primitive
        for v in val.values():
            if isinstance(v, (str, int, float, bool)):
                return v
            nested = _get_nested_value(v, depth + 1)
            if nested is not None:
                return nested
        return None
    
    if isinstance(val, (list, tuple)):
        if not val:
            return None
        return _get_nested_value(val[0], depth + 1)
    
    return None


# ============================================================================
# Low-Level Protobuf Decoding: Wire Format Parser
# ============================================================================

def _decode_protobuf_to_numeric_map(data: bytes) -> Dict[int, Any]:
    """Decode raw protobuf wire format to numeric-keyed dict.
    
    This is a lightweight best-effort parser supporting:
    - Varint fields (wire type 0)
    - Length-delimited fields (wire type 2) including nested messages
    
    Unsupported wire types are logged and skipped.
    
    Args:
        data: Raw protobuf bytes
    
    Returns:
        Dict with numeric keys (field numbers) and decoded values
    """
    def _read_varint(buf: bytes, offset: int) -> tuple:
        """Read a varint from buffer at offset. Returns (value, next_offset)."""
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

    def _parse_message(buf: bytes, offset: int, end: int) -> Dict[int, Any]:
        """Parse a protobuf message in the given byte range."""
        out = {}
        pos = offset
        
        while pos < end:
            # Read key (field number + wire type)
            key, pos = _read_varint(buf, pos)
            field_number = key >> 3
            wire_type = key & 0x7

            if wire_type == 0:  # Varint field
                val, pos = _read_varint(buf, pos)
                out[field_number] = val
            
            elif wire_type == 2:  # Length-delimited field (strings, bytes, nested messages)
                length, pos = _read_varint(buf, pos)
                value_bytes = buf[pos:pos+length]
                
                # Prefer UTF-8 decoding for readability
                try:
                    s = value_bytes.decode('utf-8')
                    if s.isprintable():
                        out[field_number] = s
                    else:
                        # Not printable; try nested message decode
                        nested = _parse_message(value_bytes, 0, len(value_bytes))
                        out[field_number] = nested if nested else value_bytes.hex()
                except Exception:
                    # Not UTF-8; try nested message decode
                    try:
                        nested = _parse_message(value_bytes, 0, len(value_bytes))
                        out[field_number] = nested if nested else value_bytes.hex()
                    except Exception:
                        out[field_number] = value_bytes.hex()
                
                pos += length
            
            else:
                # Unsupported wire type (fixed32=1, fixed64=5, etc.)
                logger.debug(f"Unsupported protobuf wire type {wire_type}, stopping parse")
                break
        
        return out

    try:
        return _parse_message(data, 0, len(data))
    except Exception as e:
        logger.debug(f"Protobuf wire format decode failed: {e}")
        return {}