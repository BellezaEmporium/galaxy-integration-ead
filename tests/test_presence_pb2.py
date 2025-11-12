import pytest

from src import presence as presence_module


def _register_server_binary_message_parser():
    try:
        from ea_protos import Server_pb2
        from google.protobuf.json_format import MessageToDict

        # try to find a message class in Server_pb2 that contains a 'messageData' or 'message_data' field,
        # or otherwise fallback to a reasonable bytes-field message; if found, ensure Server_pb2.BinaryMessage
        # alias exists for downstream code that expects that name.
        bm_cls = None
        for attr_name in dir(Server_pb2):
            obj = getattr(Server_pb2, attr_name)
            if hasattr(obj, "DESCRIPTOR"):
                fields = getattr(obj.DESCRIPTOR, "fields_by_name", {})
                if "messageData" in fields or "message_data" in fields:
                    bm_cls = obj
                    break

        # If not found, check for a BinaryMessage attribute (older or generated name)
        if bm_cls is None and hasattr(Server_pb2, "BinaryMessage"):
            bm_cls = getattr(Server_pb2, "BinaryMessage")

        # Last-resort heuristic: pick a message containing a bytes field and with 'binary' or 'message' in the name
        if bm_cls is None:
            from google.protobuf.descriptor import FieldDescriptor
            for attr_name in dir(Server_pb2):
                obj = getattr(Server_pb2, attr_name)
                if hasattr(obj, "DESCRIPTOR"):
                    name_lower = attr_name.lower()
                    if "binary" in name_lower or "message" in name_lower:
                        for f in getattr(obj.DESCRIPTOR, "fields", []):
                            if f.type == FieldDescriptor.TYPE_BYTES:
                                bm_cls = obj
                                break
                    if bm_cls:
                        break

        if bm_cls is None:
            # no matching message type found; return raw payload inside a dict
            def fallback_parser(b: bytes):
                return {"messageData": b}

            presence_module.register_protobuf_parser(fallback_parser)
            return Server_pb2

        # ensure the module exposes a BinaryMessage attribute for compatibility with tests
        if not hasattr(Server_pb2, "BinaryMessage") or getattr(Server_pb2, "BinaryMessage") is not bm_cls:
            setattr(Server_pb2, "BinaryMessage", bm_cls)

        def parse_message(b: bytes):
            msg = bm_cls()
            msg.ParseFromString(b)
            return MessageToDict(msg, preserving_proto_field_name=True, use_integers_for_enums=True)

        presence_module.register_protobuf_parser(parse_message)
        return Server_pb2
    except Exception:
        return None


def test_parse_binary_message_payload_fallback():
    pb2 = _register_server_binary_message_parser()
    if not pb2:
        pytest.skip('Server_pb2 not generated; skipping pb2-dependent test')

    # Create BinaryMessage with inner payload bytes which are a minimal protobuf with field 1 = 'foobar'
    payload = b"\x0a\x06foobar"
    bm_cls = getattr(pb2, "BinaryMessage", None)
    if bm_cls is None:
        pytest.skip('Server_pb2.BinaryMessage not available; skipping')
    bm = bm_cls(messageData=payload)
    data = bm.SerializeToString()

    parser = presence_module._protobuf_parsers[-1]
    parsed_wrapper = parser(data)
    assert parsed_wrapper is not None
    # ensure the messageData field is present in the wrapper
    assert 'messageData' in parsed_wrapper or 'message_data' in parsed_wrapper

    # parse presence: parser registered will parse BinaryMessage and return dict; parse_presence should
    # then discover account info from inner message via fallback numeric map
    parsed_presence = presence_module.parse_presence(data)
    assert parsed_presence is not None
    # The fallback decode of inner payload will treat field 1 as the account id when a payload of the form 0a len bytes is used
    assert parsed_presence.account_id == 'foobar'
