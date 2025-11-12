from src.presence import parse_presence


def test_parse_presence_from_numeric_map():
    raw = {
        1: "\n\r10123456789012",
        2: 1,
        4: {
            1: "10123456789012:AQvu1z0d",
            2: 1,
            3: {1: 1762968951, 2: 235170724},
            7: [
                {1: 'ea_app.productId', 2: {1: 'Origin.OFR.50.0002694'}},
                {1: 'ea_app.presenceStatus', 2: {1: 'Apex Legends\u2122 Lobby (1/3)'}},
            ],
            12: {1: 1762968951, 2: 235170724},
            13: 1
        }
    }

    presence = parse_presence(raw)
    assert presence is not None
    assert presence.account_id == '10123456789012'
    assert presence.product_id == 'Origin.OFR.50.0002694'
    assert presence.presence_status is not None and 'Apex Legends' in presence.presence_status


def test_parse_presence_from_raw_bytes_fallback():
    # A minimal protobuf-like payload with field 1 = "foobar"; encoded as [tag=1,w2,len=6] + bytes
    payload = b"\x0a\x06foobar"
    presence = parse_presence(payload)
    assert presence is not None
    # Since field 1 is a top-level account key, we should get account_id foobar
    assert presence.account_id == 'foobar'


def test_presence_joinable_annotation():
    from src.presence import parse_presence, presence_to_user_presence
    raw = {
        1: "\n\r10123456789012",
        4: {
            1: "10123456789012:AQvu1z0d",
            7: [
                {1: 'ea_app.presenceStatus', 2: {1: 'Playing Solo'}},
                {1: 'ea_app.isJoinable', 2: {1: True}},
            ]
        }
    }
    presence = parse_presence(raw)
    assert presence is not None
    assert presence.is_joinable is True
    gp = presence_to_user_presence(presence)
    assert 'Joinable' in (gp.full_status or '')
