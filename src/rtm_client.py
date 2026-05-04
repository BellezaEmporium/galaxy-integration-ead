import asyncio
import json
import logging
import random
import ssl
import struct
from typing import Callable, Dict, Optional, List

from google.protobuf.message import DecodeError

# -- protobuf generated stubs (see rtm_pb2.py below) --
from generated_protos import rtm_pb2, requests_pb2

logger = logging.getLogger(__name__)

RTM_HOST = "rtm.tnt-ea.com"
RTM_PORT = 9000

BASIC_PRESENCE_OFFLINE  = 2
BASIC_PRESENCE_ONLINE   = 3
BASIC_PRESENCE_DND      = 4
BASIC_PRESENCE_AWAY     = 5
BASIC_PRESENCE_INVISIBLE = 6

PLATFORM_PC   = 3
USER_NUCLEUS  = 2


class RtmClient:
    """
    Async EA RTM client.  Maintains a persistent TLS TCP connection,
    logs in, subscribes to a friend list, and delivers presence updates
    via callback.
    """

    def __init__(
        self,
        access_token_provider: Callable[[], Optional[str]],
        on_presence_update: Callable[[str, dict], None],
        reconnect_delay: float = 5.0,
    ) -> None:
        self._get_token = access_token_provider
        self._on_presence = on_presence_update
        self._reconnect_delay = reconnect_delay

        self._subscribed_ids: list[str] = []
        self._request_index: int = 0

        self._writer: Optional[asyncio.StreamWriter] = None
        self._task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._close_writer()

    def set_friends(self, nucleus_ids: "list[str]") -> None:
        """Update the list of friend IDs to subscribe to."""
        self._subscribed_ids = list(nucleus_ids)

    # ------------------------------------------------------------------ #
    #  Connection loop                                                     #
    # ------------------------------------------------------------------ #

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("RTM connection lost (%s), retrying in %.1fs", exc, self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)

    async def _connect_and_run(self) -> None:
        ssl_ctx = ssl.create_default_context()
        reader, writer = await asyncio.open_connection(RTM_HOST, RTM_PORT, ssl=ssl_ctx)
        self._writer = writer
        logger.info("RTM TCP+TLS connected to %s:%d", RTM_HOST, RTM_PORT)

        try:
            await self._login()
            if self._subscribed_ids:
                await self._subscribe(self._subscribed_ids)
            await self._receive_loop(reader)
        finally:
            self._close_writer()

    # ------------------------------------------------------------------ #
    #  Frame read/write                                                    #
    # ------------------------------------------------------------------ #

    async def _receive_loop(self, reader: asyncio.StreamReader) -> None:
        buf = b""
        expected = -1

        while self._running:
            chunk = await reader.read(4096)
            if not chunk:
                logger.warning("RTM socket closed by server")
                return
            buf += chunk

            while True:
                if expected == -1:
                    if len(buf) < 4:
                        break
                    (expected,) = struct.unpack(">i", buf[:4])
                    buf = buf[4:]

                if len(buf) < expected:
                    break

                raw = buf[:expected]
                buf = buf[expected:]
                expected = -1

                try:
                    comm = rtm_pb2.Communication()
                    comm.ParseFromString(raw)
                    await self._dispatch(comm)
                except DecodeError as exc:
                    logger.debug("RTM decode error: %s", exc)

    async def _send(self, body_name: str, body_msg) -> None:
        """Encode a CommunicationV1 body and write it with a 4-byte length prefix."""
        comm = rtm_pb2.Communication()
        v1 = comm.v1
        v1.requestId = self._next_request_id()
        getattr(v1, body_name).CopyFrom(body_msg)

        payload = comm.SerializeToString()
        frame = struct.pack(">i", len(payload)) + payload
        if self._writer:
            self._writer.write(frame)
            await self._writer.drain()

    def _next_request_id(self) -> str:
        import time
        ts = int(time.time())
        rid = f"c-{self._request_index}-{ts}-{ts}"
        self._request_index += 1
        return rid

    def _close_writer(self) -> None:
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
            self._writer = None

    # ------------------------------------------------------------------ #
    #  RTM protocol messages                                               #
    # ------------------------------------------------------------------ #

    async def _login(self) -> None:
        token = self._get_token()
        if not token:
            raise RuntimeError("No access token available for RTM login")

        import os
        version_str = json.dumps({
            "clientType": "Client",
            "version": "galaxy-plugin-1.0.0-rtm",
            "integrations": "",
        })

        req = requests_pb2.LoginRequestV3()
        req.token = token
        req.reconnect = False
        req.heartbeat = False
        req.userType = USER_NUCLEUS
        req.productId = "origin"
        req.platform = PLATFORM_PC
        req.clientVersion = version_str

        await self._send("loginRequestV3", req)
        logger.info("RTM LoginRequestV3 sent")

    async def _subscribe(self, nucleus_ids: List[str]) -> None:
        req = requests_pb2.PresenceSubscribeV1()
        for pid in nucleus_ids:
            p = req.players.add()
            p.playerId = pid
            p.productId = "origin"
        await self._send("presenceSubscribe", req)
        logger.debug("RTM PresenceSubscribeV1 sent for %d players", len(nucleus_ids))

    async def _heartbeat(self) -> None:
        await self._send("heartbeat", requests_pb2.HeartbeatV1())

    # ------------------------------------------------------------------ #
    #  Incoming message dispatch                                           #
    # ------------------------------------------------------------------ #

    async def _dispatch(self, comm: "rtm_pb2.Communication") -> None:
        v1 = comm.v1
        if not v1:
            return

        body_type = v1.WhichOneof("body")

        if body_type == "presence":
            self._handle_presence(v1.presence)
        elif body_type == "success":
            logger.debug("RTM success response for request %s", v1.requestId)
        elif body_type == "error":
            err = v1.error
            logger.warning("RTM error %s: %s", err.errorCode, err.errorMessage)
        elif body_type == "heartbeat":
            # Echo back
            await self._heartbeat()
        else:
            logger.debug("RTM unhandled body type: %s", body_type)

    def _handle_presence(self, presence: "rtm_pb2.PresenceV1") -> None:
        # Only process updates from EA Desktop clients
        if presence.HasField("clientVersion"):
            try:
                cv = json.loads(presence.clientVersion)
                if cv.get("clientType") not in ("Client", "LegacyClient"):
                    return
            except (json.JSONDecodeError, AttributeError):
                return
        else:
            # If no clientVersion, skip to avoid duplicates from mobile/web sessions
            return

        player = presence.player
        if not player or not player.playerId:
            logger.debug("RTM presence update with no player ID, skipping")
            return

        pid = player.playerId
        basic = presence.basicPresenceType  # int

        game_id: Optional[str] = None
        game_title: Optional[str] = None

        if presence.HasField("richPresence"):
            rp = presence.richPresence
            game_title = rp.game or None
            try:
                custom = json.loads(rp.customRichPresenceData)
                game_id = custom.get("gameProductId") or None
            except (json.JSONDecodeError, AttributeError):
                pass

        self._on_presence(pid, {
            "playerId": pid,
            "basicPresenceType": basic,
            "gameId": game_id,
            "gameTitle": game_title,
        })