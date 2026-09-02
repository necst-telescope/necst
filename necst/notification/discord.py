"""Small Discord REST client for posting qlook images.

The notifier uses the Discord HTTP API directly so the Recorder/Analysis
container does not need a long-lived Discord gateway client. Bot credentials
are read from environment variables and are never included in log messages.
"""

from __future__ import annotations

import json
import os
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib import request

from ..utils import env_file as env_file_utils

DEFAULT_ATTACHMENT_LIMIT_BYTES = 10 * 1024 * 1024


class DiscordAttachmentTooLarge(RuntimeError):
    """Report an attachment rejected locally before a Discord upload."""

    def __init__(
        self,
        size_bytes: int,
        limit_bytes: int,
        *,
        notification_sent: bool,
        notification_error: Optional[Exception] = None,
    ) -> None:
        self.size_bytes = size_bytes
        self.limit_bytes = limit_bytes
        self.notification_sent = notification_sent
        self.notification_error = notification_error
        super().__init__(
            f"PNG size {size_bytes} bytes exceeds Discord attachment limit "
            f"{limit_bytes} bytes"
        )


def _configured_env_file() -> Optional[Path]:
    """Return the shared env file configured by the active site config.

    ``DISCORD_ENV_FILE`` is kept as a process-environment override for reduced
    environments and tests.  In a normal NECST process the preferred setting
    is ``[environment] env_file`` in the active site TOML.
    """

    raw_path = os.environ.get("DISCORD_ENV_FILE", "").strip()
    if not raw_path:
        try:
            from necst import config as necst_config

            for key in ("environment.env_file", "notification.discord.env_file"):
                try:
                    raw_path = str(necst_config.get(key) or "").strip()
                except (AttributeError, KeyError):
                    continue
                if raw_path:
                    break
        except Exception:
            # Direct environment variables remain usable in reduced/test
            # environments where the full NECST config is unavailable.
            raw_path = ""
    return Path(raw_path).expanduser() if raw_path else None


class DiscordNotifier:
    """Post a matplotlib figure as a PNG attachment to one Discord channel."""

    api_base = "https://discord.com/api/v10"

    def __init__(
        self,
        token: str,
        channel_id: str,
        *,
        opener: Optional[Callable[..., Any]] = None,
        timeout_sec: float = 30.0,
        attachment_limit_bytes: int = DEFAULT_ATTACHMENT_LIMIT_BYTES,
    ) -> None:
        if not token.strip():
            raise ValueError("Discord bot token must not be empty")
        if not channel_id.strip():
            raise ValueError("Discord channel ID must not be empty")
        if attachment_limit_bytes <= 0:
            raise ValueError("Discord attachment limit must be positive")
        self._token = token
        self.channel_id = channel_id
        self._opener = opener or request.urlopen
        self.timeout_sec = timeout_sec
        self.attachment_limit_bytes = attachment_limit_bytes

    @classmethod
    def from_environment(cls) -> "DiscordNotifier":
        """Create a notifier from site-configured env file or environment."""

        file_values: Dict[str, str] = {}
        configured_env_file = _configured_env_file()
        if configured_env_file is not None:
            if not configured_env_file.is_file():
                raise FileNotFoundError(
                    f"Environment file does not exist: {configured_env_file}"
                )
            file_values = env_file_utils.read(configured_env_file)

        token = file_values.get("DISCORD_BOT_TOKEN") or os.environ.get(
            "DISCORD_BOT_TOKEN", ""
        )
        channel_id = file_values.get("DISCORD_CHANNEL_ID") or os.environ.get(
            "DISCORD_CHANNEL_ID", ""
        )
        if not token or not channel_id:
            raise RuntimeError("DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID are required")
        raw_limit_mib = file_values.get(
            "DISCORD_ATTACHMENT_LIMIT_MIB"
        ) or os.environ.get("DISCORD_ATTACHMENT_LIMIT_MIB", "10")
        try:
            limit_mib = float(raw_limit_mib)
        except ValueError as exc:
            raise ValueError("DISCORD_ATTACHMENT_LIMIT_MIB must be a number") from exc
        if limit_mib <= 0:
            raise ValueError("DISCORD_ATTACHMENT_LIMIT_MIB must be positive")
        return cls(
            token,
            channel_id,
            attachment_limit_bytes=int(limit_mib * 1024 * 1024),
        )

    def send_figure(
        self,
        figure: Any,
        observation_name: str,
        *,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Encode ``figure`` in memory and upload it to Discord.

        The caller owns the figure lifecycle and should close it after this
        method returns. No output file is created.
        """

        buffer = BytesIO()
        figure.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
        buffer.seek(0)
        try:
            size_bytes = buffer.getbuffer().nbytes
            if size_bytes > self.attachment_limit_bytes:
                notification_error = None
                try:
                    safe_name = observation_name.replace("`", "'")
                    notice = (
                        "⚠️ Analysis image could not be uploaded\n\n"
                        f"Observation: `{safe_name}`\n\n"
                    )
                    if content:
                        notice = f"{content}\n\n{notice}"
                    notice += (
                        "Reason:\n"
                        f"PNG size {size_bytes / 1024 / 1024:.2f} MiB exceeds "
                        "the configured attachment limit "
                        f"({self.attachment_limit_bytes / 1024 / 1024:.2f} MiB)."
                    )
                    self._send_text(notice)
                except Exception as exc:
                    notification_error = exc
                raise DiscordAttachmentTooLarge(
                    size_bytes,
                    self.attachment_limit_bytes,
                    notification_sent=notification_error is None,
                    notification_error=notification_error,
                )
            return self._send_png(buffer, observation_name, content=content)
        finally:
            buffer.close()

    def send_text(self, content: str) -> Dict[str, Any]:
        """Post a text-only analysis result or diagnostic message."""

        return self._send_text(content)

    def _send_text(self, content: str) -> Dict[str, Any]:
        body = json.dumps({"content": content}, ensure_ascii=False).encode("utf-8")
        req = request.Request(
            self._message_url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bot {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "NECST-Analysis-Qlook/1.0",
            },
        )
        return self._open_json(req)

    def _send_png(
        self,
        image: BytesIO,
        observation_name: str,
        *,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        boundary = "----necst-discord-" + uuid.uuid4().hex
        safe_name = observation_name.replace("`", "'")
        payload = {
            "content": content or f"📡 Analysis Result\n\nObservation: `{safe_name}`"
        }
        body = b"".join(
            (
                self._part(
                    boundary,
                    "payload_json",
                    json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    content_type="application/json",
                ),
                self._part(
                    boundary,
                    "files[0]",
                    image.read(),
                    filename=f"{observation_name}_skydip.png",
                    content_type="image/png",
                ),
                f"--{boundary}--\r\n".encode("ascii"),
            )
        )
        req = request.Request(
            self._message_url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bot {self._token}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "User-Agent": "NECST-Analysis-Qlook/1.0",
            },
        )
        return self._open_json(req)

    @property
    def _message_url(self) -> str:
        return f"{self.api_base}/channels/{self.channel_id}/messages"

    def _open_json(self, req: request.Request) -> Dict[str, Any]:
        with self._opener(req, timeout=self.timeout_sec) as response:
            raw = response.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    @staticmethod
    def _part(
        boundary: str,
        field_name: str,
        content: bytes,
        *,
        filename: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> bytes:
        disposition = f'form-data; name="{field_name}"'
        if filename is not None:
            disposition += f'; filename="{filename}"'
        headers = [f"--{boundary}", f"Content-Disposition: {disposition}"]
        if content_type:
            headers.append(f"Content-Type: {content_type}")
        prefix = ("\r\n".join(headers) + "\r\n\r\n").encode("utf-8")
        return prefix + content + b"\r\n"
