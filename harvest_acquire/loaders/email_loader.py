"""
EmailLoader — ingest .eml and .mbox email files.

Uses stdlib `email` + `mailbox` modules only — zero extra dependencies.

Extracts:
- sender, recipients, subject, date
- body: text/plain preferred; text/html stripped to plain text as fallback
- attachments list (filename, content-type, size)

Returns a list of EmailDocument objects, one per message.

Constitutional guarantees:
- Local-first: all processing is local, no network calls
- Fail-closed: missing file or unreadable format raises NormalizationError
- Zero-ambiguity: load() always returns list[EmailDocument]
- Privacy-aware: binary attachment content is NOT stored, only metadata
"""

from __future__ import annotations

import email
import mailbox
import re
from dataclasses import dataclass, field
from email import policy
from email.message import EmailMessage, Message
from pathlib import Path
from typing import List, Optional

from harvest_core.control.exceptions import NormalizationError


@dataclass
class AttachmentMeta:
    """Metadata about an email attachment (content not stored)."""
    filename: str
    content_type: str
    size_bytes: int


@dataclass
class EmailDocument:
    """A single email message extracted from .eml or .mbox."""
    message_id: str
    sender: str
    recipients: List[str]
    subject: str
    date: str
    body_text: str  # plain-text body (may be converted from HTML)
    attachments: List[AttachmentMeta] = field(default_factory=list)
    source_path: str = ""

    @property
    def markdown(self) -> str:
        """Render the email as a markdown document."""
        lines = [
            f"# {self.subject or '(no subject)'}",
            "",
            f"**From:** {self.sender}",
            f"**To:** {', '.join(self.recipients)}",
            f"**Date:** {self.date}",
            f"**Message-ID:** {self.message_id}",
            "",
        ]
        if self.body_text:
            lines += ["## Body", "", self.body_text, ""]
        if self.attachments:
            lines += ["## Attachments", ""]
            for att in self.attachments:
                lines.append(f"- `{att.filename}` ({att.content_type}, {att.size_bytes} bytes)")
            lines.append("")
        return "\n".join(lines)


def _html_to_text(html: str) -> str:
    """Very lightweight HTML→text: strip tags, collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_body_and_attachments(
    msg: Message,
) -> tuple[str, List[AttachmentMeta]]:
    """Walk the MIME tree; collect plain-text body and attachment metadata."""
    plain_parts: List[str] = []
    html_parts: List[str] = []
    attachments: List[AttachmentMeta] = []

    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            disp = str(part.get("Content-Disposition") or "")
            filename = part.get_filename()

            if "attachment" in disp or filename:
                payload = part.get_payload(decode=True)
                attachments.append(
                    AttachmentMeta(
                        filename=filename or "unnamed",
                        content_type=ct,
                        size_bytes=len(payload) if payload else 0,
                    )
                )
                continue

            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    plain_parts.append(payload.decode(charset, errors="replace"))
            elif ct == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html_parts.append(payload.decode(charset, errors="replace"))
    else:
        ct = msg.get_content_type()
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            if ct == "text/plain":
                plain_parts.append(text)
            elif ct == "text/html":
                html_parts.append(text)

    if plain_parts:
        body = "\n\n".join(plain_parts)
    elif html_parts:
        body = _html_to_text("\n".join(html_parts))
    else:
        body = ""

    return body, attachments


def _parse_message(msg: Message, source_path: str = "") -> EmailDocument:
    """Convert a stdlib email.Message into an EmailDocument."""
    sender = str(msg.get("From") or "")
    to_raw = str(msg.get("To") or "")
    cc_raw = str(msg.get("Cc") or "")
    recipients = [
        addr.strip()
        for addr in (to_raw + ("," + cc_raw if cc_raw else "")).split(",")
        if addr.strip()
    ]
    subject = str(msg.get("Subject") or "")
    date = str(msg.get("Date") or "")
    message_id = str(msg.get("Message-ID") or "")
    body, attachments = _extract_body_and_attachments(msg)

    return EmailDocument(
        message_id=message_id,
        sender=sender,
        recipients=recipients,
        subject=subject,
        date=date,
        body_text=body,
        attachments=attachments,
        source_path=source_path,
    )


class EmailLoader:
    """
    Load .eml and .mbox email files into EmailDocument objects.

    Usage:
        loader = EmailLoader()
        docs = loader.load(Path("messages.mbox"))
        for doc in docs:
            print(doc.markdown)
    """

    SUPPORTED_SUFFIXES = {".eml", ".mbox"}

    def load(self, path: Path) -> List[EmailDocument]:
        """
        Load email file. Returns list of EmailDocument (1 for .eml, N for .mbox).
        Raises NormalizationError on missing file or unreadable format.
        """
        path = Path(path)
        if not path.exists():
            raise NormalizationError(f"Email file not found: {path}")
        if not path.is_file():
            raise NormalizationError(f"Path is not a file: {path}")

        suffix = path.suffix.lower()
        if suffix not in self.SUPPORTED_SUFFIXES:
            raise NormalizationError(
                f"Unsupported email format: {suffix!r}. "
                f"Supported: {', '.join(sorted(self.SUPPORTED_SUFFIXES))}"
            )

        if suffix == ".eml":
            return self._load_eml(path)
        else:
            return self._load_mbox(path)

    def _load_eml(self, path: Path) -> List[EmailDocument]:
        """Parse a single .eml file."""
        try:
            raw = path.read_bytes()
            msg = email.message_from_bytes(raw, policy=policy.compat32)
        except Exception as exc:
            raise NormalizationError(f"Failed to parse .eml {path}: {exc}") from exc

        return [_parse_message(msg, source_path=str(path))]

    def _load_mbox(self, path: Path) -> List[EmailDocument]:
        """Parse a .mbox file — may contain multiple messages."""
        try:
            mbox = mailbox.mbox(str(path))
        except Exception as exc:
            raise NormalizationError(f"Failed to open .mbox {path}: {exc}") from exc

        docs: List[EmailDocument] = []
        try:
            for msg in mbox:
                try:
                    docs.append(_parse_message(msg, source_path=str(path)))
                except Exception:
                    # Skip malformed messages but continue processing
                    continue
        finally:
            mbox.close()

        return docs
