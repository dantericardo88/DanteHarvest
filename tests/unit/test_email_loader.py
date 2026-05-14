"""Unit tests for EmailLoader — CI-safe, all I/O via tmp_path or in-memory bytes."""

from __future__ import annotations

import email as email_stdlib
import mailbox
import textwrap
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

import pytest

from harvest_acquire.loaders.email_loader import (
    EmailLoader,
    EmailDocument,
    AttachmentMeta,
    _html_to_text,
    _parse_message,
)
from harvest_core.control.exceptions import NormalizationError


# ---------------------------------------------------------------------------
# _html_to_text
# ---------------------------------------------------------------------------

class TestHtmlToText:
    def test_strips_tags(self):
        result = _html_to_text("<p>Hello <b>world</b></p>")
        assert "<" not in result
        assert "Hello" in result
        assert "world" in result

    def test_decodes_html_entities(self):
        result = _html_to_text("&amp; &lt; &gt; &quot; &nbsp;")
        assert "&amp;" not in result
        assert "&" in result

    def test_collapses_whitespace(self):
        result = _html_to_text("<p>   lots   of   space   </p>")
        assert "  " not in result

    def test_empty_string(self):
        assert _html_to_text("") == ""

    def test_plain_text_passthrough(self):
        result = _html_to_text("no tags here")
        assert result == "no tags here"


# ---------------------------------------------------------------------------
# Helpers to build .eml bytes
# ---------------------------------------------------------------------------

def _make_eml(
    subject: str = "Test Subject",
    sender: str = "alice@example.com",
    to: str = "bob@example.com",
    body: str = "Hello, this is the body.",
    body_type: str = "plain",
) -> bytes:
    msg = MIMEText(body, body_type)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to
    msg["Date"] = "Thu, 14 May 2026 12:00:00 +0000"
    msg["Message-ID"] = "<test-001@example.com>"
    return msg.as_bytes()


def _make_multipart_eml(
    subject: str = "Multipart",
    sender: str = "alice@example.com",
    to: str = "bob@example.com",
    plain: str = "Plain body",
    html: str = "<p>HTML body</p>",
) -> bytes:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to
    msg["Date"] = "Thu, 14 May 2026 12:00:00 +0000"
    msg["Message-ID"] = "<multi-001@example.com>"
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))
    return msg.as_bytes()


def _make_eml_with_attachment(tmp_path: Path) -> bytes:
    msg = MIMEMultipart()
    msg["Subject"] = "With Attachment"
    msg["From"] = "alice@example.com"
    msg["To"] = "bob@example.com"
    msg["Date"] = "Thu, 14 May 2026 12:00:00 +0000"
    msg["Message-ID"] = "<att-001@example.com>"
    msg.attach(MIMEText("See attached.", "plain"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(b"binary content here")
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", "attachment", filename="report.pdf")
    msg.attach(part)
    return msg.as_bytes()


# ---------------------------------------------------------------------------
# EML loading
# ---------------------------------------------------------------------------

class TestEmailLoaderEML:
    def test_load_simple_eml(self, tmp_path):
        f = tmp_path / "test.eml"
        f.write_bytes(_make_eml())
        loader = EmailLoader()
        docs = loader.load(f)
        assert len(docs) == 1
        doc = docs[0]
        assert doc.subject == "Test Subject"
        assert "alice@example.com" in doc.sender
        assert "bob@example.com" in doc.recipients
        assert "Hello, this is the body." in doc.body_text

    def test_load_eml_date_and_message_id(self, tmp_path):
        f = tmp_path / "test.eml"
        f.write_bytes(_make_eml())
        docs = EmailLoader().load(f)
        assert "2026" in docs[0].date
        assert "test-001@example.com" in docs[0].message_id

    def test_load_eml_html_body_converted_to_text(self, tmp_path):
        f = tmp_path / "html.eml"
        f.write_bytes(_make_eml(body="<p>Hello <b>World</b></p>", body_type="html"))
        docs = EmailLoader().load(f)
        assert "<p>" not in docs[0].body_text
        assert "Hello" in docs[0].body_text
        assert "World" in docs[0].body_text

    def test_load_eml_multipart_prefers_plain(self, tmp_path):
        f = tmp_path / "multi.eml"
        f.write_bytes(_make_multipart_eml(plain="Plain preferred", html="<p>HTML</p>"))
        docs = EmailLoader().load(f)
        assert "Plain preferred" in docs[0].body_text

    def test_load_eml_with_attachment_metadata(self, tmp_path):
        f = tmp_path / "att.eml"
        f.write_bytes(_make_eml_with_attachment(tmp_path))
        docs = EmailLoader().load(f)
        assert len(docs[0].attachments) == 1
        att = docs[0].attachments[0]
        assert att.filename == "report.pdf"
        assert att.size_bytes > 0

    def test_load_eml_source_path_recorded(self, tmp_path):
        f = tmp_path / "test.eml"
        f.write_bytes(_make_eml())
        docs = EmailLoader().load(f)
        assert str(f) == docs[0].source_path

    def test_eml_markdown_contains_subject(self, tmp_path):
        f = tmp_path / "test.eml"
        f.write_bytes(_make_eml(subject="My Important Email"))
        docs = EmailLoader().load(f)
        md = docs[0].markdown
        assert "# My Important Email" in md
        assert "**From:**" in md
        assert "**To:**" in md

    def test_eml_markdown_contains_body_section(self, tmp_path):
        f = tmp_path / "test.eml"
        f.write_bytes(_make_eml(body="Check this out"))
        docs = EmailLoader().load(f)
        assert "## Body" in docs[0].markdown
        assert "Check this out" in docs[0].markdown


# ---------------------------------------------------------------------------
# MBOX loading
# ---------------------------------------------------------------------------

class TestEmailLoaderMBOX:
    def _write_mbox(self, path: Path, messages: list[bytes]) -> None:
        mbox = mailbox.mbox(str(path))
        for raw in messages:
            msg = email_stdlib.message_from_bytes(raw)
            mbox.add(msg)
        mbox.flush()
        mbox.close()

    def test_load_mbox_single_message(self, tmp_path):
        f = tmp_path / "single.mbox"
        self._write_mbox(f, [_make_eml(subject="Solo")])
        docs = EmailLoader().load(f)
        assert len(docs) == 1
        assert docs[0].subject == "Solo"

    def test_load_mbox_multiple_messages(self, tmp_path):
        f = tmp_path / "multi.mbox"
        self._write_mbox(f, [
            _make_eml(subject="First"),
            _make_eml(subject="Second"),
            _make_eml(subject="Third"),
        ])
        docs = EmailLoader().load(f)
        assert len(docs) == 3
        subjects = [d.subject for d in docs]
        assert "First" in subjects
        assert "Second" in subjects
        assert "Third" in subjects

    def test_load_empty_mbox(self, tmp_path):
        f = tmp_path / "empty.mbox"
        self._write_mbox(f, [])
        docs = EmailLoader().load(f)
        assert docs == []


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestEmailLoaderErrors:
    def test_missing_file_raises(self):
        loader = EmailLoader()
        with pytest.raises(NormalizationError, match="not found"):
            loader.load(Path("/nonexistent/file.eml"))

    def test_unsupported_suffix_raises(self, tmp_path):
        f = tmp_path / "messages.msg"
        f.write_bytes(b"content")
        loader = EmailLoader()
        with pytest.raises(NormalizationError, match="Unsupported"):
            loader.load(f)


# ---------------------------------------------------------------------------
# AttachmentMeta and EmailDocument dataclass
# ---------------------------------------------------------------------------

class TestEmailDocumentDataclass:
    def test_attachment_meta_fields(self):
        att = AttachmentMeta(filename="doc.pdf", content_type="application/pdf", size_bytes=1024)
        assert att.filename == "doc.pdf"
        assert att.size_bytes == 1024

    def test_email_document_no_subject_fallback(self):
        doc = EmailDocument(
            message_id="",
            sender="a@b.com",
            recipients=[],
            subject="",
            date="",
            body_text="",
        )
        assert "(no subject)" in doc.markdown

    def test_email_document_attachments_section(self):
        doc = EmailDocument(
            message_id="<x>",
            sender="a@b.com",
            recipients=["b@c.com"],
            subject="Hi",
            date="Today",
            body_text="Body here",
            attachments=[AttachmentMeta("file.zip", "application/zip", 512)],
        )
        md = doc.markdown
        assert "## Attachments" in md
        assert "file.zip" in md
