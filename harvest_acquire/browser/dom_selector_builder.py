"""
DOMSelectorBuilder — builds CSS selectors from page snapshot text.

No external dependencies; pure stdlib + regex parsing of raw HTML.

Constitutional guarantees:
- No network calls; purely local string analysis
- Never raises on malformed HTML — falls back gracefully
- Returns non-empty fallback selectors so callers always get something usable
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _attr(tag: str, name: str) -> Optional[str]:
    """Extract the value of an HTML attribute from a tag string.

    Handles single-quotes, double-quotes, and unquoted values.
    Returns None when attribute is absent.
    """
    pattern = rf'{name}\s*=\s*(?:"([^"]*?)"|\'([^\']*?)\'|([^\s>\'"/]+))'
    m = re.search(pattern, tag, re.IGNORECASE)
    if m:
        return m.group(1) or m.group(2) or m.group(3) or ""
    return None


def _inner_text(tag_start: str, html: str) -> str:
    """Return stripped inner text for a self-contained inline tag occurrence.

    Works by finding the closing tag right after the opening tag.
    Returns empty string on failure.
    """
    try:
        close_bracket = html.index(">", html.index(tag_start))
        # Find tag name from tag_start
        tag_name_m = re.match(r"<(\w+)", tag_start)
        if not tag_name_m:
            return ""
        tag_name = tag_name_m.group(1)
        close_tag = f"</{tag_name}>"
        close_pos = html.find(close_tag, close_bracket)
        if close_pos == -1:
            return ""
        text = html[close_bracket + 1:close_pos].strip()
        # Strip nested tags
        text = re.sub(r"<[^>]+>", "", text).strip()
        return text
    except (ValueError, IndexError):
        return ""


def _css_escape(value: str) -> str:
    """Minimally escape a value for use in a CSS attribute selector."""
    # Escape double-quotes inside the value
    return value.replace('"', '\\"')


# ---------------------------------------------------------------------------
# DOMSelectorBuilder
# ---------------------------------------------------------------------------

class DOMSelectorBuilder:
    """Builds CSS selectors from page snapshot text rather than guessing.

    All methods are pure functions — no state retained between calls.
    """

    # ------------------------------------------------------------------
    # extract_interactive_elements
    # ------------------------------------------------------------------

    def extract_interactive_elements(self, snapshot: str) -> Dict[str, List[str]]:
        """Parse a text/HTML snapshot and return categorized selectors.

        Returns dict with keys:
          - 'buttons': list of selectors for clickable buttons
          - 'inputs':  list of selectors for text inputs
          - 'links':   list of selectors for links
          - 'forms':   list of selectors for forms
          - 'submit':  list of selectors for submit actions

        Preference order per element: #id > [name=] > [aria-label=] > .class > tag
        """
        result: Dict[str, List[str]] = {
            "buttons": [],
            "inputs": [],
            "links": [],
            "forms": [],
            "submit": [],
        }

        if not snapshot:
            return result

        # --- Buttons --------------------------------------------------------
        for m in re.finditer(r"<button([^>]*)>", snapshot, re.IGNORECASE | re.DOTALL):
            attrs = m.group(1)
            sel = self._best_selector("button", attrs)
            if sel and sel not in result["buttons"]:
                result["buttons"].append(sel)
            # Also capture submit buttons
            btn_type = _attr(attrs, "type")
            if btn_type and btn_type.lower() == "submit":
                if sel and sel not in result["submit"]:
                    result["submit"].append(sel)

        # --- Inputs ---------------------------------------------------------
        for m in re.finditer(r"<input([^>]*)>?", snapshot, re.IGNORECASE | re.DOTALL):
            attrs = m.group(1)
            input_type = (_attr(attrs, "type") or "text").lower()
            # Skip hidden inputs
            if input_type == "hidden":
                continue
            sel = self._best_input_selector(attrs)
            if sel and sel not in result["inputs"]:
                result["inputs"].append(sel)
            if input_type == "submit":
                if sel and sel not in result["submit"]:
                    result["submit"].append(sel)

        # --- Links ----------------------------------------------------------
        for m in re.finditer(r"<a([^>]*)>", snapshot, re.IGNORECASE | re.DOTALL):
            attrs = m.group(1)
            href = _attr(attrs, "href")
            if not href:
                continue
            elem_id = _attr(attrs, "id")
            aria = _attr(attrs, "aria-label")
            css_cls = _attr(attrs, "class")
            if elem_id:
                sel = f"#{elem_id}"
            elif aria:
                sel = f'a[aria-label="{_css_escape(aria)}"]'
            elif href and href not in ("#", "javascript:void(0)", "javascript:;"):
                sel = f'a[href="{_css_escape(href)}"]'
            elif css_cls:
                first_cls = css_cls.split()[0]
                sel = f"a.{first_cls}"
            else:
                continue
            if sel not in result["links"]:
                result["links"].append(sel)

        # --- Forms ----------------------------------------------------------
        for m in re.finditer(r"<form([^>]*)>", snapshot, re.IGNORECASE | re.DOTALL):
            attrs = m.group(1)
            elem_id = _attr(attrs, "id")
            action = _attr(attrs, "action")
            if elem_id:
                sel = f"#{elem_id}"
            elif action:
                sel = f'form[action="{_css_escape(action)}"]'
            else:
                sel = "form"
            if sel not in result["forms"]:
                result["forms"].append(sel)

        # Fallback: guarantee at least one submit selector
        if not result["submit"]:
            result["submit"] = ["button[type=submit]", "input[type=submit]"]

        return result

    # ------------------------------------------------------------------
    # build_click_selector
    # ------------------------------------------------------------------

    def build_click_selector(self, target_text: str, snapshot: str) -> str:
        """Find the best CSS selector for an element matching target_text in snapshot.

        Strategy:
        1. Exact text match inside <button> or <a> tags
        2. aria-label attribute match
        3. placeholder match
        4. Fall back to generic: button, [role=button], a
        """
        if not snapshot or not target_text:
            return "button, [role=button], a"

        target_lower = target_text.lower().strip()

        # 1. Check <button> tags for text content match
        for m in re.finditer(r"(<button([^>]*)>)(.*?)</button>",
                              snapshot, re.IGNORECASE | re.DOTALL):
            btn_attrs = m.group(2)
            inner = re.sub(r"<[^>]+>", "", m.group(3)).strip()
            if inner.lower() == target_lower:
                sel = self._best_selector("button", btn_attrs)
                if sel:
                    return sel

        # 2. Check <a> tags for text content match
        for m in re.finditer(r"(<a([^>]*)>)(.*?)</a>",
                              snapshot, re.IGNORECASE | re.DOTALL):
            a_attrs = m.group(2)
            inner = re.sub(r"<[^>]+>", "", m.group(3)).strip()
            if inner.lower() == target_lower:
                elem_id = _attr(a_attrs, "id")
                href = _attr(a_attrs, "href")
                if elem_id:
                    return f"#{elem_id}"
                if href:
                    return f'a[href="{_css_escape(href)}"]'

        # 3. Check aria-label on any clickable element
        for m in re.finditer(r"<(?:button|a|div|span|input)([^>]*)>",
                              snapshot, re.IGNORECASE | re.DOTALL):
            attrs = m.group(1)
            aria = _attr(attrs, "aria-label")
            if aria and aria.lower() == target_lower:
                tag = re.match(r"<(\w+)", m.group()).group(1).lower()
                sel = self._best_selector(tag, attrs)
                if sel:
                    return sel

        # 4. Partial text match in button tags (case-insensitive)
        for m in re.finditer(r"(<button([^>]*)>)(.*?)</button>",
                              snapshot, re.IGNORECASE | re.DOTALL):
            btn_attrs = m.group(2)
            inner = re.sub(r"<[^>]+>", "", m.group(3)).strip()
            if target_lower in inner.lower():
                sel = self._best_selector("button", btn_attrs)
                if sel:
                    return sel

        return "button, [role=button], a"

    # ------------------------------------------------------------------
    # build_input_selector
    # ------------------------------------------------------------------

    def build_input_selector(self, field_hint: str, snapshot: str) -> str:
        """Find input selector matching field_hint.

        Strategy:
        1. input[name=<field_hint>] if name attr matches
        2. input[placeholder*=<field_hint>] if placeholder contains hint
        3. input[type=<field_hint>] for type=email, type=search etc
        4. input#<field_hint> if id matches
        5. Fall back to input[type=text]:first-of-type
        """
        if not snapshot or not field_hint:
            return "input[type=text]:first-of-type"

        hint_lower = field_hint.lower().strip()

        for m in re.finditer(r"<input([^>]*)>?", snapshot, re.IGNORECASE | re.DOTALL):
            attrs = m.group(1)
            input_type = (_attr(attrs, "type") or "text").lower()
            if input_type == "hidden":
                continue

            name = _attr(attrs, "name") or ""
            placeholder = _attr(attrs, "placeholder") or ""
            elem_id = _attr(attrs, "id") or ""
            input_type_val = input_type

            # 1. Exact name match
            if name.lower() == hint_lower:
                return f'input[name="{_css_escape(name)}"]'

            # 4. Exact id match
            if elem_id.lower() == hint_lower:
                return f"#{elem_id}"

            # 3. type match (e.g. hint="email" → input[type=email])
            if input_type_val == hint_lower and input_type_val not in ("text", "hidden"):
                return f"input[type={input_type_val}]"

            # 2. Placeholder contains hint
            if hint_lower in placeholder.lower():
                return f'input[placeholder*="{_css_escape(hint_lower)}"]'

        return "input[type=text]:first-of-type"

    # ------------------------------------------------------------------
    # build_navigate_selector
    # ------------------------------------------------------------------

    def build_navigate_selector(self, url_hint: str, snapshot: str) -> Optional[str]:
        """Find link selector for a given URL hint.

        Returns None when no matching link is found in snapshot.
        """
        if not snapshot or not url_hint:
            return None

        url_hint_lower = url_hint.lower()

        for m in re.finditer(r"<a([^>]*)>", snapshot, re.IGNORECASE | re.DOTALL):
            attrs = m.group(1)
            href = _attr(attrs, "href") or ""
            if url_hint_lower in href.lower():
                elem_id = _attr(attrs, "id")
                if elem_id:
                    return f"#{elem_id}"
                return f'a[href="{_css_escape(href)}"]'

        return None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _best_selector(self, tag: str, attrs: str) -> Optional[str]:
        """Build the highest-specificity selector for a tag+attrs string.

        Preference: #id > [aria-label=] > .class > tag
        """
        elem_id = _attr(attrs, "id")
        if elem_id:
            return f"#{elem_id}"

        aria = _attr(attrs, "aria-label")
        if aria:
            return f'{tag}[aria-label="{_css_escape(aria)}"]'

        css_cls = _attr(attrs, "class")
        if css_cls:
            first_cls = css_cls.split()[0]
            if first_cls:
                return f"{tag}.{first_cls}"

        return tag

    def _best_input_selector(self, attrs: str) -> Optional[str]:
        """Build a specific selector for an <input> element."""
        elem_id = _attr(attrs, "id")
        if elem_id:
            return f"#{elem_id}"

        name = _attr(attrs, "name")
        if name:
            return f'input[name="{_css_escape(name)}"]'

        placeholder = _attr(attrs, "placeholder")
        if placeholder:
            # Use first 30 chars of placeholder for readability
            snippet = placeholder[:30]
            return f'input[placeholder="{_css_escape(snippet)}"]'

        input_type = _attr(attrs, "type")
        if input_type and input_type.lower() not in ("text", "hidden"):
            return f"input[type={input_type.lower()}]"

        return "input"
