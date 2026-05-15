"""
Unit tests for DOMSelectorBuilder.

Tests cover:
- extract_interactive_elements parsing of buttons, inputs, links, forms
- build_click_selector by text, by aria-label, fallback
- build_input_selector by name, placeholder, type, fallback
- _heuristic_plan integration: uses snapshot selectors, empty snapshot, navigate intent
"""

from harvest_acquire.browser.dom_selector_builder import DOMSelectorBuilder
from harvest_acquire.browser.agent_session import _heuristic_plan


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BUILDER = DOMSelectorBuilder()

HTML_FULL = """
<html>
<body>
  <form id="login-form" action="/login">
    <input id="email-field" name="email" type="email" placeholder="Enter your email">
    <input id="pass-field" name="password" type="password" placeholder="Password">
    <button id="submit-btn" type="submit">Login</button>
    <button id="cancel-btn" class="btn-secondary">Cancel</button>
  </form>
  <a href="/products" id="products-link">Products</a>
  <a href="/about">About Us</a>
  <button aria-label="Open menu">☰</button>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# extract_interactive_elements
# ---------------------------------------------------------------------------

class TestExtractInteractiveElements:

    def test_extract_buttons_from_html(self):
        elements = BUILDER.extract_interactive_elements(HTML_FULL)
        buttons = elements["buttons"]
        # Should find the submit button by id
        assert "#submit-btn" in buttons

    def test_extract_buttons_includes_cancel(self):
        elements = BUILDER.extract_interactive_elements(HTML_FULL)
        buttons = elements["buttons"]
        # cancel button has id
        assert "#cancel-btn" in buttons

    def test_extract_inputs_from_html(self):
        elements = BUILDER.extract_interactive_elements(HTML_FULL)
        inputs = elements["inputs"]
        # HTML_FULL email input has id="email-field" → highest-priority selector is #email-field.
        # Accept either the id-based or name-based selector.
        assert any("email" in sel for sel in inputs), f"No email selector in {inputs}"

    def test_extract_inputs_password(self):
        elements = BUILDER.extract_interactive_elements(HTML_FULL)
        inputs = elements["inputs"]
        assert any("pass" in sel or "password" in sel for sel in inputs), f"No password selector in {inputs}"

    def test_extract_inputs_name_selector_when_no_id(self):
        # When the input has no id, the selector must use name=
        html = '<input name="email" type="email" placeholder="Enter your email">'
        elements = BUILDER.extract_interactive_elements(html)
        assert 'input[name="email"]' in elements["inputs"]

    def test_extract_links_from_html(self):
        elements = BUILDER.extract_interactive_elements(HTML_FULL)
        links = elements["links"]
        # Products link has an id
        assert "#products-link" in links

    def test_extract_links_without_id(self):
        elements = BUILDER.extract_interactive_elements(HTML_FULL)
        links = elements["links"]
        # About link has no id — falls back to href selector
        assert any("/about" in sel for sel in links)

    def test_extract_forms_from_html(self):
        elements = BUILDER.extract_interactive_elements(HTML_FULL)
        forms = elements["forms"]
        assert "#login-form" in forms

    def test_extract_submit_selectors(self):
        elements = BUILDER.extract_interactive_elements(HTML_FULL)
        submit = elements["submit"]
        # submit-btn is type=submit → should appear in submit list
        assert "#submit-btn" in submit

    def test_submit_fallback_when_no_submit_button(self):
        html = "<html><body><button>Click me</button></body></html>"
        elements = BUILDER.extract_interactive_elements(html)
        # No type=submit → fallback defaults kick in
        assert len(elements["submit"]) > 0

    def test_empty_snapshot_returns_empty_lists(self):
        elements = BUILDER.extract_interactive_elements("")
        assert elements["buttons"] == []
        assert elements["inputs"] == []
        assert elements["links"] == []
        assert elements["forms"] == []

    def test_none_snapshot_returns_empty_lists(self):
        elements = BUILDER.extract_interactive_elements(None)  # type: ignore[arg-type]
        assert elements["buttons"] == []

    def test_hidden_inputs_excluded(self):
        html = '<input type="hidden" name="csrf" value="abc"><input name="email" type="email">'
        elements = BUILDER.extract_interactive_elements(html)
        inputs = elements["inputs"]
        # Only the email input, not the hidden one
        assert all("hidden" not in sel for sel in inputs)
        assert 'input[name="email"]' in inputs


# ---------------------------------------------------------------------------
# build_click_selector
# ---------------------------------------------------------------------------

class TestBuildClickSelector:

    def test_build_click_selector_by_text(self):
        sel = BUILDER.build_click_selector("Login", HTML_FULL)
        # Should return #submit-btn because that button's text is "Login"
        assert sel == "#submit-btn"

    def test_build_click_selector_case_insensitive(self):
        sel = BUILDER.build_click_selector("login", HTML_FULL)
        assert sel == "#submit-btn"

    def test_build_click_selector_by_aria_label(self):
        html = '<button aria-label="Close dialog">X</button>'
        sel = BUILDER.build_click_selector("Close dialog", html)
        assert 'aria-label="Close dialog"' in sel

    def test_build_click_selector_partial_text_match(self):
        html = '<button id="go-btn">Go to dashboard</button>'
        sel = BUILDER.build_click_selector("dashboard", html)
        assert sel == "#go-btn"

    def test_build_click_selector_link_by_text(self):
        html = '<a href="/signup" id="signup-link">Sign Up</a>'
        sel = BUILDER.build_click_selector("Sign Up", html)
        assert sel == "#signup-link"

    def test_build_click_selector_fallback_empty_snapshot(self):
        sel = BUILDER.build_click_selector("Submit", "")
        assert sel == "button, [role=button], a"

    def test_build_click_selector_fallback_no_match(self):
        html = "<html><body><p>No buttons here</p></body></html>"
        sel = BUILDER.build_click_selector("Submit", html)
        assert sel == "button, [role=button], a"

    def test_build_click_selector_empty_target(self):
        sel = BUILDER.build_click_selector("", HTML_FULL)
        assert sel == "button, [role=button], a"


# ---------------------------------------------------------------------------
# build_input_selector
# ---------------------------------------------------------------------------

class TestBuildInputSelector:

    def test_build_input_selector_by_name(self):
        sel = BUILDER.build_input_selector("email", HTML_FULL)
        assert sel == 'input[name="email"]'

    def test_build_input_selector_by_name_password(self):
        sel = BUILDER.build_input_selector("password", HTML_FULL)
        assert sel == 'input[name="password"]'

    def test_build_input_selector_by_placeholder(self):
        html = '<input type="text" placeholder="Enter email address">'
        sel = BUILDER.build_input_selector("email", html)
        assert "placeholder" in sel
        assert "email" in sel

    def test_build_input_selector_by_type(self):
        html = '<input type="search" placeholder="Search products">'
        sel = BUILDER.build_input_selector("search", html)
        assert sel == "input[type=search]"

    def test_build_input_selector_by_id(self):
        html = '<input id="username" type="text">'
        sel = BUILDER.build_input_selector("username", html)
        assert sel == "#username"

    def test_build_input_selector_fallback_empty_snapshot(self):
        sel = BUILDER.build_input_selector("email", "")
        assert sel == "input[type=text]:first-of-type"

    def test_build_input_selector_fallback_no_match(self):
        html = "<html><body><p>No inputs</p></body></html>"
        sel = BUILDER.build_input_selector("email", html)
        assert sel == "input[type=text]:first-of-type"

    def test_build_input_selector_empty_hint(self):
        sel = BUILDER.build_input_selector("", HTML_FULL)
        assert sel == "input[type=text]:first-of-type"


# ---------------------------------------------------------------------------
# build_navigate_selector
# ---------------------------------------------------------------------------

class TestBuildNavigateSelector:

    def test_build_navigate_selector_finds_link(self):
        sel = BUILDER.build_navigate_selector("/products", HTML_FULL)
        assert sel is not None
        assert "products" in sel or sel == "#products-link"

    def test_build_navigate_selector_no_match_returns_none(self):
        sel = BUILDER.build_navigate_selector("/nonexistent", HTML_FULL)
        assert sel is None

    def test_build_navigate_selector_empty_snapshot(self):
        sel = BUILDER.build_navigate_selector("/products", "")
        assert sel is None


# ---------------------------------------------------------------------------
# _heuristic_plan integration tests
# ---------------------------------------------------------------------------

class TestHeuristicPlan:

    def test_heuristic_plan_navigate_intent_url(self):
        plan = _heuristic_plan("go to https://example.com", "", "")
        assert len(plan) >= 1
        assert plan[0]["type"] == "navigate"
        assert plan[0]["value"] == "https://example.com"

    def test_heuristic_plan_navigate_intent_quoted(self):
        plan = _heuristic_plan("open 'example.com'", "", "")
        assert plan[0]["type"] == "navigate"
        assert "example.com" in plan[0]["value"]

    def test_heuristic_plan_uses_snapshot_for_click(self):
        snapshot = '<button id="real-submit-btn" type="submit">Submit Order</button>'
        plan = _heuristic_plan("click 'Submit Order'", "https://shop.com/cart", snapshot)
        assert len(plan) >= 1
        assert plan[0]["type"] == "click"
        # Must use the real selector from the DOM, not the hardcoded fallback
        assert plan[0]["value"] == "#real-submit-btn"
        assert plan[0]["value"] != "button[type=submit]"

    def test_heuristic_plan_click_no_quoted_uses_dom_submit(self):
        snapshot = '<button id="checkout-btn" type="submit">Checkout</button>'
        plan = _heuristic_plan("submit the form", "https://shop.com", snapshot)
        assert plan[0]["type"] == "click"
        # Should prefer DOM-derived submit selector, not hardcoded
        assert plan[0]["value"] != "button[type=submit]"
        assert "#checkout-btn" in plan[0]["value"]

    def test_heuristic_plan_empty_snapshot_no_crash(self):
        plan = _heuristic_plan("click the button", "https://example.com", "")
        assert isinstance(plan, list)
        assert len(plan) >= 1
        assert plan[0]["type"] == "click"

    def test_heuristic_plan_none_snapshot_no_crash(self):
        plan = _heuristic_plan("submit", "https://example.com", None)  # type: ignore[arg-type]
        assert isinstance(plan, list)
        assert len(plan) >= 1

    def test_heuristic_plan_type_intent_two_quotes(self):
        snapshot = '<input name="email" type="email">'
        plan = _heuristic_plan("fill 'email' with 'user@example.com'", "", snapshot)
        # Should produce click + type pair
        assert any(a["type"] == "type" for a in plan)
        assert any(a["type"] == "click" for a in plan)
        type_action = next(a for a in plan if a["type"] == "type")
        assert type_action["value"] == "user@example.com"

    def test_heuristic_plan_type_intent_uses_dom_input(self):
        snapshot = '<input name="email" type="email" placeholder="Your email">'
        plan = _heuristic_plan("fill 'email' with 'hello@test.com'", "", snapshot)
        click_action = next((a for a in plan if a["type"] == "click"), None)
        assert click_action is not None
        # Should use the real input selector from the DOM
        assert click_action["value"] == 'input[name="email"]'

    def test_heuristic_plan_extract_intent(self):
        plan = _heuristic_plan("extract the page title", "https://example.com", "")
        types = {a["type"] for a in plan}
        assert "evaluate" in types or "screenshot" in types

    def test_heuristic_plan_scroll_intent(self):
        plan = _heuristic_plan("scroll down to bottom", "", "")
        assert plan[0]["type"] == "evaluate"
        assert "scrollTo" in plan[0]["value"]

    def test_heuristic_plan_wait_intent(self):
        # Avoid "load" which triggers the navigate branch; use "pause" instead
        plan = _heuristic_plan("pause until element appears", "", "")
        assert any(a["type"] == "wait" for a in plan)

    def test_heuristic_plan_default_returns_screenshot(self):
        plan = _heuristic_plan("do something vague", "", "")
        assert any(a["type"] == "screenshot" for a in plan)
