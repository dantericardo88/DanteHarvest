"""Tests for harvest_ui.api.agent_tool_registry."""
import pytest
from harvest_ui.api.agent_tool_registry import AgentToolRegistry, AgentSession, AgentTool


@pytest.fixture(autouse=True)
def isolated_registry():
    """Each test gets a clean registry (minus built-in tools)."""
    AgentToolRegistry.clear()
    yield
    AgentToolRegistry.clear()


def _make_tool(name: str = "test_tool", description: str = "A test tool") -> AgentTool:
    def handler(x: int = 0) -> dict:
        return {"result": x}

    return AgentTool(
        name=name,
        description=description,
        parameters={
            "type": "object",
            "properties": {"x": {"type": "integer"}},
            "required": [],
        },
        handler=handler,
    )


# ---------------------------------------------------------------------------
# AgentToolRegistry tests
# ---------------------------------------------------------------------------

class TestRegisterTool:
    def test_register_tool_adds_tool(self):
        tool = _make_tool("my_tool")
        AgentToolRegistry.register_tool(tool)
        assert AgentToolRegistry.get_tool("my_tool") is tool

    def test_register_decorator_adds_tool(self):
        @AgentToolRegistry.register(
            name="decorator_tool",
            description="Registered via decorator",
            parameters={"type": "object", "properties": {}, "required": []},
        )
        def my_func() -> dict:
            return {"ok": True}

        assert AgentToolRegistry.get_tool("decorator_tool") is not None


class TestListTools:
    def test_list_tools_returns_list_of_schema_dicts(self):
        AgentToolRegistry.register_tool(_make_tool("tool_a"))
        AgentToolRegistry.register_tool(_make_tool("tool_b"))
        tools = AgentToolRegistry.list_tools()
        assert isinstance(tools, list)
        assert len(tools) == 2
        names = {t["name"] for t in tools}
        assert "tool_a" in names
        assert "tool_b" in names


class TestCallTool:
    def test_call_tool_invokes_handler(self):
        tool = _make_tool("invoke_me")
        AgentToolRegistry.register_tool(tool)
        result = AgentToolRegistry.call_tool("invoke_me", {"x": 42})
        assert result == {"result": 42}

    def test_call_tool_raises_key_error_for_unknown(self):
        with pytest.raises(KeyError, match="not registered"):
            AgentToolRegistry.call_tool("does_not_exist", {})


class TestOpenAIFormat:
    def test_get_openai_format_has_type_function_entries(self):
        AgentToolRegistry.register_tool(_make_tool("oai_tool"))
        result = AgentToolRegistry.get_openai_format()
        assert len(result) == 1
        assert result[0]["type"] == "function"
        assert "function" in result[0]
        assert result[0]["function"]["name"] == "oai_tool"


class TestAnthropicFormat:
    def test_get_anthropic_format_has_input_schema_entries(self):
        AgentToolRegistry.register_tool(_make_tool("anth_tool"))
        result = AgentToolRegistry.get_anthropic_format()
        assert len(result) == 1
        assert "input_schema" in result[0]
        assert result[0]["name"] == "anth_tool"


# ---------------------------------------------------------------------------
# AgentSession tests
# ---------------------------------------------------------------------------

class TestAgentSessionRecordCall:
    def test_record_tool_call_adds_to_history(self):
        session = AgentSession("sess-001")
        session.record_tool_call("my_tool", {"x": 1}, {"result": 1})
        history = session.get_history()
        assert len(history) == 1
        assert history[0]["tool"] == "my_tool"
        assert history[0]["params"] == {"x": 1}

    def test_multiple_calls_accumulate(self):
        session = AgentSession()
        session.record_tool_call("tool_a", {}, "result_a")
        session.record_tool_call("tool_b", {}, "result_b")
        assert len(session.get_history()) == 2


class TestAgentSessionGetSummary:
    def test_get_summary_returns_session_id(self):
        session = AgentSession("sess-abc")
        summary = session.get_summary()
        assert summary["session_id"] == "sess-abc"

    def test_get_summary_returns_tool_calls_count(self):
        session = AgentSession()
        session.record_tool_call("t", {}, None)
        session.record_tool_call("t", {}, None)
        summary = session.get_summary()
        assert summary["tool_calls"] == 2

    def test_get_summary_includes_context_keys(self):
        session = AgentSession()
        session.set_context("user_id", "u123")
        summary = session.get_summary()
        assert "user_id" in summary["context_keys"]


class TestAgentSessionContext:
    def test_set_and_get_context(self):
        session = AgentSession()
        session.set_context("mode", "streaming")
        assert session.get_context("mode") == "streaming"

    def test_get_context_returns_default_for_missing(self):
        session = AgentSession()
        assert session.get_context("missing", "fallback") == "fallback"
