"""Agent tool registry — exposes DanteHarvest capabilities as callable agent tools."""
import json
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class AgentTool:
    name: str
    description: str
    parameters: Dict[str, Any]  # JSON Schema for parameters
    handler: Callable
    streaming: bool = False

    def get_schema(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
            "streaming": self.streaming,
        }


class AgentToolRegistry:
    """Registry of DanteHarvest tools available to AI agents."""

    _tools: Dict[str, AgentTool] = {}

    @classmethod
    def register(cls, name: str, description: str, parameters: dict, streaming: bool = False):
        """Decorator to register a function as an agent tool."""
        def decorator(func: Callable) -> Callable:
            cls._tools[name] = AgentTool(
                name=name, description=description,
                parameters=parameters, handler=func, streaming=streaming
            )
            return func
        return decorator

    @classmethod
    def register_tool(cls, tool: AgentTool) -> None:
        cls._tools[tool.name] = tool

    @classmethod
    def get_tool(cls, name: str) -> Optional[AgentTool]:
        return cls._tools.get(name)

    @classmethod
    def list_tools(cls) -> List[dict]:
        return [t.get_schema() for t in cls._tools.values()]

    @classmethod
    def call_tool(cls, name: str, parameters: dict) -> Any:
        """Call a registered tool by name with parameters."""
        tool = cls._tools.get(name)
        if not tool:
            raise KeyError(f"Tool '{name}' not registered. Available: {list(cls._tools)}")
        return tool.handler(**parameters)

    @classmethod
    def get_openai_format(cls) -> List[dict]:
        """Export tool definitions in OpenAI function-calling format."""
        result = []
        for tool in cls._tools.values():
            result.append({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                }
            })
        return result

    @classmethod
    def get_anthropic_format(cls) -> List[dict]:
        """Export tool definitions in Anthropic tool_use format."""
        result = []
        for tool in cls._tools.values():
            result.append({
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.parameters,
            })
        return result

    @classmethod
    def clear(cls) -> None:
        cls._tools.clear()


class AgentSession:
    """Context for an AI agent session — tracks tool calls, results, and state."""

    def __init__(self, session_id: str = None):
        import uuid
        self.session_id = session_id or str(uuid.uuid4())
        self._tool_calls: list = []
        self._context: dict = {}

    def record_tool_call(self, tool_name: str, params: dict, result: Any) -> None:
        self._tool_calls.append({
            "tool": tool_name,
            "params": params,
            "result": result,
            "ts": __import__('time').time(),
        })

    def set_context(self, key: str, value: Any) -> None:
        self._context[key] = value

    def get_context(self, key: str, default: Any = None) -> Any:
        return self._context.get(key, default)

    def get_history(self) -> list:
        return list(self._tool_calls)

    def get_summary(self) -> dict:
        return {
            "session_id": self.session_id,
            "tool_calls": len(self._tool_calls),
            "context_keys": list(self._context.keys()),
        }


# ---------------------------------------------------------------------------
# Built-in DanteHarvest tools registered at import time
# ---------------------------------------------------------------------------

@AgentToolRegistry.register(
    name="harvest_url",
    description="Crawl and extract content from a URL",
    parameters={
        "type": "object",
        "properties": {
            "url": {"type": "string", "description": "URL to harvest"},
            "depth": {"type": "integer", "default": 1, "description": "Crawl depth"},
        },
        "required": ["url"]
    }
)
def _harvest_url_tool(url: str, depth: int = 1) -> dict:
    return {"url": url, "depth": depth, "status": "queued", "tool": "harvest_url"}


@AgentToolRegistry.register(
    name="search_artifacts",
    description="Search harvested artifacts by query",
    parameters={
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "limit": {"type": "integer", "default": 10},
        },
        "required": ["query"]
    }
)
def _search_artifacts_tool(query: str, limit: int = 10) -> dict:
    return {"query": query, "limit": limit, "results": [], "tool": "search_artifacts"}
