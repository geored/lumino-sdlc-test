"""
LUMINO MCP Server - Middleware Module

Logging setup, tool execution logging decorator, and enhanced tool decorator
for the MCP server. Extracted from server-mcp.py (Fixes #52).
"""

import asyncio
import functools
import logging

from mcp.server.fastmcp import FastMCP

# Configure logging with custom format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("lumino-mcp")

# Suppress the default MCP server logging to replace with our enhanced version
mcp_server_logger = logging.getLogger("mcp.server.lowlevel.server")
mcp_server_logger.setLevel(logging.WARNING)  # Only show warnings and errors

# Initialize FastMCP server with streaming support
mcp = FastMCP(name="lumino-mcp-server", stateless_http=False)


# Create a decorator to add tool execution logging
def log_tool_execution(func):
    """Decorator to log tool execution with tool name."""

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        tool_name = func.__name__
        logger.info(f"Executing tool: {tool_name}")
        try:
            result = await func(*args, **kwargs)
            logger.info(f"Tool completed: {tool_name}")
            return result
        except Exception as e:
            logger.error(f"Tool failed: {tool_name} - Error: {str(e)}")
            raise

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        tool_name = func.__name__
        logger.info(f"Executing tool: {tool_name}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Tool completed: {tool_name}")
            return result
        except Exception as e:
            logger.error(f"Tool failed: {tool_name} - Error: {str(e)}")
            raise

    # Return appropriate wrapper based on whether function is async
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


# Override the mcp.tool decorator to include our logging
original_tool_decorator = mcp.tool


def enhanced_tool_decorator(*args, **kwargs):
    """Enhanced tool decorator that adds logging."""

    def decorator(func):
        # First apply our logging decorator
        logged_func = log_tool_execution(func)
        # Then apply the original MCP tool decorator
        return original_tool_decorator(*args, **kwargs)(logged_func)

    # Handle both @mcp.tool and @mcp.tool() usage
    if len(args) == 1 and callable(args[0]) and not kwargs:
        # Direct decoration: @mcp.tool
        func = args[0]
        logged_func = log_tool_execution(func)
        return original_tool_decorator(logged_func)
    else:
        # Parameterized decoration: @mcp.tool()
        return decorator


# Replace the tool decorator
mcp.tool = enhanced_tool_decorator
