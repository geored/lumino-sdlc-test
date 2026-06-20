#!/usr/bin/env python3
"""
LUMINO MCP Server - Main Entry Point

This module serves as the entry point for the LUMINO MCP (Model Context Protocol) server.
It imports and runs the MCP server with proper configuration for both local and
Kubernetes environments.
"""

import importlib.util
import logging
import os
import sys
from pathlib import Path

# Add the src directory to the Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("lumino-mcp-main")


def main():
    """Main entry point for the LUMINO MCP Server."""
    logger.info("Starting LUMINO MCP Server...")

    try:
        # Import the MCP server module (with hyphen in filename)
        spec = importlib.util.spec_from_file_location(
            "server_mcp", src_path / "server-mcp.py"
        )
        server_mcp = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(server_mcp)

        # Get the MCP server instance
        mcp = server_mcp.mcp

        # Detect whether to use HTTP or stdio transport.
        # Containers (K8s, podman, docker) need HTTP; local/CLI tools use stdio.
        # Note: stdin.isatty() is False for both containers AND piped subprocesses
        # (e.g., Claude Code), so we can't rely on it alone.
        use_http = (
            os.getenv("KUBERNETES_NAMESPACE")
            or os.getenv("K8S_NAMESPACE")
            or os.getenv("CONTAINER_MODE")
            or os.path.exists("/run/.containerenv")
            or os.path.exists("/.dockerenv")
        )

        if use_http:
            logger.info("Running streamable HTTP server on 0.0.0.0:8000")
            mcp.run(transport="streamable-http")
        else:
            logger.info("Running in local environment - using stdio transport")
            mcp.run()

        logger.info("MCP server finished successfully")

    except Exception as e:
        logger.error(f"Failed to start LUMINO MCP Server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
