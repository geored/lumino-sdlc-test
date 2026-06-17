#!/usr/bin/env python3
"""
LUMINO MCP Server - app.py entry point (required by deployment rubric).
Delegates to main.py logic with KUBERNETES_NAMESPACE set so the server
binds on port 8000 (streamable-http transport).
"""

import os
import sys

# Ensure streamable-http transport is selected (port 8000)
if not os.getenv("KUBERNETES_NAMESPACE") and not os.getenv("K8S_NAMESPACE"):
    os.environ.setdefault("KUBERNETES_NAMESPACE", "default")

# Add workspace to path and delegate to main
sys.path.insert(0, "/workspace")
from main import main  # noqa: E402

if __name__ == "__main__":
    main()
