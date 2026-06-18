"""Root conftest.py — adds src/ to sys.path so 'from models import ...' resolves."""

import os
import sys

# Add src/ to path so helpers can use 'from models import ...' style imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
