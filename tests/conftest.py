"""
pytest configuration — adds project root to sys.path so that
`config` and `scripts.*` are importable from test files.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
