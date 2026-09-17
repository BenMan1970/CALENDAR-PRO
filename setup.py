#!/usr/bin/env python3
"""
BLUESTAR Calendar Pro — Setup Script
Generates the seed file needed for Streamlit Cloud deployment.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from pipeline_calendar import emit_seed, read_json

def main():
    print("BLUESTAR Calendar Pro - Setup")
    print("=" * 40)
    
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"[OK] Created data/ directory: {data_dir}")
    
    print("\nGenerating seed file...")
    seed_path = emit_seed(data_dir)
    
    if seed_path:
        print(f"[OK] Seed generated: {seed_path}")
        data = read_json(seed_path)
        if data and "events" in data:
            print(f"[OK] {len(data['events'])} events included")
        return 0
    else:
        print("[ERROR] Failed to generate seed")
        print("-> Check your internet connection")
        return 1

if __name__ == "__main__":
    sys.exit(main())
