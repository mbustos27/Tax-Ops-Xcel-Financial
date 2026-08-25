#!/usr/bin/env python3
"""
Quick connectivity test for the Ollama server.
Run from the project root: python test_ollama.py
"""
import os
import requests

base_url = os.environ.get("OLLAMA_BASE_URL", "http://192.168.1.173:11434")

print(f"Testing Ollama at: {base_url}")

# Test 1: tags endpoint
try:
    r = requests.get(f"{base_url}/api/tags", timeout=5)
    models = [m["name"] for m in r.json().get("models", [])]
    print(f"✓ Connected — models available: {models}")
except Exception as e:
    print(f"✗ Connection failed: {e}")
    exit(1)

# Test 2: simple generation
print("Testing generation (llama3.2)...")
try:
    r = requests.post(f"{base_url}/api/generate", json={
        "model": "llama3.2",
        "prompt": "Reply with the single word: working",
        "stream": False
    }, timeout=60)
    response = r.json().get("response", "").strip()
    print(f"✓ Generation response: {response}")
except Exception as e:
    print(f"✗ Generation failed: {e}")

print("Done.")
