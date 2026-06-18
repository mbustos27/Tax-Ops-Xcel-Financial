## Parent epic
Security hardening epic

## Problem
No `MAX_CONTENT_LENGTH` on Flask. Large uploads can exhaust disk or choke Waitress threads. Add validation on document upload routes.

## Fix sketch
```python
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
```
```python
from werkzeug.exceptions import RequestEntityTooLarge

@app.errorhandler(RequestEntityTooLarge)
def handle_large_file(_e):
    return jsonify({'error': 'File too large. Maximum size is 50MB.'}), 413
```

Optional: python-magic / content sniff — on Windows verify libmagic availability; document fallback to extension-only if needed.

## Definition of done
- MAX_CONTENT_LENGTH active
- 413 JSON error verified
- python -m pytest tests/ -v passes
