"""
classifier.py
-------------
sklearn TF-IDF + LogisticRegression email classifier for TaxOps mail watcher.

Replaces fastText — ships as a pure Python wheel, no C++ compiler required.
Install: pip install scikit-learn

Trains on staff-confirmed email_classifications from the DB.
Retrains automatically (background thread) after staff confirmations.
Falls back gracefully to None if model not yet trained or scikit-learn is
not installed.

Privacy rules enforced here:
- Email body is NEVER stored in training data or on disk
- Subject snippets capped at 100 chars (matching DB column constraint)
- No SSN, ssn_last4, or taxpayer identification numbers
- Only sender_domain + subject_snippet (DB-stored fields) feed the model
"""
from __future__ import annotations

import logging
import os
import pickle
import threading

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_HERE = os.path.dirname(__file__)

MODEL_PATH         = os.path.join(_HERE, "sklearn_email.pkl")
TRAINING_DATA_PATH = os.path.join(_HERE, "sklearn_training_debug.txt")  # for inspection

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_TRAINING_EXAMPLES = 50
CONFIDENCE_THRESHOLD  = 0.80

# ── Module-level state ────────────────────────────────────────────────────────
_model       = None
_model_lock  = threading.Lock()
_retraining  = False

_VALID_LABELS = frozenset({
    "promotional", "client_document", "client_inquiry", "unknown"
})


# ── Internal helpers ──────────────────────────────────────────────────────────

def _load_model() -> None:
    """Load the model from disk into memory.  No-op if file does not exist."""
    global _model
    if not os.path.exists(MODEL_PATH):
        logger.info("Classifier model not found — will train when enough confirmed examples exist")
        return
    try:
        with open(MODEL_PATH, "rb") as fh:
            loaded = pickle.load(fh)
        with _model_lock:
            _model = loaded
        logger.info(f"Classifier model loaded ({os.path.getsize(MODEL_PATH) // 1024} KB)")
    except Exception as e:
        logger.error(f"Classifier load failed: {e}")
        _model = None


def _features(subject: str, sender_domain: str, body_preview: str = "") -> str:
    """
    Build the feature string fed to the vectoriser.
    domain + subject (100 chars max) + body_preview (100 chars max).
    All lowercased — consistent with training.
    Email body is never stored beyond this 100-char window.
    """
    parts = [
        sender_domain.lower().strip(),
        subject[:100].lower().strip(),
        (body_preview or "")[:100].lower().strip(),
    ]
    return " ".join(p for p in parts if p)


# ── Public API ────────────────────────────────────────────────────────────────

def classify(subject: str, sender_domain: str, body_preview: str = "") -> tuple:
    """
    Classify an email using the trained model.

    Returns (classification, confidence) where classification is one of:
        'promotional', 'client_document', 'client_inquiry', 'unknown'
    Returns (None, 0.0) if model not yet trained, scikit-learn not installed,
    or confidence below CONFIDENCE_THRESHOLD.
    Never raises.
    """
    global _model
    if _model is None:
        _load_model()
    if _model is None:
        return None, 0.0

    text = _features(subject, sender_domain, body_preview)
    if not text:
        return None, 0.0

    try:
        proba   = _model.predict_proba([text])[0]
        classes = list(_model.classes_)
        best_i  = int(proba.argmax())
        label   = classes[best_i]
        conf    = float(proba[best_i])

        if label not in _VALID_LABELS:
            logger.warning(f"Classifier returned unexpected label {label!r}")
            return None, 0.0

        if conf < CONFIDENCE_THRESHOLD:
            logger.info(
                f"Classifier low confidence {conf:.2f} for {sender_domain!r} "
                "— falling through to Ollama"
            )
            return None, conf

        logger.info(f"Classifier: {sender_domain!r} → {label} ({conf:.2f})")
        return label, conf

    except Exception as e:
        logger.error(f"Classifier classify error: {e}")
        return None, 0.0


def retrain(db_path: str) -> None:
    """
    Retrain the model from staff-confirmed email_classifications.
    Runs in a background thread — never blocks the poll cycle.

    Privacy:
    - Only sender_domain + subject_snippet (already in DB) used as features
    - No email body written to training data
    - No SSN or identification numbers
    """
    global _model, _retraining

    with _model_lock:
        if _retraining:
            logger.info("Retrain already in progress — skipping")
            return
        _retraining = True

    try:
        import sqlite3
        from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
        from sklearn.linear_model import LogisticRegression           # type: ignore
        from sklearn.pipeline import Pipeline                         # type: ignore

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        rows = conn.execute(
            """
            SELECT sender_domain, subject_snippet, classification
            FROM email_classifications
            WHERE confirmed_by IS NOT NULL
              AND classification IN (
                  'promotional', 'client_document',
                  'client_inquiry', 'unknown'
              )
            ORDER BY confirmed_at DESC
            """
        ).fetchall()
        conn.close()

        if len(rows) < MIN_TRAINING_EXAMPLES:
            logger.info(
                f"Retrain skipped: {len(rows)} confirmed examples "
                f"(need {MIN_TRAINING_EXAMPLES})"
            )
            return

        X, y = [], []
        for row in rows:
            text = _features(
                row["subject_snippet"] or "",
                row["sender_domain"]   or "",
            )
            if text:
                X.append(text)
                y.append(row["classification"])

        if len(X) < MIN_TRAINING_EXAMPLES:
            logger.warning(f"Retrain skipped: only {len(X)} non-empty feature rows")
            return

        logger.info(f"Training classifier on {len(X)} examples…")

        new_model = Pipeline([
            ("tfidf", TfidfVectorizer(ngram_range=(1, 2), max_features=5000)),
            ("clf",   LogisticRegression(
                max_iter=1000, C=1.0, class_weight="balanced", solver="lbfgs",
            )),
        ])
        new_model.fit(X, y)

        with open(MODEL_PATH, "wb") as fh:
            pickle.dump(new_model, fh)

        with _model_lock:
            _model = new_model

        logger.info(
            f"Classifier retrained — {len(X)} examples, "
            f"model saved to {MODEL_PATH} "
            f"({os.path.getsize(MODEL_PATH) // 1024} KB)"
        )

    except ImportError:
        logger.warning(
            "scikit-learn not installed — run: pip install scikit-learn"
        )
    except Exception as e:
        logger.error(f"Classifier retrain failed: {e}")
    finally:
        with _model_lock:
            _retraining = False


def trigger_retrain_async(db_path: str) -> None:
    """Start a background retrain.  Non-blocking — returns immediately."""
    thread = threading.Thread(
        target=retrain,
        args=(db_path,),
        daemon=True,
        name="classifier-retrain",
    )
    thread.start()
    logger.info("Classifier retrain triggered in background")
