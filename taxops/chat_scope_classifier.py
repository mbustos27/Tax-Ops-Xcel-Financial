"""
Binary scope gate for POST /ai/chat — TF-IDF + LogisticRegression trained on
static example questions (chat_scope_examples.tsv).

If the model confidently predicts *out_of_scope*, we skip the LLM tool-router call
and return the same guidance as a null tool response (vague / off-topic input).

No DB schema. If scikit-learn is missing or sklearn_chat_scope.pkl is absent,
the sklearn path is skipped; deterministic rules still block clearly off-topic SSO/login analytics text.
"""
from __future__ import annotations

import csv
import logging
import os
import pickle
import re
import threading

logger = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(_HERE, "sklearn_chat_scope.pkl")
EXAMPLES_PATH = os.path.join(_HERE, "chat_scope_examples.tsv")

# SSO / audit questions TaxOps KPIs cannot answer (high-precision; runs before sklearn).
_DET_STAFF_LOGIN_OR_AUTH_ANALYTICS = re.compile(
    r"\blogged\s+in\b|"
    r"\blogged\s+on\b|"
    r"\blog\s+in\b|"
    r"\blogins\b|"
    r"\blogin\b|"
    r"\bsigned?\s+in\b|"
    r"\bsign\s+in\b|"
    r"\bactive\s+users\b|"
    r"\buser\s+sessions?\b",
    re.IGNORECASE,
)

IN_SCOPE = "in_scope"
OUT_SCOPE = "out_of_scope"
# Block router only when we are this confident the question is off-topic.
BLOCK_MIN_PROBA = float(os.environ.get("CHAT_SCOPE_BLOCK_MIN_PROBA", "0.55"))

_model = None
_model_lock = threading.Lock()


def _normalize(text: str) -> str:
    return (text or "").strip().lower()


def _load_model():
    global _model
    if _model is not None:
        return
    if not os.path.exists(MODEL_PATH):
        logger.debug("Chat scope model not found — router LLM always allowed")
        return
    try:
        with open(MODEL_PATH, "rb") as fh:
            loaded = pickle.load(fh)
        with _model_lock:
            _model = loaded
        logger.info(
            "Chat scope classifier loaded (%s KB)",
            os.path.getsize(MODEL_PATH) // 1024,
        )
    except Exception as e:
        logger.error("Chat scope model load failed: %s", e)
        _model = None


def should_block_tool_router_llm(question: str) -> bool:
    """
    Return True to skip extract_json tool selection (vague / non-TaxOps question).

    False when: no model, predict in_scope, or low confidence on out_of_scope.
    """
    qn = _normalize(question)
    if len(qn) < 2:
        return True

    if _DET_STAFF_LOGIN_OR_AUTH_ANALYTICS.search(qn):
        logger.info(
            "Chat scope gate: blocked router (deterministic login/auth analytics wording)"
        )
        return True

    _load_model()
    if _model is None:
        return False

    try:
        proba = _model.predict_proba([qn])[0]
        classes = list(_model.classes_)
        best_i = int(proba.argmax())
        label = classes[best_i]
        conf = float(proba[best_i])
    except Exception as e:
        logger.warning("Chat scope predict failed: %s", e)
        return False

    if label == OUT_SCOPE and conf >= BLOCK_MIN_PROBA:
        logger.info(
            "Chat scope gate: blocked router (label=%s conf=%.2f)",
            label,
            conf,
        )
        return True
    return False


def train_and_save_from_tsv(
    tsv_path: str = EXAMPLES_PATH,
    out_path: str = MODEL_PATH,
) -> int:
    """Fit pipeline on examples TSV; write pickle. Returns row count."""
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
    except ImportError as e:
        raise RuntimeError("scikit-learn is required to train: pip install scikit-learn") from e

    rows: list[tuple[str, str]] = []
    with open(tsv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            lab = (row.get("label") or "").strip()
            q = (row.get("question") or "").strip()
            if not lab or not q:
                continue
            if lab not in (IN_SCOPE, OUT_SCOPE):
                raise ValueError(f"Bad label {lab!r} in {tsv_path}")
            rows.append((lab, _normalize(q)))

    if len(rows) < 8:
        raise ValueError(f"Need at least 8 labeled rows, got {len(rows)}")

    X = [q for _, q in rows]
    y = [lab for lab, _ in rows]

    model = Pipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    ngram_range=(1, 2),
                    min_df=1,
                    max_features=8000,
                    sublinear_tf=True,
                ),
            ),
            (
                "clf",
                LogisticRegression(
                    max_iter=2000,
                    C=1.0,
                    class_weight="balanced",
                    solver="lbfgs",
                ),
            ),
        ]
    )
    model.fit(X, y)

    with open(out_path, "wb") as fh:
        pickle.dump(model, fh)

    global _model
    with _model_lock:
        _model = model

    return len(rows)
