"""Re-queue documents for background extraction after pipeline fixes."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
from config import DB_PATH
from db import get_connection


def main() -> None:
    conn = get_connection()
    try:
        stale = conn.execute(
            """
            SELECT rd.id, rd.return_id, rd.filename
            FROM return_documents rd
            WHERE rd.is_deleted = 0
            AND rd.doc_type IN ('unknown', 'W-2', '1099')
            AND NOT EXISTS (
                SELECT 1 FROM extraction_queue eq
                WHERE eq.doc_id = rd.id
                AND eq.status IN ('pending', 'processing')
            )
            """
        ).fetchall()

        now = datetime.now(timezone.utc).isoformat()
        print(f"Re-queuing {len(stale)} documents")

        for doc in stale:
            doc_id, return_id, filename = doc[0], doc[1], doc[2]
            conn.execute(
                """
                UPDATE extraction_queue SET status='pending', attempts=0
                WHERE doc_id=?
                """,
                (doc_id,),
            )
            existing = conn.execute(
                "SELECT id FROM extraction_queue WHERE doc_id=?",
                (doc_id,),
            ).fetchone()
            if not existing:
                conn.execute(
                    """
                    INSERT INTO extraction_queue
                        (doc_id, return_id, status, attempts, created_at)
                    VALUES (?,?, 'pending', 0, ?)
                    """,
                    (doc_id, return_id, now),
                )
            print(f"  Queued: {filename} (return {return_id}, doc {doc_id})")

        conn.commit()
    finally:
        conn.close()
    print("Done")


if __name__ == "__main__":
    main()
