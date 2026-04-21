from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path

from config import ERROR_DIR, INCOMING_DIR, PROCESSED_DIR
from db import get_connection, init_db
from importer import process_csv
from utils import hash_file, now


def ensure_directories() -> None:
    Path(INCOMING_DIR).mkdir(parents=True, exist_ok=True)
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
    Path(ERROR_DIR).mkdir(parents=True, exist_ok=True)


def main() -> None:
    ensure_directories()
    conn = get_connection()
    init_db(conn)

    incoming_files = sorted(Path(INCOMING_DIR).glob("*.csv"))
    if not incoming_files:
        print("No CSV files found in data/incoming.")
        print("taxops v2 ready")
        conn.close()
        return

    for csv_file in incoming_files:
        process_one_file(conn, csv_file)

    conn.close()
    print("taxops v2 ready")


def process_one_file(conn: sqlite3.Connection, csv_path: Path) -> None:
    file_hash = hash_file(str(csv_path))
    duplicate = conn.execute(
        "SELECT id FROM import_batches WHERE file_hash = ? LIMIT 1",
        (file_hash,),
    ).fetchone()

    if duplicate:
        print(f"Skipping duplicate file: {csv_path.name}")
        return

    batch_id = create_batch(conn, csv_path.name, file_hash)

    try:
        conn.execute("BEGIN")
        stats = process_csv(conn, str(csv_path), batch_id, csv_path.name)
        conn.execute(
            """
            UPDATE import_batches SET
              status = ?,
              row_count = ?,
              success_count = ?,
              error_count = ?,
              review_count = ?,
              created_clients = ?,
              updated_clients = ?,
              created_returns = ?,
              updated_returns = ?,
              events_created = ?,
              notes_created = ?
            WHERE id = ?
            """,
            (
                "SUCCESS",
                stats.row_count,
                stats.success_count,
                stats.error_count,
                stats.review_count,
                stats.created_clients,
                stats.updated_clients,
                stats.created_returns,
                stats.updated_returns,
                stats.events_created,
                stats.notes_created,
                batch_id,
            ),
        )
        conn.commit()
        move_file(csv_path, Path(PROCESSED_DIR) / csv_path.name)
        print(
            f"{csv_path.name}: SUCCESS rows={stats.row_count} ok={stats.success_count} "
            f"errors={stats.error_count} review={stats.review_count} "
            f"clients(+{stats.created_clients}/~{stats.updated_clients}) "
            f"returns(+{stats.created_returns}/~{stats.updated_returns}) "
            f"events={stats.events_created} notes={stats.notes_created}"
        )
    except Exception as exc:
        conn.rollback()
        conn.execute(
            """
            UPDATE import_batches
            SET status = ?, error_count = COALESCE(error_count, 0) + 1
            WHERE id = ?
            """,
            ("FAILED", batch_id),
        )
        conn.commit()
        move_file(csv_path, Path(ERROR_DIR) / csv_path.name)
        print(f"{csv_path.name}: FAILED ({exc})")


def create_batch(conn: sqlite3.Connection, filename: str, file_hash: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO import_batches (filename, file_hash, imported_at, status)
        VALUES (?, ?, ?, ?)
        """,
        (filename, file_hash, now(), "PROCESSING"),
    )
    conn.commit()
    return int(cur.lastrowid)


def move_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination = destination.with_name(f"{destination.stem}_{int(os.times().elapsed)}{destination.suffix}")
    shutil.move(str(source), str(destination))


if __name__ == "__main__":
    main()
