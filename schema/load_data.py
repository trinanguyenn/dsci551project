"""
load_data.py
DSCI 551 Group Project - Spring 2026

Loads IEEE and JMLR research paper datasets into the research_papers
PostgreSQL database. Run this script once after schema.sql to populate
all tables.

Performance: uses psycopg2.extras.execute_values() so all inserts go
out as batched statements instead of one round-trip per row. Loading
~3,400 papers + ~7,600 authors + ~11,000 author-paper links over a
remote connection finishes in roughly 30 seconds rather than 15
minutes.

Usage (local):
    python schema/load_data.py

Usage (Railway / cloud):
    Set DATABASE_URL env var (Railway provides this automatically when a
    Postgres service is attached) and run the same command. Falls back
    to PG* env vars and finally to localhost defaults.

Requirements:
    pip install psycopg2-binary pandas
"""

import ast
import os
import re
import sys

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ─── Database connection settings ────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    DB_KWARGS = {"dsn": DATABASE_URL}
else:
    DB_KWARGS = {
        "host":     os.environ.get("PGHOST", "localhost"),
        "port":     int(os.environ.get("PGPORT", 5432)),
        "dbname":   os.environ.get("PGDATABASE", "research_papers"),
        "user":     os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
    }

# ─── File paths ───────────────────────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)


def find_csv(filename):
    candidates = [
        os.path.join(PROJECT_ROOT, "data", filename),
        os.path.join(HERE, filename),
        os.path.join(PROJECT_ROOT, filename),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        f"Could not find {filename}. Looked in: {candidates}"
    )


IEEE_CSV = find_csv("IEEE_Research_Data.csv")
JMLR_CSV = find_csv("Papers_MLResearch_Data.csv")


# ─── Helpers ────────────────────────────────────────────────────────────────
def parse_authors(author_str):
    """Parse author string like \"['Author One', 'Author Two']\" into a list."""
    if not author_str or pd.isna(author_str):
        return []
    try:
        result = ast.literal_eval(str(author_str))
        if isinstance(result, list):
            return [a.strip() for a in result if a.strip()]
    except Exception:
        pass
    cleaned = re.sub(r"[\[\]']", "", str(author_str))
    return [a.strip() for a in cleaned.split(",") if a.strip()]


def safe_int(val):
    if pd.isna(val):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_str(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None


# ─── Phase 1: collect all rows in memory ────────────────────────────────────
def collect_papers_from_ieee(filepath):
    """Read IEEE CSV, return list of (paper_tuple, authors_list)."""
    print(f"  Reading IEEE data from {filepath}...")
    df = pd.read_csv(filepath)
    out = []
    for _, row in df.iterrows():
        title = safe_str(row.get("title"))
        if not title:
            continue
        paper_tuple = (
            title,
            safe_str(row.get("abstract")),
            safe_int(row.get("year")),
            safe_int(row.get("pages")),
            safe_str(row.get("link")),
            safe_str(row.get("code")),
            "IEEE",
        )
        authors = parse_authors(row.get("authors"))
        out.append((paper_tuple, authors))
    print(f"    Found {len(out)} valid IEEE rows.")
    return out


def collect_papers_from_jmlr(filepath):
    """Read JMLR CSV, return list of (paper_tuple, authors_list)."""
    print(f"  Reading JMLR data from {filepath}...")
    df = pd.read_csv(filepath)
    out = []
    for _, row in df.iterrows():
        title = safe_str(row.get("title"))
        if not title:
            continue
        paper_tuple = (
            title,
            None,  # JMLR CSV has no abstract
            safe_int(row.get("year")),
            safe_int(row.get("pages")),
            safe_str(row.get("link")),
            safe_str(row.get("code")),
            "JMLR",
        )
        authors = parse_authors(row.get("authors"))
        out.append((paper_tuple, authors))
    print(f"    Found {len(out)} valid JMLR rows.")
    return out


# ─── Phase 2: bulk insert ────────────────────────────────────────────────────
def bulk_insert(conn, all_records):
    """Insert papers, authors, and paper_authors using batched statements."""
    cur = conn.cursor()

    # 1. Bulk insert papers, get back paper_ids in input order
    print("  Inserting papers...")
    paper_rows = [rec[0] for rec in all_records]
    paper_ids = execute_values(
        cur,
        """
        INSERT INTO papers (title, abstract, year, pages, link, code_link, source)
        VALUES %s
        RETURNING paper_id
        """,
        paper_rows,
        fetch=True,
    )
    paper_ids = [pid[0] for pid in paper_ids]
    print(f"    Inserted {len(paper_ids)} papers.")

    # 2. Collect every unique author name, bulk insert, then map name -> author_id
    print("  Collecting and inserting authors...")
    all_author_names = set()
    for _, authors in all_records:
        all_author_names.update(authors)

    if all_author_names:
        execute_values(
            cur,
            "INSERT INTO authors (name) VALUES %s ON CONFLICT (name) DO NOTHING",
            [(n,) for n in all_author_names],
        )

    cur.execute(
        "SELECT name, author_id FROM authors WHERE name = ANY(%s)",
        (list(all_author_names),),
    )
    name_to_id = dict(cur.fetchall())
    print(f"    Resolved {len(name_to_id)} unique author IDs.")

    # 3. Build all paper_authors rows and bulk insert
    print("  Inserting paper-author links...")
    pa_rows = []
    for paper_id, (_, authors) in zip(paper_ids, all_records):
        for author_name in authors:
            author_id = name_to_id.get(author_name)
            if author_id is not None:
                pa_rows.append((paper_id, author_id))

    if pa_rows:
        execute_values(
            cur,
            """
            INSERT INTO paper_authors (paper_id, author_id) VALUES %s
            ON CONFLICT DO NOTHING
            """,
            pa_rows,
        )
    print(f"    Inserted {len(pa_rows)} paper-author links.")

    cur.close()


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    print("Connecting to PostgreSQL...")
    if DATABASE_URL:
        host_part = DATABASE_URL.split("@")[-1].split("/")[0]
        print(f"  Using DATABASE_URL (host: {host_part})")
    else:
        print(f"  Using host: {DB_KWARGS['host']}, db: {DB_KWARGS['dbname']}")

    print("\nReading CSV files...")
    ieee_records = collect_papers_from_ieee(IEEE_CSV)
    jmlr_records = collect_papers_from_jmlr(JMLR_CSV)
    all_records = ieee_records + jmlr_records
    print(f"\nTotal records to load: {len(all_records)}")

    conn = psycopg2.connect(**DB_KWARGS)
    conn.autocommit = False
    try:
        print("\nLoading data...")
        bulk_insert(conn, all_records)
        conn.commit()
        print("\nAll data loaded successfully.")

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM papers")
        print(f"  Total papers:  {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM authors")
        print(f"  Total authors: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM paper_authors")
        print(f"  Total paper-author links: {cur.fetchone()[0]}")
        cur.execute("SELECT source, COUNT(*) FROM papers GROUP BY source ORDER BY source")
        for row in cur.fetchall():
            print(f"  {row[0]} papers: {row[1]}")
        cur.close()
    except Exception as e:
        conn.rollback()
        print(f"\nError loading data: {e}", file=sys.stderr)
        raise
    finally:
        conn.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
