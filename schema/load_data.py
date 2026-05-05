"""
load_data.py
DSCI 551 Group Project - Spring 2026

Loads IEEE and JMLR research paper datasets into the research_papers
PostgreSQL database. Run this script once after schema.sql to populate
all tables.

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

# ─── Database connection settings ────────────────────────────────────────────
# Three sources, in priority order:
#   1. DATABASE_URL  (Railway, Heroku-style providers)
#   2. PG* env vars  (matches what app.py and ui/streamlit_app.py use)
#   3. localhost defaults
DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    # psycopg2 understands DATABASE_URL via dsn=
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
# Resolve CSVs relative to this script's location (schema/), not the current
# working directory. This way `python schema/load_data.py` works from anywhere.
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)

# Look in two places: project_root/data/ first, then alongside this script.
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


def get_or_create_author(cursor, name):
    """Insert author if not exists, return author_id."""
    cursor.execute(
        "INSERT INTO authors (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
        (name,),
    )
    cursor.execute("SELECT author_id FROM authors WHERE name = %s", (name,))
    return cursor.fetchone()[0]


def insert_paper(cursor, title, abstract, year, pages, link, code_link, source):
    """Insert a paper and return its paper_id."""
    cursor.execute(
        """
        INSERT INTO papers (title, abstract, year, pages, link, code_link, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING paper_id
        """,
        (title, abstract, year, pages, link, code_link, source),
    )
    return cursor.fetchone()[0]


def load_ieee(cursor, filepath):
    """Load IEEE dataset into the database."""
    print(f"\nLoading IEEE data from {filepath}...")
    df = pd.read_csv(filepath)
    count = 0
    for _, row in df.iterrows():
        try:
            title     = str(row.get("title", "")).strip()
            abstract  = str(row.get("abstract", "")).strip() or None
            year      = int(row["year"]) if pd.notna(row.get("year")) else None
            pages     = int(row["pages"]) if pd.notna(row.get("pages")) else None
            link      = str(row.get("link", "")).strip() or None
            code_link = str(row.get("code", "")).strip() or None
            authors   = parse_authors(row.get("authors"))
            if not title:
                continue
            paper_id = insert_paper(
                cursor, title, abstract, year, pages, link, code_link, "IEEE"
            )
            for author_name in authors:
                author_id = get_or_create_author(cursor, author_name)
                cursor.execute(
                    "INSERT INTO paper_authors (paper_id, author_id) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (paper_id, author_id),
                )
            count += 1
        except Exception as e:
            print(f"  Skipping row due to error: {e}")
            continue
    print(f"  Loaded {count} IEEE papers.")


def load_jmlr(cursor, filepath):
    """Load JMLR dataset into the database."""
    print(f"\nLoading JMLR data from {filepath}...")
    df = pd.read_csv(filepath)
    count = 0
    for _, row in df.iterrows():
        try:
            title     = str(row.get("title", "")).strip()
            year      = int(row["year"]) if pd.notna(row.get("year")) else None
            pages     = int(row["pages"]) if pd.notna(row.get("pages")) else None
            link      = str(row.get("link", "")).strip() or None
            code_link = str(row.get("code", "")).strip() or None
            authors   = parse_authors(row.get("authors"))
            if not title:
                continue
            paper_id = insert_paper(
                cursor, title, None, year, pages, link, code_link, "JMLR"
            )
            for author_name in authors:
                author_id = get_or_create_author(cursor, author_name)
                cursor.execute(
                    "INSERT INTO paper_authors (paper_id, author_id) "
                    "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (paper_id, author_id),
                )
            count += 1
        except Exception as e:
            print(f"  Skipping row due to error: {e}")
            continue
    print(f"  Loaded {count} JMLR papers.")


def main():
    print("Connecting to PostgreSQL...")
    if DATABASE_URL:
        # Don't print the full URL (contains password); just say where we're going
        print(f"  Using DATABASE_URL (host: {DATABASE_URL.split('@')[-1].split('/')[0]})")
    else:
        print(f"  Using host: {DB_KWARGS['host']}, db: {DB_KWARGS['dbname']}")

    conn = psycopg2.connect(**DB_KWARGS)
    conn.autocommit = False
    cursor = conn.cursor()
    try:
        load_ieee(cursor, IEEE_CSV)
        load_jmlr(cursor, JMLR_CSV)
        conn.commit()
        print("\nAll data loaded successfully.")
        cursor.execute("SELECT COUNT(*) FROM papers")
        print(f"  Total papers:  {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM authors")
        print(f"  Total authors: {cursor.fetchone()[0]}")
        cursor.execute("SELECT source, COUNT(*) FROM papers GROUP BY source")
        for row in cursor.fetchall():
            print(f"  {row[0]} papers: {row[1]}")
    except Exception as e:
        conn.rollback()
        print(f"\nError loading data: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()
        conn.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
