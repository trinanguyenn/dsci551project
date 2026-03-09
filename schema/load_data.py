"""
load_data.py
DSCI 551 Group Project - Spring 2026

Loads IEEE and JMLR research paper datasets into the research_papers PostgreSQL database.
Run this script once after schema.sql to populate all tables.

Usage:
    python load_data.py

Requirements:
    pip install psycopg2-binary pandas
"""

import psycopg2
import pandas as pd
import ast
import os
import re

# ─── Database connection settings ────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "research_papers",
    "user":     "postgres",
    "password": "postgres"   # Change this to your PostgreSQL password if different
}

# ─── File paths ───────────────────────────────────────────────────────────────
# Place both CSV files in the same folder as this script, or update these paths
IEEE_CSV  = "IEEE_Research_Data.csv"
JMLR_CSV  = "Papers_MLResearch_Data.csv"


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
    # Fallback: strip brackets and split by comma
    cleaned = re.sub(r"[\[\]']", "", str(author_str))
    return [a.strip() for a in cleaned.split(",") if a.strip()]


def get_or_create_author(cursor, name):
    """Insert author if not exists, return author_id."""
    cursor.execute(
        "INSERT INTO authors (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
        (name,)
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
        (title, abstract, year, pages, link, code_link, source)
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
            abstract  = str(row.get("abstract", "")).strip()
            year      = int(row["year"]) if pd.notna(row.get("year")) else None
            link      = str(row.get("link", "")).strip() or None
            authors   = parse_authors(row.get("authors"))

            if not title:
                continue

            paper_id = insert_paper(cursor, title, abstract, year, None, link, None, "IEEE")

            for author_name in authors:
                author_id = get_or_create_author(cursor, author_name)
                cursor.execute(
                    "INSERT INTO paper_authors (paper_id, author_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (paper_id, author_id)
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

            paper_id = insert_paper(cursor, title, None, year, pages, link, code_link, "JMLR")

            for author_name in authors:
                author_id = get_or_create_author(cursor, author_name)
                cursor.execute(
                    "INSERT INTO paper_authors (paper_id, author_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (paper_id, author_id)
                )

            count += 1

        except Exception as e:
            print(f"  Skipping row due to error: {e}")
            continue

    print(f"  Loaded {count} JMLR papers.")


def main():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        load_ieee(cursor, IEEE_CSV)
        load_jmlr(cursor, JMLR_CSV)
        conn.commit()
        print("\nAll data loaded successfully.")

        # Quick summary
        cursor.execute("SELECT COUNT(*) FROM papers")
        print(f"  Total papers:  {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM authors")
        print(f"  Total authors: {cursor.fetchone()[0]}")
        cursor.execute("SELECT source, COUNT(*) FROM papers GROUP BY source")
        for row in cursor.fetchall():
            print(f"  {row[0]} papers: {row[1]}")

    except Exception as e:
        conn.rollback()
        print(f"\nError loading data: {e}")
        raise

    finally:
        cursor.close()
        conn.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
