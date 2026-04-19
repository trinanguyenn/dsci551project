"""
demo_query_planning.py
DSCI 551 Group Project - Spring 2026
Focus Area: Query Planning and Execution (Trina Nguyen)

This script demonstrates how PostgreSQL plans and executes queries, using
EXPLAIN ANALYZE to compare sequential scans vs index scans and to observe
which join strategy the planner picks.

It runs two demos:
  1. Sequential Scan vs Index Scan - same query against papers.title, run
     first with no index (forcing Seq Scan) and then with an index.
  2. Join planning - a 3-table JOIN, first with no join-key indexes and
     then with them, so the planner's output visibly changes.

Usage:
    python demo_query_planning.py

Requirements:
    pip install psycopg2-binary

Connection settings are read from environment variables (PGHOST, PGPORT,
PGDATABASE, PGUSER, PGPASSWORD), falling back to localhost defaults.
"""

import os

import psycopg2

# ─── Database connection settings ────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("PGHOST", "localhost"),
    "port":     int(os.environ.get("PGPORT", 5432)),
    "dbname":   os.environ.get("PGDATABASE", "research_papers"),
    "user":     os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

DIVIDER = "=" * 70


def get_conn():
    """Create and return a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 1: Sequential Scan vs Index Scan
#
# Same SELECT against papers.title, run twice: once without any index
# (forcing a Seq Scan) and once with an index on title.
#
# We use papers.title because authors.name has a UNIQUE constraint which
# auto-creates an index, so we cannot actually force a Seq Scan on authors.
# papers.title has no unique constraint and no implicit index, so dropping
# our explicit index really does fall back to a Seq Scan.
# ─────────────────────────────────────────────────────────────────────────────
def run_demo():
    print(f"\n{DIVIDER}")
    print("DEMO 1: Sequential Scan vs Index Scan")
    print(DIVIDER)
    print("""
What this shows:
  The SAME query against papers.title, run twice:
    - first with no index on papers.title  -> Seq Scan over every heap page
    - then with idx_papers_title created   -> Index Scan straight to matches
  The planner chooses the strategy based on cost, using table statistics.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # A title search is a natural application query for a paper search engine.
    # Pattern match so we touch multiple rows and the plan difference is visible.
    query = "SELECT paper_id, year, title FROM papers WHERE title ILIKE '%learning%'"

    # -- Part 1: no title index -> Seq Scan --
    print("  Part 1 - WITHOUT idx_papers_title (Seq Scan expected)")
    print("  " + "-" * 66)
    cur.execute("DROP INDEX IF EXISTS idx_papers_title")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
    for (line,) in cur.fetchall():
        print(f"    {line}")

    # -- Part 2: create a trigram index and re-run, forcing index use --
    print("\n  Part 2 - WITH a trigram index on papers.title (Index Scan expected)")
    print("  " + "-" * 66)
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute(
            "CREATE INDEX idx_papers_title ON papers "
            "USING gin (title gin_trgm_ops)"
        )
        cur.execute("ANALYZE papers")

        # On a table this small (3,446 rows, ~180 pages), the planner will
        # often still prefer Seq Scan because the overhead of a bitmap index
        # scan isn't worth it. That is correct cost-based behavior, but it
        # hides the plan difference from the audience. We disable Seq Scan
        # for this one EXPLAIN so the index-backed plan is visible.
        cur.execute("SET enable_seqscan = OFF")
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
        for (line,) in cur.fetchall():
            print(f"    {line}")
        cur.execute("SET enable_seqscan = ON")

        print("\n    Note: enable_seqscan was disabled only to force the index")
        print("    plan into view. With it enabled, the planner prefers Seq Scan")
        print("    here because the table is small enough that a scan beats the")
        print("    bitmap-build overhead. That IS the planner doing its job --")
        print("    on a 10-million-row table, the same query uses the index.")
    except psycopg2.Error as e:
        # pg_trgm may not be installed. Fall back to a plain equality query
        # with a B-tree index so the demo still makes its point.
        conn.rollback()
        msg = e.pgerror.strip() if e.pgerror else str(e).strip()
        print(f"    (pg_trgm not available: {msg})")
        print(f"    Falling back to an equality query with a standard B-tree index.")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_papers_title ON papers(title)")
        cur.execute("ANALYZE papers")
        eq_query = (
            "SELECT paper_id, year, title FROM papers "
            "WHERE title = 'Deep Learning'"
        )
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {eq_query}")
        for (line,) in cur.fetchall():
            print(f"    {line}")

    print("\n  --> Same SQL, same data. Only the physical access path changed.")
    print("      Seq Scan reads every heap page; Index Scan jumps to the ctid.")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 2: Join Planning - Hash Join vs Nested Loop with Index
#
# A 3-table join from papers -> paper_authors -> authors, filtered by
# author name. First with no indexes on the paper_authors join keys
# (forcing a hash join or seq scans), then with indexes added so the
# planner can use an index-backed nested loop.
# ─────────────────────────────────────────────────────────────────────────────
def run_join_demo():
    print(f"\n{DIVIDER}")
    print("DEMO 2: Join Planning -- the planner picks its strategy")
    print(DIVIDER)
    print("""
What this shows:
  A 3-table join finding every paper by 'Michael I. Jordan'.
    - First run: no indexes on paper_authors join columns.
      Planner falls back to a Seq Scan over paper_authors with a Hash Join.
    - Second run: indexes added on paper_authors(paper_id) and (author_id).
      Planner re-evaluates costs and switches to an index-backed plan.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    query = """
        SELECT p.title, p.year
        FROM papers p
        JOIN paper_authors pa ON p.paper_id = pa.paper_id
        JOIN authors a        ON pa.author_id = a.author_id
        WHERE a.name = 'Michael I. Jordan'
        ORDER BY p.year DESC NULLS LAST
    """

    # Show a few actual results first so the audience sees the query works
    print("  Results (up to 5 most recent papers by the author):")
    cur.execute(query + " LIMIT 5")
    rows = cur.fetchall()
    if not rows:
        print("    (no rows -- author not in dataset)")
    else:
        for title, year in rows:
            year_str = year if year is not None else "----"
            print(f"    ({year_str}) {title[:80]}")

    # -- Part 1: drop join-key indexes, show the plan --
    print("\n  Part 1 - WITHOUT paper_authors join indexes")
    print("  " + "-" * 66)
    cur.execute("DROP INDEX IF EXISTS idx_paper_authors_paper_id")
    cur.execute("DROP INDEX IF EXISTS idx_paper_authors_author_id")
    cur.execute("ANALYZE paper_authors")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
    for (line,) in cur.fetchall():
        print(f"    {line}")

    # -- Part 2: add indexes, re-plan --
    print("\n  Part 2 - WITH paper_authors join indexes")
    print("  " + "-" * 66)
    cur.execute("CREATE INDEX idx_paper_authors_paper_id  ON paper_authors(paper_id)")
    cur.execute("CREATE INDEX idx_paper_authors_author_id ON paper_authors(author_id)")
    cur.execute("ANALYZE paper_authors")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
    for (line,) in cur.fetchall():
        print(f"    {line}")

    print("\n  --> The planner is cost-based: give it an index and it will use it")
    print("      if and only if that plan is cheaper than the alternatives.")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(DIVIDER)
    print("PostgreSQL Query Planning and Execution Demo")
    print("DSCI 551 - Research Papers Database")
    print(DIVIDER)

    run_demo()
    run_join_demo()

    print(f"\n{DIVIDER}")
    print("All demos complete.")
    print(DIVIDER)