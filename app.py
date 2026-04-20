"""
app.py
DSCI 551 Group Project - Spring 2026
Research Paper Search Engine - Unified CLI Application

Bernard Yu      (Storage and Indexing)
Trina Nguyen    (Query Planning and Execution)
Stephen Rosario (Concurrency and Recovery)

This CLI ties the three focus-area demos into one user-facing application.
Every menu option is a real search-engine action AND a window into a specific
PostgreSQL internal mechanism, so the live demo naturally flows from
"what the user does" -> "what Postgres does internally" -> "why it matters."

Usage:
    python app.py                # interactive menu
    python app.py --demo all     # run every demo non-interactively (for grading)
    python app.py --demo storage
    python app.py --demo query
    python app.py --demo concurrency

Requirements:
    pip install psycopg2-binary

Connection settings are read from environment variables (PGHOST, PGPORT,
PGDATABASE, PGUSER, PGPASSWORD), falling back to localhost defaults. This
lets the same code run on Windows and WSL without edits.
"""

import argparse
import os
import sys

import psycopg2

# Make the focus-area demo modules importable regardless of where this is run from.
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "demos", "bernard"))
sys.path.insert(0, os.path.join(HERE, "demos", "stephen"))
sys.path.insert(0, os.path.join(HERE, "demos", "trina"))

import demo_storage_indexing as storage_demo   # noqa: E402
import demo_concurrency as concurrency_demo    # noqa: E402
import demo_query_planning as query_demo       # noqa: E402

# ─── Database connection ─────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("PGHOST", "localhost"),
    "port":     int(os.environ.get("PGPORT", 5432)),
    "dbname":   os.environ.get("PGDATABASE", "research_papers"),
    "user":     os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

DIVIDER = "=" * 72
SUB     = "-" * 72


def get_conn():
    """Open a fresh connection. Each menu action uses its own connection
    so transactions don't leak between operations."""
    return psycopg2.connect(**DB_CONFIG)


def pause():
    """Pause so the audience can read the previous output during a live demo."""
    if sys.stdin.isatty():
        try:
            input("\n  [press Enter to continue] ")
        except EOFError:
            pass


def print_header(title):
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


# ─────────────────────────────────────────────────────────────────────────────
# Action 1: Search papers by year  (Bernard - index vs seq scan)
# ─────────────────────────────────────────────────────────────────────────────
def action_search_by_year():
    print_header("Action 1: Search papers by year")

    year_in = input("  Enter a year to search (e.g., 2021): ").strip()
    try:
        year = int(year_in)
    except ValueError:
        print("  Not a valid year. Returning to menu.")
        return

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Show a few results so the user sees the application working.
    cur.execute(
        "SELECT paper_id, title, source FROM papers WHERE year = %s LIMIT 5",
        (year,),
    )
    rows = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM papers WHERE year = %s", (year,))
    total = cur.fetchone()[0]

    print(f"\n  Found {total} papers from {year}. Showing up to 5:")
    for r in rows:
        print(f"    [{r[0]:>5}] ({r[2]:<5}) {r[1][:80]}")

    # Expose internals: same query, EXPLAIN ANALYZE, with and without the index.
    print(f"\n  EXPLAIN ANALYZE -- WITHOUT the index (sequential scan forced):")
    cur.execute("SET enable_indexscan = OFF")
    cur.execute("SET enable_bitmapscan = OFF")
    cur.execute("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM papers WHERE year = %s",
                (year,))
    for line in cur.fetchall():
        print(f"    {line[0]}")

    print(f"\n  EXPLAIN ANALYZE -- WITH the index (idx_papers_year):")
    cur.execute("SET enable_indexscan = ON")
    cur.execute("SET enable_bitmapscan = ON")
    cur.execute("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM papers WHERE year = %s",
                (year,))
    for line in cur.fetchall():
        print(f"    {line[0]}")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Action 2: Heap storage + ctid  (Bernard)
# ─────────────────────────────────────────────────────────────────────────────
def action_heap_storage():
    print_header("Action 2: Heap storage -- 8 KB pages and ctid addressing")
    storage_demo.demo_heap_storage()


# ─────────────────────────────────────────────────────────────────────────────
# Action 3: B-tree internals  (Bernard)
# ─────────────────────────────────────────────────────────────────────────────
def action_btree_internals():
    print_header("Action 3: B-tree internals via pgstatindex")
    storage_demo.demo_btree_internals()


# ─────────────────────────────────────────────────────────────────────────────
# Action 4: Index creation impact  (Bernard)
# ─────────────────────────────────────────────────────────────────────────────
def action_index_creation_impact():
    print_header("Action 4: Index creation impact -- build cost vs read speedup")
    storage_demo.demo_index_creation_impact()


# ─────────────────────────────────────────────────────────────────────────────
# Action 5: TOAST  (Bernard)
# ─────────────────────────────────────────────────────────────────────────────
def action_toast_internals():
    print_header("Action 5: TOAST -- compressing and spilling long abstracts")
    storage_demo.demo_toast()


# ─────────────────────────────────────────────────────────────────────────────
# Action 6: Search papers by title  (Trina - trigram index)
#
# This is the core user-facing feature of a paper search engine: type part
# of a title, get matching papers. Internally it shows off the trigram GIN
# index that accelerates ILIKE '%substr%' queries.
# ─────────────────────────────────────────────────────────────────────────────
def action_search_by_title():
    print_header("Action 6: Search papers by title")

    term = input("  Enter a title keyword or phrase (e.g., 'learning'): ").strip()
    if not term:
        print("  Empty search. Returning to menu.")
        return

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Make sure pg_trgm + idx_papers_title exist. If the user hasn't run
    # Trina's demo yet, the index may be missing -- create it idempotently.
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_title "
            "ON papers USING gin (title gin_trgm_ops)"
        )
    except psycopg2.Error:
        conn.rollback()  # index creation may fail if extension isn't permitted

    pattern = f"%{term}%"

    # Show results for the user
    cur.execute(
        "SELECT paper_id, year, source, title "
        "FROM papers WHERE title ILIKE %s "
        "ORDER BY year DESC NULLS LAST LIMIT 10",
        (pattern,),
    )
    rows = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM papers WHERE title ILIKE %s", (pattern,))
    total = cur.fetchone()[0]

    print(f"\n  Found {total} papers matching '{term}'. Showing up to 10 (most recent first):")
    if not rows:
        print("    (no matches)")
    else:
        for r in rows:
            year = r[1] if r[1] is not None else "----"
            print(f"    [{r[0]:>5}] ({year}, {r[2]:<5}) {r[3][:72]}")

    # Expose the internals: same query, EXPLAIN ANALYZE.
    print(f"\n  EXPLAIN ANALYZE of the search:")
    cur.execute(
        "EXPLAIN (ANALYZE, BUFFERS) "
        "SELECT paper_id, year, source, title "
        "FROM papers WHERE title ILIKE %s "
        "ORDER BY year DESC NULLS LAST LIMIT 10",
        (pattern,),
    )
    for line in cur.fetchall():
        print(f"    {line[0]}")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Action 7: Look up an author  (Trina - join planning)
# ─────────────────────────────────────────────────────────────────────────────
def action_lookup_author():
    print_header("Action 7: Look up an author and their papers")

    name = input("  Enter author name (e.g., Michael I. Jordan): ").strip()
    if not name:
        print("  Empty name. Returning to menu.")
        return

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    join_query = """
        SELECT p.paper_id, p.year, p.source, p.title
        FROM papers p
        JOIN paper_authors pa ON p.paper_id = pa.paper_id
        JOIN authors a        ON pa.author_id = a.author_id
        WHERE a.name = %s
        ORDER BY p.year DESC NULLS LAST
        LIMIT 10
    """
    cur.execute(join_query, (name,))
    rows = cur.fetchall()
    if not rows:
        print(f"\n  No papers found for '{name}'.")
        print("  Tip: try a name from the dataset, e.g., 'Michael I. Jordan' or "
              "'Yoshua Bengio'.")
    else:
        print(f"\n  Most recent papers by '{name}':")
        for r in rows:
            year = r[1] if r[1] is not None else "----"
            print(f"    [{r[0]:>5}] ({year}, {r[2]:<5}) {r[3][:75]}")

    # Show the planner's choice on the actual join.
    print(f"\n  EXPLAIN ANALYZE of the join:")
    explain_q = "EXPLAIN (ANALYZE, BUFFERS) " + join_query.replace("%s", "%(name)s")
    cur.execute(explain_q, {"name": name})
    for line in cur.fetchall():
        print(f"    {line[0]}")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Action 8: Analytics aggregation  (Trina - HashAggregate)
# ─────────────────────────────────────────────────────────────────────────────
def action_analytics():
    print_header("Action 8: Analytics -- papers per year per source")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    q = """
        SELECT year, source, COUNT(*) AS n
        FROM papers
        WHERE year IS NOT NULL
        GROUP BY year, source
        ORDER BY year DESC, source
        LIMIT 20
    """
    cur.execute(q)
    rows = cur.fetchall()
    print(f"\n  Top 20 (year, source) buckets by recency:")
    print(f"    {'year':<6} {'source':<6} {'count':>6}")
    print(f"    {'-'*6} {'-'*6} {'-'*6}")
    for r in rows:
        print(f"    {r[0]:<6} {r[1]:<6} {r[2]:>6}")

    print(f"\n  EXPLAIN ANALYZE of the aggregation:")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {q}")
    for line in cur.fetchall():
        print(f"    {line[0]}")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Action 9: Query planner walk-through  (Trina - planner re-plans)
# ─────────────────────────────────────────────────────────────────────────────
def action_query_planning_walkthrough():
    print_header("Action 9: Query planner walk-through")

    query_demo.run_demo()
    pause()
    query_demo.run_join_demo()


# ─────────────────────────────────────────────────────────────────────────────
# Action 10: Insert paper while reading  (Stephen - MVCC)
# ─────────────────────────────────────────────────────────────────────────────
def action_insert_paper_concurrent():
    print_header("Action 10: Insert a paper while a reader holds a snapshot")

    print("""  This simulates two users hitting the search engine at the same time:
    Reader  -- counts 2021 papers, sleeps 2s, counts again (single transaction)
    Writer  -- inserts a brand-new 2021 paper while the reader is sleeping
""")

    concurrency_demo.demo_mvcc()


# ─────────────────────────────────────────────────────────────────────────────
# Action 11: Bulk update + VACUUM  (Stephen)
# ─────────────────────────────────────────────────────────────────────────────
def action_bulk_update_vacuum():
    print_header("Action 11: Bulk update, dead tuples, and VACUUM")

    concurrency_demo.demo_vacuum()


# ─────────────────────────────────────────────────────────────────────────────
# Action 12: Isolation levels  (Stephen)
# ─────────────────────────────────────────────────────────────────────────────
def action_isolation_levels():
    print_header("Action 12: Isolation levels -- Read Committed vs Repeatable Read")
    concurrency_demo.demo_isolation_levels()


# ─────────────────────────────────────────────────────────────────────────────
# Action 13: Atomic transaction  (Stephen)
# ─────────────────────────────────────────────────────────────────────────────
def action_atomic_transaction():
    print_header("Action 13: Transaction atomicity -- all-or-nothing")
    concurrency_demo.demo_atomicity()


# ─────────────────────────────────────────────────────────────────────────────
# Action 14: Health check / row counts
# ─────────────────────────────────────────────────────────────────────────────
def action_health_check():
    print_header("Action 14: Database health check")

    try:
        conn = get_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT version()")
        print(f"  Connected: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM papers")
        print(f"  papers:         {cur.fetchone()[0]:,}")
        cur.execute("SELECT COUNT(*) FROM authors")
        print(f"  authors:        {cur.fetchone()[0]:,}")
        cur.execute("SELECT COUNT(*) FROM paper_authors")
        print(f"  paper_authors:  {cur.fetchone()[0]:,}")
        cur.execute("SELECT COUNT(*) FROM keywords")
        print(f"  keywords:       {cur.fetchone()[0]:,}")
        cur.execute("SELECT source, COUNT(*) FROM papers GROUP BY source ORDER BY source")
        for r in cur.fetchall():
            print(f"  papers ({r[0]}):    {r[1]:,}")
        cur.close()
        conn.close()
        print("\n  All good.")
    except Exception as e:
        print(f"  FAILED to connect: {e}")
        print(f"  Check DB_CONFIG at the top of app.py, or your PG* env vars.")


# ─── Menu ────────────────────────────────────────────────────────────────────
MENU = [
    # --- Bernard: storage and indexing ---
    ("Search papers by year         (Bernard - index vs seq scan)",      action_search_by_year),
    ("Heap storage + ctid           (Bernard - 8 KB heap pages)",        action_heap_storage),
    ("B-tree internals              (Bernard - pgstatindex depth)",      action_btree_internals),
    ("Index creation impact         (Bernard - build cost vs speedup)",  action_index_creation_impact),
    ("TOAST for long abstracts      (Bernard - oversized values)",       action_toast_internals),
    # --- Trina: query planning and execution ---
    ("Search papers by title        (Trina   - trigram index)",          action_search_by_title),
    ("Look up an author             (Trina   - join planning)",          action_lookup_author),
    ("Analytics aggregation         (Trina   - HashAggregate)",          action_analytics),
    ("Query planner walk-through    (Trina   - planner re-plans)",       action_query_planning_walkthrough),
    # --- Stephen: concurrency and recovery ---
    ("Insert paper while reading    (Stephen - MVCC snapshot)",          action_insert_paper_concurrent),
    ("Bulk update + VACUUM          (Stephen - dead tuples)",            action_bulk_update_vacuum),
    ("Isolation levels in action    (Stephen - RC vs RR)",               action_isolation_levels),
    ("Atomic transaction + rollback (Stephen - WAL atomicity)",          action_atomic_transaction),
    # --- Ops ---
    ("Health check / row counts",                                         action_health_check),
]


def interactive_menu():
    while True:
        print(f"\n{DIVIDER}")
        print("  Research Paper Search Engine -- DSCI 551 Group Project")
        print("  PostgreSQL internals demo")
        print(DIVIDER)
        for i, (label, _) in enumerate(MENU, start=1):
            print(f"   {i:>2}.  {label}")
        print(f"    q.  Quit")
        print(DIVIDER)

        choice = input("  Choose: ").strip().lower()
        if choice in ("q", "quit", "exit"):
            print("  Bye.")
            return
        try:
            idx = int(choice) - 1
            if not 0 <= idx < len(MENU):
                raise ValueError
        except ValueError:
            print("  Invalid choice.")
            continue

        try:
            MENU[idx][1]()
        except KeyboardInterrupt:
            print("\n  Interrupted. Returning to menu.")
        except psycopg2.Error as e:
            print(f"\n  Database error: {e}")
        except Exception as e:
            print(f"\n  Unexpected error: {e}")


# ─── Non-interactive runner for TA / grading ─────────────────────────────────
def run_demo_bundle(name):
    bundles = {
        "storage":     [action_health_check,
                        action_search_by_year_canned,
                        action_heap_storage,
                        action_btree_internals,
                        action_index_creation_impact,
                        action_toast_internals],
        "query":       [action_health_check,
                        action_search_by_title_canned,
                        action_lookup_author_canned,
                        action_analytics,
                        action_query_planning_walkthrough],
        "concurrency": [action_health_check,
                        action_insert_paper_concurrent,
                        action_bulk_update_vacuum,
                        action_isolation_levels,
                        action_atomic_transaction],
        "all":         [action_health_check,
                        # Bernard
                        action_search_by_year_canned,
                        action_heap_storage,
                        action_btree_internals,
                        action_index_creation_impact,
                        action_toast_internals,
                        # Trina
                        action_search_by_title_canned,
                        action_lookup_author_canned,
                        action_analytics,
                        action_query_planning_walkthrough,
                        # Stephen
                        action_insert_paper_concurrent,
                        action_bulk_update_vacuum,
                        action_isolation_levels,
                        action_atomic_transaction],
    }
    if name not in bundles:
        print(f"Unknown demo bundle '{name}'. Choose from: {list(bundles)}")
        sys.exit(1)
    for fn in bundles[name]:
        fn()


# Canned (non-interactive) variants used by --demo so we don't block on input().
def action_search_by_year_canned():
    print_header("Action 1 (canned): Search papers by year = 2021")
    conn = get_conn(); conn.autocommit = True; cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM papers WHERE year = 2021")
    print(f"  Papers from 2021: {cur.fetchone()[0]}")
    print("\n  EXPLAIN ANALYZE -- without index:")
    cur.execute("SET enable_indexscan = OFF"); cur.execute("SET enable_bitmapscan = OFF")
    cur.execute("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM papers WHERE year = 2021")
    for line in cur.fetchall(): print(f"    {line[0]}")
    print("\n  EXPLAIN ANALYZE -- with index:")
    cur.execute("SET enable_indexscan = ON"); cur.execute("SET enable_bitmapscan = ON")
    cur.execute("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM papers WHERE year = 2021")
    for line in cur.fetchall(): print(f"    {line[0]}")
    cur.close(); conn.close()


def action_search_by_title_canned():
    print_header("Action 6 (canned): Title search for 'learning'")
    conn = get_conn(); conn.autocommit = True; cur = conn.cursor()
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_papers_title "
            "ON papers USING gin (title gin_trgm_ops)"
        )
    except psycopg2.Error:
        conn.rollback()

    cur.execute(
        "SELECT paper_id, year, title FROM papers "
        "WHERE title ILIKE '%learning%' "
        "ORDER BY year DESC NULLS LAST LIMIT 5"
    )
    print("  Top 5 results for 'learning':")
    for r in cur.fetchall():
        year = r[1] if r[1] is not None else "----"
        print(f"    [{r[0]:>5}] ({year}) {r[2][:75]}")

    print("\n  EXPLAIN ANALYZE of the title search:")
    cur.execute(
        "EXPLAIN (ANALYZE, BUFFERS) "
        "SELECT paper_id, year, title FROM papers "
        "WHERE title ILIKE '%learning%' "
        "ORDER BY year DESC NULLS LAST LIMIT 5"
    )
    for line in cur.fetchall(): print(f"    {line[0]}")
    cur.close(); conn.close()


def action_lookup_author_canned():
    print_header("Action 7 (canned): Author lookup -- 'Michael I. Jordan'")
    conn = get_conn(); conn.autocommit = True; cur = conn.cursor()
    q = """
        SELECT p.title, p.year
        FROM papers p
        JOIN paper_authors pa ON p.paper_id = pa.paper_id
        JOIN authors a        ON pa.author_id = a.author_id
        WHERE a.name = 'Michael I. Jordan'
        ORDER BY p.year DESC NULLS LAST
        LIMIT 5
    """
    cur.execute(q)
    for r in cur.fetchall():
        print(f"    ({r[1]}) {r[0][:80]}")
    print("\n  EXPLAIN ANALYZE of the join:")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {q}")
    for line in cur.fetchall(): print(f"    {line[0]}")
    cur.close(); conn.close()


# ─── Entry point ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="DSCI 551 Research Paper Search Engine")
    parser.add_argument("--demo",
                        choices=["all", "storage", "query", "concurrency"],
                        help="Run a demo bundle non-interactively (no prompts).")
    args = parser.parse_args()

    if args.demo:
        run_demo_bundle(args.demo)
    else:
        interactive_menu()


if __name__ == "__main__":
    main()