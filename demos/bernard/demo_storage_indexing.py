"""
demo_storage_indexing.py
DSCI 551 Group Project - Spring 2026
Focus Area: Physical Storage and Indexing (Bernard Yu)

This script demonstrates how PostgreSQL physically stores data and how
indexes speed up queries compared to full sequential scans, using the
research_papers database.

It runs five demos:
  1. Heap storage - 8 KB pages, ctid addressing, non-deterministic row order
  2. Sequential Scan vs Index Scan - planner-level comparison on papers.year
  3. B-tree internals - pgstatindex depth, leaf pages, entry counts
  4. Index creation impact - drop/recreate idx_authors_name and measure cost
  5. TOAST - how PostgreSQL handles oversized text values

Usage:
    python demo_storage_indexing.py

Requirements:
    pip install psycopg2-binary

Connection settings are read from environment variables (PGHOST, PGPORT,
PGDATABASE, PGUSER, PGPASSWORD), falling back to localhost defaults.
"""

import os
import time

import psycopg2

# ─── Database connection settings ────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.environ.get("PGHOST", "localhost"),
    "port":     int(os.environ.get("PGPORT", 5432)),
    "dbname":   os.environ.get("PGDATABASE", "research_papers"),
    "user":     os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

DIVIDER = "=" * 65


def get_conn():
    """Create and return a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 1: Heap Storage - physical storage of data
# ─────────────────────────────────────────────────────────────────────────────
def demo_heap_storage():
    print(f"\n{DIVIDER}")
    print("DEMO 1: Heap Storage")
    print(DIVIDER)
    print("""
  PostgreSQL stores table data in 8KB pages, which hold multiple row tuples.
  Rows aren't stored in order and are instead stored in heaps, which
  is more space efficient. We'll observe file size and page layout.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Show total relation size (data + indexes + toast)
    print("  [Papers Table] Inspecting physical storage layout...")
    cur.execute("""
        SELECT
            pg_size_pretty(pg_total_relation_size('papers'))   AS total_size,
            pg_size_pretty(pg_relation_size('papers'))         AS table_data_size,
            pg_size_pretty(pg_indexes_size('papers'))          AS index_size,
            pg_relation_size('papers') / 8192                  AS data_pages
    """)
    row = cur.fetchone()
    print(f"    Total size (data + indexes): {row[0]}")
    print(f"    Table data (heap) size:      {row[1]}")
    print(f"    Index size:                  {row[2]}")
    print(f"    Data pages (8 KB each):      {row[3]}")

    # Show for authors
    print(f"\n  [Authors Table] Inspecting physical storage layout...")
    cur.execute("""
        SELECT
            pg_size_pretty(pg_total_relation_size('authors'))  AS total_size,
            pg_size_pretty(pg_relation_size('authors'))        AS table_data_size,
            pg_size_pretty(pg_indexes_size('authors'))         AS index_size,
            pg_relation_size('authors') / 8192                 AS data_pages
    """)
    row = cur.fetchone()
    print(f"    Total size (data + indexes): {row[0]}")
    print(f"    Table data (heap) size:      {row[1]}")
    print(f"    Index size:                  {row[2]}")
    print(f"    Data pages (8 KB each):      {row[3]}")

    # Show tuple-level details using the system columns
    cur.execute("""
        SELECT ctid, paper_id, title
        FROM papers
        ORDER BY ctid
        LIMIT 10
    """)
    rows = cur.fetchall()
    print(f"\n  [Physical Addresses] First 10 tuples by ctid:")
    print(f"  {'ctid':<12} {'paper_id':<10} {'title':<50}")
    print(f"  {'-'*12} {'-'*10} {'-'*50}")
    for r in rows:
        title_trunc = r[2][:50] if r[2] else ""
        print(f"  {str(r[0]):<12} {r[1]:<10} {title_trunc}")

    print(f"""
  Result:
    ctid = (page_number, tuple_number) is the physical address.
    (0,1) means page 0, tuple 1. PostgreSQL reads entire 8 KB pages
    into memory, so rows on the same page are fetched together.
    In a heap, new rows go wherever there is free space --
    there is no guaranteed ordering by paper_id or any other column.
""")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 2: Sequential Scan vs Index Scan
# ─────────────────────────────────────────────────────────────────────────────
def demo_seq_vs_index_scan():
    print(f"\n{DIVIDER}")
    print("DEMO 2: Sequential Scan vs Index Scan")
    print(DIVIDER)
    print("""
  Following up on Demo 1, we'll compare the execution times for sequential
  scans versus index scans. With an index, PostgreSQL can jump directly to
  the heap page containing matching tuples instead of reading every page.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    test_query = "SELECT * FROM papers WHERE year = 2020"

    # -- Part A: Seq Scan (indexes disabled) --
    print(f"  [Seq Scan] Query: {test_query}\n")
    cur.execute("SET enable_indexscan = OFF")
    cur.execute("SET enable_bitmapscan = OFF")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {test_query}")
    plan_seq = cur.fetchall()
    for line in plan_seq:
        print(f"    {line[0]}")

    seq_time = next((l[0].strip() for l in plan_seq if "Execution Time" in l[0]), None)

    # -- Part B: Index Scan (indexes re-enabled) --
    print(f"\n  [Index Scan] Query: {test_query}\n")
    cur.execute("SET enable_indexscan = ON")
    cur.execute("SET enable_bitmapscan = ON")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {test_query}")
    plan_idx = cur.fetchall()
    for line in plan_idx:
        print(f"    {line[0]}")

    idx_time = next((l[0].strip() for l in plan_idx if "Execution Time" in l[0]), None)

    print(f"""
  Result:
    Sequential Scan: {seq_time}
    Index Scan:      {idx_time}

    The sequential scan reads every page of the table.
    The index scan uses the B-tree on year to find only matching rows,
    skipping straight to the applicable tuples for a faster read time.
""")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 3: B-Tree Index Structure
# ─────────────────────────────────────────────────────────────────────────────
def demo_btree_internals():
    print(f"\n{DIVIDER}")
    print("DEMO 3: B-Tree Index Structure")
    print(DIVIDER)
    print("""
  PostgreSQL's default index type is B-tree. We inspect the internal
  structure of our indexes, including size, depth, and number of entries.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Install pgstattuple (provides pgstatindex)
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS pgstattuple")
        has_pgstattuple = True
    except Exception:
        conn.rollback()
        has_pgstattuple = False

    # Show all indexes on our tables
    print("  [Index Catalog] All indexes in the database:")
    cur.execute("""
        SELECT
            indexname,
            tablename,
            pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
            pg_relation_size(indexname::regclass) / 8192 AS index_pages
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY pg_relation_size(indexname::regclass) DESC
    """)
    rows = cur.fetchall()
    print(f"  {'Index Name':<35} {'Table':<20} {'Size':<10} {'Pages':<6}")
    print(f"  {'-'*35} {'-'*20} {'-'*10} {'-'*6}")
    for r in rows:
        print(f"  {r[0]:<35} {r[1]:<20} {r[2]:<10} {r[3]:<6}")

    # Show B-tree depth using pgstatindex if available
    if has_pgstattuple:
        print(f"\n  [B-tree Depth] Analyzing index internals via pgstatindex:")
        # Map each index to the (table, column) it indexes so we can count
        # entries directly. pgstatindex itself doesn't expose a tuple count.
        # We exclude idx_keywords_keyword because the keywords table is
        # empty in our loaded dataset, so its stats would just be zeros.
        index_sources = {
            'idx_papers_year':   ('papers',  'year'),
            'idx_papers_source': ('papers',  'source'),
            'idx_authors_name':  ('authors', 'name'),
        }
        for idx_name, (table, column) in index_sources.items():
            try:
                cur.execute(f"SELECT * FROM pgstatindex('{idx_name}')")
                stats = cur.fetchone()
                col_names = [desc[0] for desc in cur.description]
                stats_dict = dict(zip(col_names, stats))

                # B-tree indexes include one entry per non-NULL row.
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL"
                )
                entries = cur.fetchone()[0]

                print(f"\n    {idx_name}:")
                print(f"      Tree depth (levels):     {stats_dict.get('tree_level', 'N/A')}")
                print(f"      Total index size:         {stats_dict.get('index_size', 'N/A')} bytes")
                print(f"      Leaf pages:               {stats_dict.get('leaf_pages', 'N/A')}")
                print(f"      Internal pages:           {stats_dict.get('internal_pages', 'N/A')}")
                print(f"      Entries:                  {entries}")
                print(f"      Avg leaf density:         {stats_dict.get('avg_leaf_density', 'N/A')}%")
                print(f"      Leaf fragmentation:       {stats_dict.get('leaf_fragmentation', 'N/A')}%")
            except Exception as e:
                print(f"    {idx_name}: Could not read stats ({e})")
                conn.rollback()
    else:
        print(f"\n  (pgstattuple extension not working -- cannot run B-tree analysis)")

    print(f"""
  Result:
    B-tree indexes organize keys in a sorted tree of 8 KB pages.
    The tree level shows how many pages PostgreSQL traverses from root
    to leaf. Leaf pages hold the key and the ctid, which points to
    the heap row. Leaf density shows how full pages are; fragmentation
    measures how much the physical page order has drifted from key order.
""")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 4: Impact of Creating and Dropping an Index
#
# Note: authors.name has a UNIQUE constraint, which auto-creates an implicit
# index (authors_name_key). To force a real Seq Scan, we must drop BOTH the
# explicit idx_authors_name AND the implicit authors_name_key, then recreate
# both afterward to leave the schema as we found it.
# ─────────────────────────────────────────────────────────────────────────────
def demo_index_creation_impact():
    print(f"\n{DIVIDER}")
    print("DEMO 4: Index Creation Impact -- before and after")
    print(DIVIDER)
    print("""
  Show the lifecycle of an index: drop it (forcing a sequential scan),
  recreate it (enabling index scan), and measure the performance difference.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    test_query = "SELECT * FROM authors WHERE name = 'Yoshua Bengio'"

    # -- Step 1: drop BOTH indexes on authors.name --
    # The unique constraint creates its own implicit index (authors_name_key),
    # so we must drop it AND our explicit idx_authors_name to force a Seq Scan.
    # We drop the unique constraint instead of the index directly, since the
    # index is owned by the constraint.
    print("  [Step 1] Dropping idx_authors_name AND the unique constraint")
    print("           on authors.name to force a true sequential scan")
    cur.execute("DROP INDEX IF EXISTS idx_authors_name")
    cur.execute("ALTER TABLE authors DROP CONSTRAINT IF EXISTS authors_name_key")

    print(f"\n  [Step 1] Query: {test_query}\n")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {test_query}")
    plan_no_index = cur.fetchall()
    for line in plan_no_index:
        print(f"    {line[0]}")

    no_idx_time = next((l[0].strip() for l in plan_no_index if "Execution Time" in l[0]), None)

    # -- Step 2: recreate both, measure build time --
    print(f"\n  [Step 2] Recreating idx_authors_name and the unique constraint...")
    start = time.time()
    cur.execute("CREATE INDEX idx_authors_name ON authors(name)")
    cur.execute("ALTER TABLE authors ADD CONSTRAINT authors_name_key UNIQUE (name)")
    create_time = (time.time() - start) * 1000
    print(f"  [Step 2] Index creation time: {create_time:.2f} ms")

    print(f"  [Step 2] Re-running same query with index...\n")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {test_query}")
    plan_with_index = cur.fetchall()
    for line in plan_with_index:
        print(f"    {line[0]}")

    idx_time = next((l[0].strip() for l in plan_with_index if "Execution Time" in l[0]), None)

    print(f"""
  Result:
    Without index (Seq Scan):  {no_idx_time}
    With index (Index Scan):   {idx_time}

    Without any index on authors.name, PostgreSQL scanned every row in
    the table to find 'Yoshua Bengio'. With the B-tree index restored,
    PostgreSQL jumped directly to the matching entry and only touched
    one heap page. Indexes speed up reads, but cost disk space and add
    write overhead (every INSERT/UPDATE must also update the B-tree).
""")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 5: TOAST - Storing Large Values
# ─────────────────────────────────────────────────────────────────────────────
def demo_toast():
    print(f"\n{DIVIDER}")
    print("DEMO 5: TOAST -- Storing Oversized Values")
    print(DIVIDER)
    print("""
  PostgreSQL pages are 8 KB, but sometimes values are bigger.
  TOAST (The Oversized-Attribute Storage Technique) compresses or moves
  large values to a separate table. We'll look at TOAST-able columns
  and how much space they occupy.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Check if the papers table has a TOAST table attached
    print("  [TOAST Table] Checking if papers table uses TOAST storage...")
    cur.execute("""
        SELECT
            c.relname AS table_name,
            pg_size_pretty(pg_relation_size(c.reltoastrelid)) AS toast_size,
            pg_relation_size(c.reltoastrelid) AS toast_bytes
        FROM pg_class c
        WHERE c.relname = 'papers'
          AND c.reltoastrelid != 0
    """)
    toast_info = cur.fetchone()
    if toast_info:
        print(f"    Papers TOAST storage size: {toast_info[1]}")
    else:
        print(f"    No TOAST data (all values fit in main pages).")

    # Show storage strategy for each column
    print(f"\n  [Column Strategies] TOAST storage strategy per column:")
    cur.execute("""
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
            CASE a.attstorage
                WHEN 'p' THEN 'plain (never TOASTed)'
                WHEN 'e' THEN 'external (stored out-of-line if large)'
                WHEN 'x' THEN 'extended (compressed, then out-of-line if still large)'
                WHEN 'm' THEN 'main (compressed in-line, out-of-line as last resort)'
            END AS storage_strategy
        FROM pg_attribute a
        JOIN pg_class c ON a.attrelid = c.oid
        WHERE c.relname = 'papers'
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    """)
    rows = cur.fetchall()
    print(f"  {'Column':<15} {'Type':<12} {'TOAST Strategy'}")
    print(f"  {'-'*15} {'-'*12} {'-'*45}")
    for r in rows:
        print(f"  {r[0]:<15} {r[1]:<12} {r[2]}")

    # Show abstract length distribution to illustrate why TOAST matters
    print(f"\n  [Abstract Stats] Measuring abstract lengths to show why TOAST matters...")
    cur.execute("""
        SELECT
            COUNT(*) AS total_papers,
            ROUND(AVG(LENGTH(abstract))) AS avg_abstract_len,
            MAX(LENGTH(abstract)) AS max_abstract_len,
            SUM(CASE WHEN LENGTH(abstract) > 2000 THEN 1 ELSE 0 END) AS abstracts_over_2kb
        FROM papers
        WHERE abstract IS NOT NULL AND abstract != ''
    """)
    stats = cur.fetchone()
    print(f"    Papers with abstracts:    {stats[0]}")
    print(f"    Average abstract length:  {stats[1]} characters")
    print(f"    Longest abstract:         {stats[2]} characters")
    print(f"    Abstracts over 2 KB:      {stats[3]}")

    print(f"""
  Result:
    TEXT columns use the "extended" strategy by default.
    PostgreSQL first compresses the value. If it's still bigger than 2 KB,
    it moves the value to a separate TOAST table and the main heap page
    stores only a small pointer. This keeps heap scans fast even when
    individual columns are huge.
""")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(DIVIDER)
    print("PostgreSQL Physical Storage and Indexing Demo")
    print("DSCI 551 - Research Papers Database")
    print(DIVIDER)

    demo_heap_storage()
    demo_seq_vs_index_scan()
    demo_btree_internals()
    demo_index_creation_impact()
    demo_toast()

    print(f"\n{DIVIDER}")
    print("All demos complete.")
    print(DIVIDER)