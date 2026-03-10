"""
demo_storage_indexing.py
DSCI 551 Group Project - Spring 2026
Focus Area: Physical Storage and Indexing (Bernard Yu)

This script demonstrates how PostgreSQL physically stores data and how
indexes speed up queries compared to full sequential scans, using the
research_papers database.

Usage:
    py demo_storage_indexing.py

Requirements:
    py -m pip install psycopg2-binary
"""

import psycopg2
import time

#Database connection settings
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "research_papers",
    "user":     "postgres",
    "password": "postgres"   # Change if your password is different
}

DIVIDER = "=" * 65


def get_conn():
    """Create and return a new database connection."""
    return psycopg2.connect(**DB_CONFIG)



# DEMO 1: Heap Storage - (physical storage of data)
def demo_heap_storage():
    print(f"\n{DIVIDER}")
    print("DEMO 1: Heap Storage")
    print(DIVIDER)
    print("""
  PostgreSQL stores table data in 8KB pages, which hold multiple row tuples.
  Rows aren't stored in order and are instead stored in heaps. which 
  is more energy efficient. We'll observe file size and page layout.
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
    print(f"  {'ctid':<10} {'paper_id':<5} {'title':<50}")
    print(f"  {'-'*12} {'-'*10} {'-'*50}")
    for r in rows:
        title_trunc = r[2][:50] if r[2] else ""
        print(f"  {str(r[0]):<12} {r[1]:<10} {title_trunc}")

    print(f"""
  Result:
    ctid = (page_number, tuple_number) possesses the physical address.
    (0,1) means page 0, tuple 1. PostgreSQL reads entire 8 KB pages
    into memory, so rows on the same page are fetched together.
    In a heap, new rows go wherever there is free space.
    There is no guaranteed ordering by paper_id or any other column.
""")

    cur.close()
    conn.close()



# DEMO 2: Sequential Scan vs Index Scan
def demo_seq_vs_index_scan():
    print(f"\n{DIVIDER}")
    print("DEMO 2: Sequential Scan vs Index Scan")
    print(DIVIDER)
    print("""
Following up on our previous concept, we'll compare the execution times for 
sequential scans versus scans for indixes. With an index, we can jump directly
to the page containing the tuple.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    test_query = "SELECT * FROM papers WHERE year = 2020"

    # index disabled for sequentail scan
    print(f"  [Seq Scan] Query: {test_query}\n")

    cur.execute("SET enable_indexscan = OFF")
    cur.execute("SET enable_bitmapscan = OFF")

    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {test_query}")
    plan_seq = cur.fetchall()
    for line in plan_seq:
        print(f"    {line[0]}")

    # time
    seq_time = None
    seq_buffers = None
    for line in plan_seq:
        if "Execution Time" in line[0]:
            seq_time = line[0].strip()
        if "Buffers" in line[0]:
            seq_buffers = line[0].strip()

    #Part B: Scan using index
    print(f"  [Index Scan] Query: {test_query}\n")

    cur.execute("SET enable_indexscan = ON")
    cur.execute("SET enable_bitmapscan = ON")

    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {test_query}")
    plan_idx = cur.fetchall()
    for line in plan_idx:
        print(f"    {line[0]}")

    idx_time = None
    idx_buffers = None
    for line in plan_idx:
        if "Execution Time" in line[0]:
            idx_time = line[0].strip()
        if "Buffers" in line[0]:
            idx_buffers = line[0].strip()

    print(f"""
  Result:
    Sequential Scan: {seq_time}
                     {seq_buffers}
    Index Scan:      {idx_time}
                     {idx_buffers}

    The sequential scan will read every page of a table.
    The index scan uses the B-tree on the selected year to choose the appropriate matching rows, 
    It can skip to the applicable tuple, resulting in a faster read time.
""")

    cur.close()
    conn.close()



# DEMO 3: B-Tree Index

def demo_btree_internals():
    print(f"\n{DIVIDER}")
    print("DEMO 3: B-Tree Index Structure")
    print(DIVIDER)
    print("""
  PostgreSQL's default index type is B-tree. We inspect the internal
  structure of our indexes, including the size, depth, and number of entries
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Install pgstattuple
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
        for idx_name in ['idx_papers_year', 'idx_papers_source', 'idx_authors_name', 'idx_keywords_keyword']:
            try:
                cur.execute(f"SELECT * FROM pgstatindex('{idx_name}')")
                stats = cur.fetchone()
                col_names = [desc[0] for desc in cur.description]
                stats_dict = dict(zip(col_names, stats))
                print(f"\n    {idx_name}:")
                print(f"      Tree depth (levels):     {stats_dict.get('tree_level', 'N/A')}")
                print(f"      Total index size:         {stats_dict.get('index_size', 'N/A')} bytes")
                print(f"      Leaf pages:               {stats_dict.get('leaf_pages', 'N/A')}")
                print(f"      Internal pages:           {stats_dict.get('internal_pages', 'N/A')}")
                print(f"      Entries:                  {stats_dict.get('num_index_tuples', 'N/A')}")
            except Exception as e:
                print(f"    {idx_name}: Could not read stats ({e})")
                conn.rollback()
    else:
        print(f"\n  (pgstattuple extension not working, cannot run B-tree analysis")

    print(f"""
  Result:
    B-tree indexes organize keys in a sorted tree of 8 KB pages.
    The tree level shows how many pages to move from root to leaf. Pages
    hold the key and the ctid, which point to the heap rows. 
""")

    cur.close()
    conn.close()



# DEMO 4: Impact of Creating and Dropping an Index
def demo_index_creation_impact():
    print(f"\n{DIVIDER}")
    print("DEMO 4: Create an Index, comparing before and after an index")
    print(DIVIDER)
    print("""
We can show how the lifecycle of an index by dropping an index (forcing a sequential scan,
recreating the index (using index scan), and measuring the performance difference on a query"
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Use the authors.name index for this demo
    test_query = "SELECT * FROM authors WHERE name = 'Yoshua Bengio'"

    # remove the index
    print("  [Step 1] Dropping idx_authors_name to force a sequential scan")
    cur.execute("DROP INDEX IF EXISTS idx_authors_name")

    print(f"  [Step 1] Query: {test_query}\n")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {test_query}")
    plan_no_index = cur.fetchall()
    for line in plan_no_index:
        print(f"    {line[0]}")

    no_idx_time = None
    for line in plan_no_index:
        if "Execution Time" in line[0]:
            no_idx_time = line[0].strip()

    # re-instate the index
    print(f"\n  [Step 2] Recreating idx_authors_name...")

    start = time.time()
    cur.execute("CREATE INDEX idx_authors_name ON authors(name)")
    create_time = (time.time() - start) * 1000
    print(f"  [Step 2] Index creation time: {create_time:.2f} ms")

    print(f"  [Step 2] Re-running same query with index...\n")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {test_query}")
    plan_with_index = cur.fetchall()
    for line in plan_with_index:
        print(f"    {line[0]}")

    idx_time = None
    for line in plan_with_index:
        if "Execution Time" in line[0]:
            idx_time = line[0].strip()

    print(f"""
  Result:
    Without index (Seq Scan):  {no_idx_time}
    With index (Index Scan):   {idx_time}

   Without the index, PostgreSQL scanned every row in authors to find 'Yoshua Bengio'.
   With the B-tree index on name, PostgreSQL skipped to the matching entry and only had to look at one heap page. 
   Indexes speed up the read, but cost disk space and can slow down writing.
""")

    cur.close()
    conn.close()



# DEMO 5: TOAST - Storing Large Values

def demo_toast():
    print(f"\n{DIVIDER}")
    print("DEMO 5: TOAST - Storing Oversized Values")
    print(DIVIDER)
    print("""

  PostgreSQL pages are 8 KB, but sometimes values are bigger.
  TOAST (The Oversized-Attribute Storage Technique) compresses or moves large values
  over to a seperate table. We'll take a look at TOAST usable columns and how much space
  they occupy.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    #check if any tables are using TOAST
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
        print(f"[TOAST Table] Papers TOAST storage size: {toast_info[1]}")
    else:
        print(f"No TOAST data (all values fit in main pages).")

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
    print(f"Papers with abstracts:    {stats[0]}")
    print(f"Average abstract length:  {stats[1]} characters")
    print(f"Longest abstract:         {stats[2]} characters")
    print(f"Abstracts over 2 KB:      {stats[3]}")

    print(f"""
  Result:
    TEXT columns use the "extended" strategy by default.
     PostgreSQL first compresses the value. If it's bigger than 2KB, it moves it
     to a separate TOAST table. The main heap page stores a small pointer to the TOAST
     table.  
""")

    cur.close()
    conn.close()



# Main
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