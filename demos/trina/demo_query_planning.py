"""
demo_query_planning.py
DSCI 551 Group Project - Spring 2026
Focus Area: Query Planning and Execution (Trina Nguyen)

This script demonstrates how PostgreSQL plans and executes queries using 
EXPLAIN ANALYZE, comparing sequential scans vs index scans, hash join vs nested loops.
"""

import psycopg2

# DEMO 1: Sequential Scans vs Index Scans (Explain Analyze) 

# Database connection settings
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "research_papers",
    "user": "postgres",
    "password": "postgres"
} 

def run_demo():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursors = conn.cursor()

    print("\n" + "="*70)
    print("Part 1 - Sequential Scan (The Slow Method)")
    print("="*70)
    
    # Sequential Scan: 
    # Drop the index to force a Seq Scan
    cursors.execute("DROP INDEX IF EXISTS idx_title")
    
    query = "SELECT * FROM papers WHERE title = 'Deep Learning';"
    print(f"Running: EXPLAIN ANALYZE {query}\n")
    
    cursors.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
    for outputs in cursors.fetchall():
        print(f"    {outputs[0]}")

    print("\n" + "="*70)
    print("Part 2 - Index Scan (The Fast Way)")
    print("="*70)
    
    # Index Scan: 
    # Create index to show the boost
    cursors.execute("CREATE INDEX idx_title ON papers(title)")
    
    print(f"Running: EXPLAIN ANALYZE {query} (with index created)\n")
    cursors.execute(f"EXPLAIN (ANALYZE, BUFFERS) {query}")
    for output in cursors.fetchall():
        print(f"    {output[0]}")

    cursors.close()
    conn.close()



# DEMO 2: Hash Join vs Nested Loop

def run_join_demo():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursors = conn.cursor()

    print("\n" + "="*70)
    print("JOIN DEMO: CONNECTING THE AUTHORS TO EACH PAPER")
    print("="*70)

    # We want to find all papers written by 'eg: Michael I. Jordan' (or any author in your data)
    # This requires joining the 'authors' table to the 'paper_authors' link table

    # QUERY FOR WHEN PAPER WAS PUBLISHED (JOIN) 
    query1 = """ 
        SELECT p.title, p.year
        FROM papers p
        JOIN paper_authors pa ON p.paper_id = pa.paper_id
        JOIN authors a ON pa.author_id = a.author_id
        WHERE a.name = 'Michael I. Jordan'; -- most popular
    """

    # QUERY FOR MOST RECENT PAPER PUBLISHED (FILTERED JOIN)
    query2 = """ 
        SELECT p.title, p.year
        FROM papers p
        JOIN paper_authors pa ON p.paper_id = pa.paper_id
        JOIN authors a ON pa.author_id = a.author_id
        WHERE a.name = 'Michael I. Jordan'; -- most popular
 	AND p.year > 2023; -- can change the year
    """

    # Specify which query to use (can change)
    current_query = query1
    
    # --- Step 1: No Index on the Join Columns ---
    print("Step 1: Dropping indexes to show a 'Heavy' Join...")
    cursors.execute("DROP INDEX IF EXISTS index_paper_id")
    cursors.execute("DROP INDEX IF EXISTS index_paper_author_id")
    
    print("\nAnalyzing Join Plan (No Indexes):")
    cursors.execute(f"EXPLAIN (ANALYZE, COSTS OFF) {current_query}")
    for output in cursors.fetchall():
        print(f"    {output[0]}")

    # --- Step 2: Adding Indexes ---
    print("\n" + "="*70)
    print("Step 2: Creating Indexes on Join Keys...")
    cursors.execute("CREATE INDEX index_paper_id ON paper_authors(paper_id)")
    cursors.execute("CREATE INDEX index_paper_author_id ON paper_authors(author_id)")
    
    print("\nAnalyzing Optimized Join Plan:")
    cursors.execute(f"EXPLAIN (ANALYZE, COSTS OFF) {current_query}")
    for output in cursors.fetchall():
        print(f"    {output[0]}")

    cursors.close()
    conn.close()

if __name__ == "__main__":
    run_demo()
    run_join_demo()
EOF
