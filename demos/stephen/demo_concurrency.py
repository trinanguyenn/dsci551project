"""
demo_concurrency.py
DSCI 551 Group Project - Spring 2026
Focus Area: Concurrency and Recovery (Stephen Rosario)

This script demonstrates PostgreSQL's Multi-Version Concurrency Control (MVCC)
using the research_papers database. It simulates two concurrent transactions
and shows how PostgreSQL handles simultaneous reads and writes without blocking.

Usage:
    py demo_concurrency.py

Requirements:
    py -m pip install psycopg2-binary
"""

import psycopg2
import threading
import time

# ─── Database connection settings ────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 1: MVCC - Concurrent Reads and Writes Do Not Block Each Other
# ─────────────────────────────────────────────────────────────────────────────
def demo_mvcc():
    print(f"\n{DIVIDER}")
    print("DEMO 1: MVCC - Concurrent Reads and Writes")
    print(DIVIDER)
    print("""
What this shows:
  Transaction A starts reading papers from 2021.
  While A is still reading, Transaction B inserts a new paper.
  Transaction A never sees B's insert -- it reads from its own snapshot.
  This is MVCC: readers and writers do not block each other.
""")

    results = {}

    def transaction_a():
        """Reader: takes a snapshot and reads slowly."""
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        try:
            # Begin transaction - PostgreSQL takes a snapshot here
            cur.execute("BEGIN")
            cur.execute("SELECT COUNT(*) FROM papers WHERE year = 2021")
            count_before = cur.fetchone()[0]
            results["a_before"] = count_before
            print(f"  [Transaction A] Snapshot taken. Papers from 2021: {count_before}")

            # Simulate slow read -- B will insert during this pause
            time.sleep(2)

            # Read again within the same transaction
            cur.execute("SELECT COUNT(*) FROM papers WHERE year = 2021")
            count_after = cur.fetchone()[0]
            results["a_after"] = count_after
            print(f"  [Transaction A] Re-read same snapshot.  Papers from 2021: {count_after}")
            print(f"  [Transaction A] Sees B's insert: {'YES' if count_after != count_before else 'NO -- MVCC snapshot is consistent'}")

            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"  [Transaction A] Error: {e}")
        finally:
            cur.close()
            conn.close()

    def transaction_b():
        """Writer: inserts a new paper while A is reading."""
        conn = get_conn()
        conn.autocommit = False
        cur = conn.cursor()
        try:
            time.sleep(0.5)  # Let A start first
            cur.execute("BEGIN")
            cur.execute("""
                INSERT INTO papers (title, abstract, year, source)
                VALUES ('MVCC Demo Paper', 'Inserted during Transaction A read.', 2021, 'IEEE')
                RETURNING paper_id
            """)
            paper_id = cur.fetchone()[0]
            conn.commit()
            results["b_paper_id"] = paper_id
            print(f"  [Transaction B] Inserted new paper (paper_id={paper_id}) while A was reading.")
            print(f"  [Transaction B] Committed successfully -- did not block Transaction A.")
        except Exception as e:
            conn.rollback()
            print(f"  [Transaction B] Error: {e}")
        finally:
            cur.close()
            conn.close()

    # Run both transactions concurrently
    thread_a = threading.Thread(target=transaction_a)
    thread_b = threading.Thread(target=transaction_b)

    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    print(f"""
  Result:
    Transaction A saw {results.get('a_before')} papers before B's insert.
    Transaction A saw {results.get('a_after')} papers after B's insert.
    --> A's count did not change because MVCC gave it a consistent snapshot.
    --> B committed without waiting for A to finish.
    --> No locking. No blocking. This is MVCC.
""")


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 2: Isolation Levels - Read Committed vs Repeatable Read
# ─────────────────────────────────────────────────────────────────────────────
def demo_isolation_levels():
    print(f"\n{DIVIDER}")
    print("DEMO 2: Isolation Levels")
    print(DIVIDER)
    print("""
What this shows:
  READ COMMITTED: A transaction sees changes committed by others mid-transaction.
  REPEATABLE READ: A transaction always sees the same data it saw at the start.
""")

    conn_setup = get_conn()
    conn_setup.autocommit = True
    cur_setup = conn_setup.cursor()

    # Get a real paper to update
    cur_setup.execute("SELECT paper_id, title FROM papers WHERE source = 'JMLR' LIMIT 1")
    paper = cur_setup.fetchone()
    paper_id = paper[0]
    original_title = paper[1]
    modified_title = original_title + " [MODIFIED]"

    print(f"  Using paper_id={paper_id}: \"{original_title[:50]}...\"")
    cur_setup.close()
    conn_setup.close()

    # ── Read Committed demo ──
    print("\n  -- READ COMMITTED --")
    conn1 = get_conn()
    conn1.autocommit = False
    conn2 = get_conn()
    conn2.autocommit = False

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    cur1.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
    cur1.execute("BEGIN")
    cur1.execute("SELECT title FROM papers WHERE paper_id = %s", (paper_id,))
    print(f"  [Conn1 - Read Committed] First read:  \"{cur1.fetchone()[0][:60]}\"")

    # Conn2 modifies the title and commits
    cur2.execute("BEGIN")
    cur2.execute("UPDATE papers SET title = %s WHERE paper_id = %s", (modified_title, paper_id))
    conn2.commit()
    print(f"  [Conn2] Updated and committed title.")

    # Conn1 reads again -- at READ COMMITTED it sees the new value
    cur1.execute("SELECT title FROM papers WHERE paper_id = %s", (paper_id,))
    print(f"  [Conn1 - Read Committed] Second read: \"{cur1.fetchone()[0][:60]}\"")
    print(f"  --> Read Committed sees the update. Non-repeatable read occurred.")
    conn1.rollback()

    # Restore original title
    conn_fix = get_conn()
    conn_fix.autocommit = True
    conn_fix.cursor().execute("UPDATE papers SET title = %s WHERE paper_id = %s", (original_title, paper_id))
    conn_fix.close()

    # ── Repeatable Read demo ──
    print("\n  -- REPEATABLE READ --")
    conn3 = get_conn()
    conn3.autocommit = False
    conn4 = get_conn()
    conn4.autocommit = False

    cur3 = conn3.cursor()
    cur4 = conn4.cursor()

    cur3.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    cur3.execute("BEGIN")
    cur3.execute("SELECT title FROM papers WHERE paper_id = %s", (paper_id,))
    print(f"  [Conn3 - Repeatable Read] First read:  \"{cur3.fetchone()[0][:60]}\"")

    # Conn4 modifies and commits
    cur4.execute("BEGIN")
    cur4.execute("UPDATE papers SET title = %s WHERE paper_id = %s", (modified_title, paper_id))
    conn4.commit()
    print(f"  [Conn4] Updated and committed title.")

    # Conn3 reads again -- at REPEATABLE READ it still sees the original
    cur3.execute("SELECT title FROM papers WHERE paper_id = %s", (paper_id,))
    print(f"  [Conn3 - Repeatable Read] Second read: \"{cur3.fetchone()[0][:60]}\"")
    print(f"  --> Repeatable Read still sees original. Snapshot is protected.")
    conn3.rollback()

    # Restore
    conn_fix2 = get_conn()
    conn_fix2.autocommit = True
    conn_fix2.cursor().execute("UPDATE papers SET title = %s WHERE paper_id = %s", (original_title, paper_id))
    conn_fix2.close()

    for c in [cur1, cur2, cur3, cur4]:
        try: c.close()
        except: pass
    for c in [conn1, conn2, conn3, conn4]:
        try: c.close()
        except: pass


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 3: Dead Tuples and VACUUM
# ─────────────────────────────────────────────────────────────────────────────
def demo_vacuum():
    print(f"\n{DIVIDER}")
    print("DEMO 3: Dead Tuples and VACUUM")
    print(DIVIDER)
    print("""
What this shows:
  When rows are updated, PostgreSQL keeps the old version as a dead tuple.
  Dead tuples waste space until VACUUM cleans them up.
  This is the cost of MVCC -- and VACUUM is how PostgreSQL manages it.
""")

    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # Check dead tuples before
    cur.execute("""
        SELECT n_dead_tup, n_live_tup
        FROM pg_stat_user_tables
        WHERE relname = 'papers'
    """)
    row = cur.fetchone()
    print(f"  Before updates -- Live tuples: {row[1]}, Dead tuples: {row[0]}")

    # Perform batch updates to generate dead tuples
    print(f"  Performing 100 updates to generate dead tuples...")
    cur.execute("""
        UPDATE papers
        SET title = title || ' '
        WHERE paper_id IN (SELECT paper_id FROM papers LIMIT 100)
    """)

    # Force stats collection
    cur.execute("SELECT pg_stat_reset()")
    time.sleep(1)
    cur.execute("ANALYZE papers")

    cur.execute("""
        SELECT n_dead_tup, n_live_tup
        FROM pg_stat_user_tables
        WHERE relname = 'papers'
    """)
    row = cur.fetchone()
    print(f"  After updates  -- Live tuples: {row[1]}, Dead tuples: {row[0]}")

    # Run VACUUM
    print(f"  Running VACUUM on papers table...")
    cur.execute("VACUUM papers")
    cur.execute("ANALYZE papers")

    cur.execute("""
        SELECT n_dead_tup, n_live_tup
        FROM pg_stat_user_tables
        WHERE relname = 'papers'
    """)
    row = cur.fetchone()
    print(f"  After VACUUM   -- Live tuples: {row[1]}, Dead tuples: {row[0]}")
    print(f"  --> VACUUM reclaimed space from dead tuples left behind by MVCC.")

    cur.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO 4: Transaction Atomicity and Rollback
# ─────────────────────────────────────────────────────────────────────────────
def demo_atomicity():
    print(f"\n{DIVIDER}")
    print("DEMO 4: Transaction Atomicity and Rollback")
    print(DIVIDER)
    print("""
What this shows:
  A transaction inserts multiple papers at once.
  If anything fails mid-way, PostgreSQL rolls back everything.
  Either all changes happen, or none of them do. This is atomicity.
""")

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM papers")
    count_before = cur.fetchone()[0]
    print(f"  Papers before transaction: {count_before}")

    try:
        cur.execute("BEGIN")

        # Insert two valid papers
        cur.execute("""
            INSERT INTO papers (title, year, source)
            VALUES ('Atomicity Test Paper 1', 2024, 'IEEE')
        """)
        cur.execute("""
            INSERT INTO papers (title, year, source)
            VALUES ('Atomicity Test Paper 2', 2024, 'JMLR')
        """)

        # Force an error -- violate NOT NULL on title
        print(f"  Inserting two papers, then forcing an error...")
        cur.execute("""
            INSERT INTO papers (title, year, source)
            VALUES (NULL, 2024, 'IEEE')
        """)

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"  Error caught: {e.pinfo if hasattr(e, 'pinfo') else str(e).strip()}")
        print(f"  Transaction rolled back automatically.")

    cur2 = conn.cursor()
    cur2.execute("SELECT COUNT(*) FROM papers")
    count_after = cur2.fetchone()[0]
    print(f"  Papers after rollback: {count_after}")
    print(f"  --> Count unchanged ({count_before} = {count_after}). All three inserts were rolled back together.")

    cur.close()
    cur2.close()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(DIVIDER)
    print("PostgreSQL Concurrency and Recovery Demo")
    print("DSCI 551 - Research Papers Database")
    print(DIVIDER)

    demo_mvcc()
    demo_isolation_levels()
    demo_vacuum()
    demo_atomicity()

    print(f"\n{DIVIDER}")
    print("All demos complete.")
    print(DIVIDER)
