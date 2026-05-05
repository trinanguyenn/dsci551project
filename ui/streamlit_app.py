"""
streamlit_app.py
DSCI 551 Group Project - Spring 2026
Research Paper Search Engine - Web UI

A Streamlit-based frontend for the same research_papers PostgreSQL database
that powers the CLI demo (../app.py). Each page exercises a different access
pattern that maps directly to a database internal:

  Search       -> trigram (GIN) index on papers.title
  Author       -> 3-table join with B-tree indexes on join keys
  Analytics    -> sequential scan + HashAggregate
  Upload       -> multi-table INSERT inside a single transaction (atomicity)

A "Behind the scenes" expander on every page reveals the SQL that ran and the
EXPLAIN plan Postgres chose, so the same internals/application mapping the
CLI demonstrates is also visible in the user-facing app.

Run:
    cd ~/dsci551project
    streamlit run ui/streamlit_app.py

Connection settings honor DATABASE_URL first (Railway / cloud), then fall
back to the same PG* environment variables app.py uses, then to localhost
defaults. Works on Windows, WSL, and Railway with no code changes.
"""

import os
import sys

import psycopg2
import psycopg2.extras
import streamlit as st

# ─── Path setup so we can reuse the project root if needed ──────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
sys.path.insert(0, PROJECT_ROOT)

# ─── Database connection (DATABASE_URL first, then PG* vars, then defaults) ─
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


def get_conn():
    """Open a fresh connection per request."""
    return psycopg2.connect(**DB_KWARGS)


# ─── Page setup ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Research Paper Search Engine | DSCI 551",
    page_icon="📚",
    layout="wide",
)

# Light styling — only what's needed to look professional, no theatre.
st.markdown(
    """
    <style>
      /* Tighten default streamlit spacing */
      .block-container { padding-top: 5rem; padding-bottom: 2rem; }
      /* Nicer dataframe header */
      thead tr th { background-color: #f7f9fb !important; }
      /* Stat bar styling */
      .stat-box {
          background: #f7f9fb;
          padding: 12px 16px;
          border-radius: 6px;
          border-left: 4px solid #065A82;
      }
      .stat-label { color: #64748b; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; }
      .stat-value { color: #065A82; font-size: 24px; font-weight: 700; line-height: 1.2; }
      /* Behind-the-scenes section */
      .bts-header { color: #1C7293; font-weight: 600; font-size: 13px; text-transform: uppercase; letter-spacing: 1px; }
      /* Header styling */
      .app-title { color: #065A82; font-size: 32px; font-weight: 700; margin-bottom: 0; }
      .app-subtitle { color: #64748b; font-size: 14px; margin-top: 0; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─── Header ─────────────────────────────────────────────────────────────────
st.markdown('<p class="app-title">📚 Research Paper Search Engine</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="app-subtitle">DSCI 551 Final Project · Spring 2026 · '
    'Stephen Rosario, Bernard Yu, Trina Nguyen · Powered by PostgreSQL 16</p>',
    unsafe_allow_html=True,
)


# ─── Live stats bar ─────────────────────────────────────────────────────────
def fetch_db_stats():
    """Pull live row counts and DB size to prove the app is wired to a real DB."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM papers")
        papers = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM authors")
        authors = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM paper_authors")
        relationships = cur.fetchone()[0]
        cur.execute("SELECT pg_size_pretty(pg_database_size('research_papers'))")
        size = cur.fetchone()[0]
        cur.close()
        conn.close()
        return papers, authors, relationships, size
    except Exception as e:
        return None, None, None, str(e)


papers_n, authors_n, rel_n, db_size = fetch_db_stats()
if papers_n is None:
    st.error(f"Could not connect to the database: {db_size}")
    st.stop()

c1, c2, c3, c4 = st.columns(4)
for col, label, val in [
    (c1, "Papers",        f"{papers_n:,}"),
    (c2, "Authors",       f"{authors_n:,}"),
    (c3, "Author-paper links", f"{rel_n:,}"),
    (c4, "Database size", db_size),
]:
    with col:
        st.markdown(
            f'<div class="stat-box"><div class="stat-label">{label}</div>'
            f'<div class="stat-value">{val}</div></div>',
            unsafe_allow_html=True,
        )

st.write("")  # spacer


# ─── Sidebar navigation ─────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Navigation")
    section = st.radio(
        "Section",
        ["Browse: Search papers", "Browse: Look up author", "Contribute: Upload paper", "Insights: Analytics"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown("### About")
    st.markdown(
        """
        This UI exercises four real database access patterns:

        - **Search** → trigram (GIN) index
        - **Author lookup** → 3-table join
        - **Upload** → atomic multi-table insert
        - **Analytics** → seq scan + aggregate

        Every page has a **Behind the scenes** section that shows the SQL and the EXPLAIN plan Postgres chose.
        """
    )
    st.markdown("---")
    st.caption("Companion CLI: `python app.py`")


# ─── Helper: show SQL + EXPLAIN ────────────────────────────────────────────
def behind_the_scenes(sql, params, conn):
    """Render the 'Behind the scenes' panel: the SQL we ran + the EXPLAIN plan."""
    st.markdown('<p class="bts-header">Behind the scenes — what Postgres did</p>',
                unsafe_allow_html=True)
    with st.expander("Show SQL and query plan"):
        st.code(sql.strip(), language="sql")
        try:
            cur = conn.cursor()
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}", params)
            plan_lines = [r[0] for r in cur.fetchall()]
            cur.close()
            st.markdown("**EXPLAIN (ANALYZE, BUFFERS):**")
            st.code("\n".join(plan_lines), language="text")
        except Exception as e:
            st.warning(f"Could not run EXPLAIN: {e}")


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — SEARCH PAPERS
# ═══════════════════════════════════════════════════════════════════════════
def render_search():
    st.subheader("Search papers by title")
    st.caption(
        "Substring search across paper titles. Powered by a GIN trigram index "
        "(`pg_trgm`) on `papers.title` so queries like `%neural%` are still indexed."
    )

    col_input, col_btn = st.columns([5, 1])
    with col_input:
        term = st.text_input("Search term", value="learning", label_visibility="collapsed",
                             placeholder="Try: learning, neural, bayesian, optimization")
    with col_btn:
        st.write("")  # vertical alignment
        go = st.button("Search", type="primary", use_container_width=True)

    if not term:
        st.info("Type a search term to see matching papers.")
        return

    pattern = f"%{term}%"
    sql = """
        SELECT paper_id, year, source, title
        FROM papers
        WHERE title ILIKE %s
        ORDER BY year DESC NULLS LAST
        LIMIT 20
    """

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (pattern,))
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) AS n FROM papers WHERE title ILIKE %s", (pattern,))
        total = cur.fetchone()["n"]
        cur.close()
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if total == 0:
        st.warning(f"No papers match '{term}'.")
    else:
        st.success(f"Found **{total:,}** papers matching '**{term}**'. Showing the 20 most recent.")
        # Pretty result list
        for r in rows:
            year = r["year"] if r["year"] is not None else "----"
            with st.container():
                st.markdown(
                    f"**{r['title']}**  \n"
                    f"<span style='color:#64748b;font-size:13px;'>"
                    f"{year} · {r['source']} · paper_id {r['paper_id']}"
                    f"</span>",
                    unsafe_allow_html=True,
                )

    st.write("")
    behind_the_scenes(sql, (pattern,), conn)
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — AUTHOR LOOKUP
# ═══════════════════════════════════════════════════════════════════════════
def render_author():
    st.subheader("Look up an author")
    st.caption(
        "3-table join: `papers` × `paper_authors` × `authors`. "
        "Demonstrates the planner picking a Nested Loop over Index Scans on the join keys."
    )

    col_input, col_btn = st.columns([5, 1])
    with col_input:
        name = st.text_input("Author name", value="Michael I. Jordan",
                             label_visibility="collapsed",
                             placeholder="Try: Michael I. Jordan, Yoshua Bengio")
    with col_btn:
        st.write("")
        go = st.button("Look up", type="primary", use_container_width=True)

    if not name:
        st.info("Type an author name to see their papers.")
        return

    sql = """
        SELECT p.paper_id, p.year, p.source, p.title
        FROM papers p
        JOIN paper_authors pa ON p.paper_id = pa.paper_id
        JOIN authors a        ON pa.author_id = a.author_id
        WHERE a.name = %s
        ORDER BY p.year DESC NULLS LAST
        LIMIT 25
    """

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, (name,))
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if not rows:
        st.warning(f"No papers found for '{name}'. "
                   "Try 'Michael I. Jordan' or 'Yoshua Bengio' — both are in the dataset.")
    else:
        st.success(f"Found **{len(rows)}** papers by **{name}**.")
        for r in rows:
            year = r["year"] if r["year"] is not None else "----"
            st.markdown(
                f"**{r['title']}**  \n"
                f"<span style='color:#64748b;font-size:13px;'>"
                f"{year} · {r['source']} · paper_id {r['paper_id']}"
                f"</span>",
                unsafe_allow_html=True,
            )

    st.write("")
    behind_the_scenes(sql, (name,), conn)
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — UPLOAD A PAPER
# ═══════════════════════════════════════════════════════════════════════════
def render_upload():
    st.subheader("Upload a new paper")
    st.caption(
        "Inserts a row into `papers`, then resolves each comma-separated author "
        "(insert-or-find), then writes one row per author into `paper_authors`. "
        "All three writes happen inside a single transaction — if any step "
        "fails, the entire upload rolls back. This is your atomicity guarantee."
    )

    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        title = st.text_input("Paper title *",
                              placeholder="A Novel Approach to Distributed Database Internals")
    with col2:
        year = st.number_input("Year *", min_value=1900, max_value=2030,
                               value=2025, step=1)
    with col3:
        source = st.text_input("Source *", placeholder="IEEE / JMLR / arXiv / …",
                               value="")

    abstract = st.text_area(
        "Abstract",
        placeholder="Optional. Long abstracts will be TOAST'd by Postgres automatically.",
        height=120,
    )

    authors_raw = st.text_input(
        "Authors (comma-separated) *",
        placeholder="Stephen Rosario, Bernard Yu, Trina Nguyen",
    )

    submitted = st.button("Upload paper", type="primary")

    last_sql = ""        # for the Behind the scenes panel after a successful insert
    last_params = None

    if submitted:
        # Validate
        if not title.strip():
            st.error("Title is required.")
            return
        if not source.strip():
            st.error("Source is required.")
            return
        if not authors_raw.strip():
            st.error("At least one author is required.")
            return

        author_names = [a.strip() for a in authors_raw.split(",") if a.strip()]
        if not author_names:
            st.error("Could not parse any author names.")
            return

        # The whole upload runs in ONE transaction. If any step throws, we
        # rollback so partial data never lands. This is the atomicity story
        # from Action 13 in the CLI demo, now wired to the UI.
        conn = None
        try:
            conn = get_conn()
            conn.autocommit = False
            cur = conn.cursor()

            # 1. Insert paper
            cur.execute(
                """
                INSERT INTO papers (title, abstract, year, source)
                VALUES (%s, %s, %s, %s)
                RETURNING paper_id
                """,
                (title.strip(), abstract.strip() or None, int(year), source.strip()),
            )
            new_paper_id = cur.fetchone()[0]

            # 2. For each author: find existing or insert new
            author_ids = []
            for an in author_names:
                cur.execute("SELECT author_id FROM authors WHERE name = %s", (an,))
                row = cur.fetchone()
                if row:
                    author_ids.append(row[0])
                else:
                    cur.execute(
                        "INSERT INTO authors (name) VALUES (%s) RETURNING author_id",
                        (an,),
                    )
                    author_ids.append(cur.fetchone()[0])

            # 3. Wire each author to the new paper
            for aid in author_ids:
                cur.execute(
                    "INSERT INTO paper_authors (paper_id, author_id) VALUES (%s, %s)",
                    (new_paper_id, aid),
                )

            conn.commit()
            cur.close()

            st.success(
                f"✅ Uploaded **{title.strip()}** as paper_id **{new_paper_id}** "
                f"with **{len(author_ids)}** author(s). "
                f"All inserts committed atomically."
            )
            last_sql = (
                "BEGIN;\n"
                "INSERT INTO papers (title, abstract, year, source) VALUES (%s, %s, %s, %s) RETURNING paper_id;\n"
                "-- for each author:\n"
                "SELECT author_id FROM authors WHERE name = %s;\n"
                "-- if missing:\n"
                "INSERT INTO authors (name) VALUES (%s) RETURNING author_id;\n"
                "-- then for each resolved author_id:\n"
                "INSERT INTO paper_authors (paper_id, author_id) VALUES (%s, %s);\n"
                "COMMIT;"
            )
        except Exception as e:
            if conn:
                conn.rollback()
            st.error(
                f"❌ Upload failed and **was rolled back**. "
                f"No partial data was committed. Error: {e}"
            )
            last_sql = ""
        finally:
            if conn:
                conn.close()

        if last_sql:
            st.markdown('<p class="bts-header">Behind the scenes — atomicity in action</p>',
                        unsafe_allow_html=True)
            with st.expander("Show the transaction"):
                st.code(last_sql, language="sql")
                st.markdown(
                    "**Why it matters:** these three INSERTs span three tables. "
                    "Wrapping them in `BEGIN ... COMMIT` means Postgres treats them "
                    "as a single unit. The write-ahead log records each statement "
                    "before the heap is touched, so a crash mid-upload still leaves "
                    "the database consistent."
                )


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════
def render_analytics():
    st.subheader("Analytics — papers per year per source")
    st.caption(
        "Group-by aggregation across the entire `papers` table. "
        "Postgres picks Sequential Scan + HashAggregate because nearly every row "
        "contributes to the result — index scans wouldn't help."
    )

    sql = """
        SELECT year, source, COUNT(*) AS papers
        FROM papers
        WHERE year IS NOT NULL
        GROUP BY year, source
        ORDER BY year DESC, source
        LIMIT 30
    """

    try:
        conn = get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    if not rows:
        st.warning("No data.")
        return

    # Render as a clean table
    import pandas as pd
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Tiny chart for visual lift
    chart_df = df.pivot_table(index="year", columns="source", values="papers", fill_value=0)
    chart_df = chart_df.sort_index()
    st.bar_chart(chart_df, height=240)

    st.write("")
    behind_the_scenes(sql, None, conn)
    conn.close()


# ─── Route to the chosen section ────────────────────────────────────────────
if section.startswith("Browse: Search"):
    render_search()
elif section.startswith("Browse: Look up"):
    render_author()
elif section.startswith("Contribute"):
    render_upload()
elif section.startswith("Insights"):
    render_analytics()
