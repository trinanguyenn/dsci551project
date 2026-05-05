# DSCI 551 Project — Research Paper Search Engine

**Spring 2026 | Group Project**

A research paper search engine built on PostgreSQL, using the IEEE and JMLR (Journal of Machine Learning Research) Kaggle datasets. The application supports paper search, author lookup, paper upload, and analytics, and it deliberately exposes the database internals that power each action — heap storage, B-tree and trigram indexes, query planning, MVCC, VACUUM, and WAL-backed atomicity. Every menu action prints a mapping from what the application did → what PostgreSQL did internally → why that internal behavior matters.

---

## Live Demo

A live, deployed instance of the web UI is available here:

**https://dsci551project-production.up.railway.app/**

The live instance runs on Railway with a managed PostgreSQL service. No local setup is required to try it — the live database is preloaded with the same IEEE + JMLR data described below.

---

## Team

| Name             | Focus Area                           |
| ---------------- | ------------------------------------ |
| Stephen Rosario  | Concurrency and Recovery             |
| Bernard Yu       | Storage and Indexing                 |
| Trina Nguyen     | Query Planning and Execution         |

---

## Three Ways to Run This Project

| Mode                  | Audience                                   | Command                              |
| --------------------- | ------------------------------------------ | ------------------------------------ |
| Live web UI           | Anyone — no install required               | Visit the Railway URL above          |
| Local CLI             | Graders / TAs verifying internals mapping  | `python app.py`                      |
| Local web UI          | Run the same Streamlit app on your machine | `streamlit run ui/streamlit_app.py`  |

The CLI and the web UI are two front-ends to the same database and the same internals story:

- The **CLI** is a 14-action menu that walks through every focus area (storage, query planning, concurrency) and prints `EXPLAIN ANALYZE` output and an internals-to-application mapping for each action. This is the most direct way to see the internals work.
- The **Web UI** wraps four user-facing actions — search, author lookup, paper upload, and analytics — with a "Behind the scenes" expander on each page that surfaces the same SQL and `EXPLAIN` output.

---

## Dataset

- **IEEE Research Papers** — 601 papers
- **JMLR Research Papers** — 2,894 papers
- **Total**: 3,495 papers, 7,984 unique authors, 11,430 paper–author links

Source: two Kaggle datasets (CSV files in `data/`).

---

## Requirements

- **PostgreSQL 16+** — https://www.postgresql.org/download/
- **Python 3.10+** — https://www.python.org/downloads/
- **Python packages**: see `requirements.txt` (`psycopg2-binary`, `pandas`, `streamlit`)

We recommend a Python virtual environment (see Step 3 below).

---

## Local Setup

### Step 1 — Clone the repository

```bash
git clone https://github.com/trinanguyenn/dsci551project.git
cd dsci551project
```

### Step 2 — Create the database

Open **SQL Shell (psql)** — installed with PostgreSQL — and authenticate as the `postgres` user.

Once you see the `postgres=#` prompt, run:

```sql
CREATE DATABASE research_papers;
\c research_papers
\i 'C:/path/to/dsci551project/schema/schema.sql'
```

Adjust the path in `\i` to match where you cloned the repo. You should see six `CREATE TABLE` and four `CREATE INDEX` messages confirming the schema is in place.

### Step 3 — Set up a Python environment and install dependencies

From the project root:

```bash
python -m venv venv
```

Activate the virtual environment:

- **Windows (PowerShell)**: `venv\Scripts\Activate.ps1`
- **macOS / Linux / WSL**: `source venv/bin/activate`

Then install everything in one shot:

```bash
pip install -r requirements.txt
```

### Step 4 — Load the data

From the project root, with the virtual environment active:

```bash
python schema/load_data.py
```

Expected output:

```
Reading CSV files...
  Reading IEEE data ...   Found 601 valid IEEE rows.
  Reading JMLR data ...   Found 2894 valid JMLR rows.

Total records to load: 3495
Loading data...
  Inserted 3495 papers.
  Resolved 7984 unique author IDs.
  Inserted 11430 paper-author links.

All data loaded successfully.
```

The loader uses batched inserts (`psycopg2.extras.execute_values`) and finishes in roughly 30 seconds against a local database, or 30–60 seconds against a remote Postgres.

---

## Step 5a — Run the CLI

```bash
python app.py
```

This launches the interactive 14-action menu. Each menu item runs a real search-engine action, then prints `EXPLAIN ANALYZE` output for the underlying SQL — letting you see exactly what PostgreSQL did internally.

To run all demos non-interactively (useful for grading):

```bash
python app.py --demo all
```

Or run a single focus area's demos:

```bash
python app.py --demo storage      # Bernard's section
python app.py --demo query        # Trina's section
python app.py --demo concurrency  # Stephen's section
```

You can also run any focus-area script directly:

```bash
python demos/bernard/demo_storage_indexing.py
python demos/trina/demo_query_planning.py
python demos/stephen/demo_concurrency.py
```

---

## Step 5b — Run the Web UI Locally

```bash
streamlit run ui/streamlit_app.py
```

Streamlit will print a URL (typically http://localhost:8501). Open it in any browser. The four pages are:

| Page                         | Internal Mechanism Demonstrated                                  |
| ---------------------------- | ---------------------------------------------------------------- |
| Browse: Search papers        | GIN trigram index on `papers.title` (handles `LIKE '%substr%'`)  |
| Browse: Look up author       | Three-table join with B-tree indexes on join keys                |
| Contribute: Upload paper     | Multi-table INSERT inside one transaction (atomicity / rollback) |
| Insights: Analytics          | Sequential scan + `HashAggregate`                                |

Every page has a **"Behind the scenes"** expander that shows the SQL and the `EXPLAIN (ANALYZE, BUFFERS)` plan PostgreSQL chose for that exact request — making the same internals/application mapping the CLI demonstrates visible inside the user-facing app.

---

## Database Connection Settings

All scripts read connection settings from environment variables in this priority order:

1. `DATABASE_URL` — used by Railway and Heroku-style providers
2. `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` — individual settings
3. Localhost defaults (host `localhost`, db `research_papers`, user `postgres`, password `postgres`)

If your local PostgreSQL password is different from `postgres`, set `PGPASSWORD` in your shell before running:

- **Windows (PowerShell)**: `$env:PGPASSWORD = "your_password"`
- **macOS / Linux / WSL**: `export PGPASSWORD=your_password`

Then run the app or the loader as above.

---

## Application Features

### CLI menu (14 actions)

**Storage & Indexing — Bernard**

1. Search papers by year (index vs sequential scan)
2. Heap storage + ctid (8 KB heap pages)
3. B-tree internals (`pgstatindex` depth and density)
4. Index creation impact (build cost vs read speedup)
5. TOAST for long abstracts (oversized values)

**Query Planning & Execution — Trina**

6. Search papers by title (trigram index)
7. Look up an author (3-table join planning)
8. Analytics aggregation (`HashAggregate`)
9. Query planner walk-through (planner re-plans on the same SQL when an index appears)

**Concurrency & Recovery — Stephen**

10. Insert paper while reading (MVCC snapshot)
11. Bulk update + VACUUM (dead tuples)
12. Isolation levels in action (Read Committed vs Repeatable Read)
13. Atomic transaction + rollback (WAL atomicity)

**Ops**

14. Health check / row counts

### Web UI pages

- **Browse: Search papers** — substring title search via the trigram index
- **Browse: Look up author** — every paper by a given author
- **Contribute: Upload paper** — adds a paper, resolves authors, and writes paper-author links inside a single transaction
- **Insights: Analytics** — papers per year per source, plus a bar chart

---

## Project Structure

```
dsci551project/
├── app.py                       # Unified CLI menu (14 actions)
├── requirements.txt             # Python dependencies
├── Procfile                     # Process spec for Railway / Heroku-style hosts
├── runtime.txt                  # Pinned Python version for cloud deploys
├── README.md
├── .gitignore
├── .streamlit/
│   └── config.toml              # Light-mode theme + brand color
├── data/                        # Raw CSV datasets
│   ├── IEEE_Research_Data.csv
│   └── Papers_MLResearch_Data.csv
├── schema/
│   ├── schema.sql               # Creates all tables and indexes
│   └── load_data.py             # Batched data loader
├── demos/
│   ├── bernard/
│   │   └── demo_storage_indexing.py
│   ├── trina/
│   │   └── demo_query_planning.py
│   └── stephen/
│       └── demo_concurrency.py
└── ui/
    └── streamlit_app.py         # Streamlit web UI
```

---

## Database Schema

Tables:

- `papers` — core paper metadata (title, abstract, year, pages, link, code_link, source)
- `authors` — unique author names
- `paper_authors` — many-to-many: papers ↔ authors
- `keywords` — unique keywords (modeled but not populated in the current load)
- `paper_keywords` — many-to-many: papers ↔ keywords (not populated)
- `citations` — paper-to-paper citation links (not populated)

Indexes:

- `idx_papers_year`, `idx_papers_source` — B-tree, used by year/source filters
- `idx_authors_name` — B-tree, used by author lookup
- `idx_keywords_keyword` — B-tree, present for completeness
- `idx_papers_title` — GIN trigram index, created on demand by the trigram demo and the title-search action
- `idx_paper_authors_paper_id`, `idx_paper_authors_author_id` — created on demand by the join-planning demo

The schema lives in `schema/schema.sql` and is re-runnable: it drops existing tables before recreating them.
