# DSCI 551 Project — Research Paper Search Engine

**Spring 2026 | Group Project**

A research paper search engine built on PostgreSQL, using data from the IEEE and JMLR (Journal of Machine Learning Research) Kaggle datasets.

Every action in the application is also a live demonstration of a PostgreSQL internal mechanism — heap storage, B-tree indexes, trigram indexes, query planning, MVCC, VACUUM, and WAL-backed atomicity — with a printed mapping from application behavior to database internals.

---

## Team

| Name            | Focus Area                      |
|-----------------|---------------------------------|
| Stephen Rosario | Concurrency and Recovery        |
| Bernard Yu      | Storage and Indexing            |
| Trina Nguyen    | Query Planning and Execution    |

---

## Dataset

- **IEEE Research Papers** — 560 papers
- **JMLR Research Papers** — 2,881 papers
- **Total**: 3,441 papers, 7,650 unique authors, 11,023 paper–author links

Source: two Kaggle datasets (see `data/`).

---

## Requirements

- PostgreSQL 16 — https://www.postgresql.org/download/
- Python 3.10+ — https://www.python.org/downloads/
- Python packages: `psycopg2-binary`, `pandas`

We recommend installing Python packages in a virtual environment (see Step 3).

---

## Setup Instructions

### Step 1: Clone the repository

```
git clone https://github.com/trinanguyenn/dsci551project.git
cd dsci551project
```

### Step 2: Create the database

Open **SQL Shell (psql)** — installed with PostgreSQL. Press Enter through the prompts until it asks for a password; enter the password you chose during PostgreSQL installation.

Once you see `postgres=#`, run:

```sql
CREATE DATABASE research_papers;
\c research_papers
\i 'C:/path/to/dsci551project/schema/schema.sql'
```

Adjust the path in `\i` to match where you cloned the repo. You should see six `CREATE TABLE` and four `CREATE INDEX` messages confirming the schema is in place.

### Step 3: Set up a Python environment and install dependencies

From the project root:

```
python -m venv venv
```

Activate the virtual environment:

- **Windows (PowerShell)**: `venv\Scripts\Activate.ps1`
- **macOS / Linux / WSL**: `source venv/bin/activate`

Then install the dependencies:

```
pip install -r requirements.txt
pip install pandas
```

(`pandas` is only needed for the data loader in Step 4.)

### Step 4: Load the data

From the project root, with the virtual environment active:

```
cd schema
python load_data.py
cd ..
```

Expected output:

```
Loaded 560 IEEE papers.
Loaded 2881 JMLR papers.
All data loaded successfully.
Total papers:  3441
Total authors: 7650
```

### Step 5: Run the application

```
python app.py
```

This launches the interactive menu. Each menu item runs a real search-engine action and then prints what PostgreSQL did internally and why it matters.

To run all demos non-interactively (useful for grading):

```
python app.py --demo all
```

You can also run each focus area's demo bundle individually:

```
python app.py --demo storage
python app.py --demo query
python app.py --demo concurrency
```

Or run any individual focus-area script on its own:

```
python demos/bernard/demo_storage_indexing.py
python demos/trina/demo_query_planning.py
python demos/stephen/demo_concurrency.py
```

---

## Database Connection Settings

All scripts read connection settings from environment variables, falling back to these defaults:

| Setting    | Env var        | Default           |
|------------|----------------|-------------------|
| Host       | `PGHOST`       | `localhost`       |
| Port       | `PGPORT`       | `5432`            |
| Database   | `PGDATABASE`   | `research_papers` |
| User       | `PGUSER`       | `postgres`        |
| Password   | `PGPASSWORD`   | `postgres`        |

If your PostgreSQL password is different, set `PGPASSWORD` in your shell before running the app. For example, on Windows PowerShell:

```
$env:PGPASSWORD = "your_password"
python app.py
```

Or on macOS / Linux / WSL:

```
export PGPASSWORD=your_password
python app.py
```

---

## Application Features

The menu exposes 14 actions, organized by focus area:

**Storage & Indexing (Bernard)**
1. Search papers by year (index vs seq scan)
2. Heap storage + ctid (8 KB heap pages)
3. B-tree internals (pgstatindex depth)
4. Index creation impact (build cost vs speedup)
5. TOAST for long abstracts (oversized values)

**Query Planning & Execution (Trina)**
6. Search papers by title (trigram index)
7. Look up an author (join planning)
8. Analytics aggregation (HashAggregate)
9. Query planner walk-through (planner re-plans)

**Concurrency & Recovery (Stephen)**
10. Insert paper while reading (MVCC snapshot)
11. Bulk update + VACUUM (dead tuples)
12. Isolation levels in action (RC vs RR)
13. Atomic transaction + rollback (WAL atomicity)

**Ops**
14. Health check / row counts

---

## Project Structure

```
dsci551project/
├── app.py                       # Unified CLI application
├── requirements.txt             # Python dependencies
├── README.md
├── data/                        # Raw CSV datasets
│   ├── IEEE_Research_Data.csv
│   └── Papers_MLResearch_Data.csv
├── schema/
│   ├── schema.sql               # Creates all tables and indexes
│   └── load_data.py             # Loads CSV data into the database
└── demos/
    ├── bernard/
    │   └── demo_storage_indexing.py
    ├── trina/
    │   └── demo_query_planning.py
    └── stephen/
        └── demo_concurrency.py
```

---

## Database Schema

- `papers` — core paper metadata (title, year, abstract, source, link, code_link, pages)
- `authors` — unique author names
- `paper_authors` — many-to-many: papers to authors
- `keywords` — unique keywords (not populated in the current load)
- `paper_keywords` — many-to-many: papers to keywords (not populated)
- `citations` — paper-to-paper citation links (not populated)

Indexes: `idx_papers_year`, `idx_papers_source`, `idx_authors_name`, `idx_keywords_keyword`, plus the trigram index `idx_papers_title` and two join-key indexes on `paper_authors` that are created on demand by the demos.