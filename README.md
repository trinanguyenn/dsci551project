# DSCI 551 Project - Research Paper Search Engine
**Spring 2026 | Group Project**

A research paper search engine built on PostgreSQL, using data from the IEEE and JMLR (Journal of Machine Learning Research) datasets.

---

## Team
| Name | Focus Area |
|------|-----------|
| Stephen Rosario | Concurrency and Recovery |
| Bernard Yu | Storage and Indexing |
| Trina Nguyen | Query Planning and Execution |

---

## Dataset
- **IEEE Research Papers** (~601 papers)
- **JMLR Research Papers** (~2,894 papers)
- **Total**: ~3,495 papers, 7,984 unique authors

---

## Requirements
- PostgreSQL 16 — https://www.postgresql.org/download/
- Python 3.x — https://www.python.org/downloads/
- Python packages: `psycopg2-binary`, `pandas`

Install Python packages by running:
```
py -m pip install psycopg2-binary pandas
```

---

## Setup Instructions

### Step 1: Clone the repository
```
git clone https://github.com/trinanguyenn/dsci551project.git
cd dsci551project
```

### Step 2: Create the database
Open **SQL Shell (psql)** — installed with PostgreSQL. Hit Enter through all prompts until it asks for a password. Enter your PostgreSQL password.

Once you see `postgres=#`, run:
```sql
CREATE DATABASE research_papers;
```

Then connect to it:
```sql
\c research_papers
```

Then run the schema file using the full path to where you cloned the repo. For example:
```sql
\i 'C:/path/to/dsci551project/schema/schema.sql'
```

You should see 6 `CREATE TABLE` and 4 `CREATE INDEX` messages confirming the tables were created.

### Step 3: Load the data
Open a terminal (PowerShell or Command Prompt), navigate to the schema folder, and run:
```
cd schema
py load_data.py
```

You should see output like:
```
Loaded 601 IEEE papers.
Loaded 2894 JMLR papers.
All data loaded successfully.
Total papers:  3495
Total authors: 7984
```

### Step 4: Run the concurrency demo
```
cd demos/stephen
py demo_concurrency.py
```

This will run 4 demonstrations of PostgreSQL concurrency and recovery internals against the real dataset.

---

## Database Connection Settings
All scripts use these default settings:
- **Host**: localhost
- **Port**: 5432
- **Database**: research_papers
- **Username**: postgres
- **Password**: postgres

If your PostgreSQL password is different, update the `DB_CONFIG` section at the top of `load_data.py` and `demo_concurrency.py`.

---

## Project Structure
```
dsci551project/
├── data/                        # Raw CSV datasets
│   ├── IEEE_Research_Data.csv
│   └── Papers_MLResearch_Data.csv
├── schema/                      # Database setup scripts
│   ├── schema.sql               # Creates all tables and indexes
│   └── load_data.py             # Loads CSV data into the database
├── demos/                       # Internal focus area demos
│   ├── stephen/                 # Concurrency and Recovery
│   │   └── demo_concurrency.py
│   ├── bernard/                 # Storage and Indexing
│   └── trina/                   # Query Planning and Execution
├── app/                         # Flask web application (Phase 3)
└── report/                      # Written report sections
```

---

## Database Schema
- `papers` — core paper metadata (title, year, abstract, source)
- `authors` — unique author names
- `paper_authors` — many-to-many: papers to authors
- `keywords` — unique keywords
- `paper_keywords` — many-to-many: papers to keywords
- `citations` — paper-to-paper citation links
