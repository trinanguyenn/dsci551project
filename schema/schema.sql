-- ============================================================
-- research_papers database schema
-- DSCI 551 Group Project - Spring 2026
-- ============================================================

-- Drop tables if they already exist (for clean re-runs)
DROP TABLE IF EXISTS paper_keywords CASCADE;
DROP TABLE IF EXISTS paper_authors CASCADE;
DROP TABLE IF EXISTS citations CASCADE;
DROP TABLE IF EXISTS keywords CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS papers CASCADE;

-- ------------------------------------------------------------
-- Core table: one row per research paper
-- ------------------------------------------------------------
CREATE TABLE papers (
    paper_id    SERIAL PRIMARY KEY,
    title       TEXT NOT NULL,
    abstract    TEXT,
    year        INT,
    pages       INT,
    link        TEXT,
    code_link   TEXT,
    source      TEXT NOT NULL  -- 'IEEE' or 'JMLR'
);

-- ------------------------------------------------------------
-- Authors: one row per unique author name
-- ------------------------------------------------------------
CREATE TABLE authors (
    author_id   SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE
);

-- ------------------------------------------------------------
-- Paper-Author relationship: links papers to their authors
-- One paper can have many authors, one author can have many papers
-- ------------------------------------------------------------
CREATE TABLE paper_authors (
    paper_id    INT REFERENCES papers(paper_id) ON DELETE CASCADE,
    author_id   INT REFERENCES authors(author_id) ON DELETE CASCADE,
    PRIMARY KEY (paper_id, author_id)
);

-- ------------------------------------------------------------
-- Citations: links a paper to another paper it cites
-- ------------------------------------------------------------
CREATE TABLE citations (
    citing_paper_id  INT REFERENCES papers(paper_id) ON DELETE CASCADE,
    cited_paper_id   INT REFERENCES papers(paper_id) ON DELETE CASCADE,
    PRIMARY KEY (citing_paper_id, cited_paper_id)
);

-- ------------------------------------------------------------
-- Keywords: one row per unique keyword
-- ------------------------------------------------------------
CREATE TABLE keywords (
    keyword_id  SERIAL PRIMARY KEY,
    keyword     TEXT NOT NULL UNIQUE
);

-- ------------------------------------------------------------
-- Paper-Keyword relationship: links papers to their keywords
-- ------------------------------------------------------------
CREATE TABLE paper_keywords (
    paper_id    INT REFERENCES papers(paper_id) ON DELETE CASCADE,
    keyword_id  INT REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    PRIMARY KEY (paper_id, keyword_id)
);

-- ------------------------------------------------------------
-- Indexes to support fast lookups (Bernard's section)
-- ------------------------------------------------------------
CREATE INDEX idx_papers_year   ON papers(year);
CREATE INDEX idx_papers_source ON papers(source);
CREATE INDEX idx_authors_name  ON authors(name);
CREATE INDEX idx_keywords_keyword ON keywords(keyword);
