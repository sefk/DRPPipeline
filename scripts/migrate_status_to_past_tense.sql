-- Migrate projects.status from present/verb form to past tense.
-- Run against your DRP pipeline database (e.g. data_cms_gov.db).
-- Usage: sqlite3 your_db.db < migrate_status_to_past_tense.sql

-- sourcing -> sourced
UPDATE projects SET status = 'sourced' WHERE status = 'sourcing';

-- collector hold - {reason} -> collector_hold - {reason}
UPDATE projects SET status = REPLACE(status, 'collector hold - ', 'collector_hold - ')
  WHERE status LIKE 'collector hold - %';
-- collector -> collected
UPDATE projects SET status = 'collected' WHERE status = 'collector';

-- upload -> uploaded
UPDATE projects SET status = 'uploaded' WHERE status = 'upload';

-- publisher -> published
UPDATE projects SET status = 'published' WHERE status = 'publisher';

-- Error -> error (case normalization)
UPDATE projects SET status = 'error' WHERE status = 'Error';

-- Failed -> failed (if present from tests)
UPDATE projects SET status = 'failed' WHERE status = 'Failed';

-- not_found, no_links, dupe_in_DL, updated_* unchanged (already past tense or descriptive)
