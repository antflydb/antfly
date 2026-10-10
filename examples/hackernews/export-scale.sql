-- Run in antfly-dev-01 with maximum_bytes_billed=21474836480.
-- Use a new immutable prefix for every export. This bucket expires after 8 days.
EXPORT DATA OPTIONS (
  uri='gs://colony-import-sources-antfly-dev-01/hn-poc/20261008-scale/archive/part-*.parquet',
  format='PARQUET', compression='SNAPPY', overwrite=false
) AS
SELECT
  id AS hn_id,
  COALESCE(title, '') AS title,
  COALESCE(url, '') AS url,
  COALESCE(text, '') AS text_html,
  REGEXP_REPLACE(CONCAT(COALESCE(title, ''), '\n', COALESCE(text, '')), r'<[^>]*>', ' ') AS body,
  COALESCE(`by`, '') AS author,
  COALESCE(score, 0) AS points,
  time AS created_at,
  type AS item_type,
  COALESCE(parent, 0) AS parent_id,
  COALESCE(descendants, 0) AS comment_count,
  COALESCE(NET.HOST(url), '') AS domain
FROM `bigquery-public-data.hacker_news.full`
WHERE timestamp >= TIMESTAMP('2025-01-01')
  AND timestamp < TIMESTAMP('2025-10-01')
  AND type IN ('story', 'comment')
  AND NOT COALESCE(deleted, false)
  AND NOT COALESCE(dead, false)
ORDER BY id DESC
LIMIT 1000000;
