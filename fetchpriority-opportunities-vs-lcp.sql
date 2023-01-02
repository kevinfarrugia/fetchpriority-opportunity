#standardSQL
# Correlation between opportunity_ms and LCP render time
#
# Notes:
# - LCP is an image
# - LCP image is served over HTTP/2 or HTTP/3
# - opportunity_ms is the difference between time the resource is discovered and time the resource is requested
# - there is at least one render-blocking script
# - there are two or more render-blocking assets

WITH lcp_image_elements AS (
  SELECT
    device,
    page,
    lcp_url,
    lcp_render_time,
    number_of_render_blocking_scripts,
    number_of_render_blocking_stylesheets
  FROM (
    SELECT
      _TABLE_SUFFIX AS device,
      url AS page,
      JSON_EXTRACT_SCALAR(payload, '$._performance.lcp_elem_stats.renderTime') AS lcp_render_time,
      JSON_EXTRACT_SCALAR(payload, '$._performance.lcp_elem_stats.url') AS lcp_url,
      JSON_EXTRACT_SCALAR(payload, '$._performance.lcp_elem_stats.nodeName') AS lcp_node_name,
      CAST(JSON_EXTRACT_SCALAR(payload, '$._renderBlockingJS') AS BIGNUMERIC) AS number_of_render_blocking_scripts,
      CAST(JSON_EXTRACT_SCALAR(payload, '$._renderBlockingCSS') AS BIGNUMERIC) AS number_of_render_blocking_stylesheets
    FROM
      `httparchive.pages.2022_10_01_*` TABLESAMPLE SYSTEM (100 PERCENT)
  )
  WHERE
    lcp_node_name = 'IMG'
),

lcp_requests AS (
  SELECT
    _TABLE_SUFFIX AS device,
    page AS page,
    url AS lcp_url,
    JSON_EXTRACT_SCALAR(payload, '$._initial_priority') AS initial_priority,
    JSON_EXTRACT_SCALAR(payload, '$._protocol') AS protocol,
    CAST(JSON_EXTRACT(payload, '$._created') AS BIGNUMERIC) AS discovered,
    CAST(JSON_EXTRACT(payload, '$._load_start') AS BIGNUMERIC) AS request_start,
  FROM
    `httparchive.requests.2022_10_01_*` TABLESAMPLE SYSTEM (100 PERCENT)
)

SELECT
  device,
  opportunity_ms,
  COUNT(0) AS num_pages,
  SUM(COUNT(0)) OVER (PARTITION BY device) AS total,
  COUNT(0) / SUM(COUNT(0)) OVER (PARTITION BY device) AS pct,
  APPROX_QUANTILES(lcp_render_time, 1000)[OFFSET(500)] AS median_lcp
FROM (
  SELECT
    device,
    initial_priority,
    lcp_render_time,
    CASE
      WHEN opportunity_ms <= 100 THEN 100
      WHEN opportunity_ms <= 200 THEN 200
      WHEN opportunity_ms <= 300 THEN 300
      WHEN opportunity_ms <= 400 THEN 400
      WHEN opportunity_ms <= 500 THEN 500
      WHEN opportunity_ms <= 600 THEN 600
      WHEN opportunity_ms <= 700 THEN 700
      WHEN opportunity_ms <= 800 THEN 800
      WHEN opportunity_ms <= 900 THEN 900
      WHEN opportunity_ms <= 1000 THEN 1000
      ELSE 1100
    END AS opportunity_ms
  FROM (
    SELECT
      device,
      initial_priority,
      IF(lcp_render_time IS NULL, 0, CAST(lcp_render_time AS BIGNUMERIC)) AS lcp_render_time,
      number_of_render_blocking_scripts,
      number_of_render_blocking_stylesheets,
      protocol,
      (request_start - discovered) AS opportunity_ms
    FROM
      lcp_image_elements
    INNER JOIN
      lcp_requests
    USING (
      page, lcp_url, device
    )
  )
  WHERE
    lcp_render_time > 0 AND
    number_of_render_blocking_scripts > 0 AND
    number_of_render_blocking_scripts + number_of_render_blocking_stylesheets > 1 AND (
      LOWER(protocol) = 'http/2' OR
      LOWER(protocol) = 'http/3' OR
      LOWER(protocol) = 'h3'
    )
)
GROUP BY
  device,
  opportunity_ms
ORDER BY
  device,
  opportunity_ms
