#standardSQL
# Correlation between render-blocking stylesheets and opportunity_ms
#
# Notes:
# - LCP is an image
# - LCP image is served over HTTP/2 or HTTP/3
# - opportunity_ms is the difference between time the resource is discovered and time the resource is requested
# - there is at least one render-blocking script

WITH lcp_image_elements AS (
  SELECT
    device,
    page,
    lcp_url,
    number_of_render_blocking_scripts,
    number_of_render_blocking_stylesheets
  FROM (
    SELECT
      _TABLE_SUFFIX AS device,
      url AS page,
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
  number_of_render_blocking_stylesheets,
  APPROX_QUANTILES(opportunity_ms, 1000)[OFFSET(500)] AS p50_opportunity_ms,
  COUNT(0) AS num_pages
FROM (
  SELECT
    device,
    IF(
      number_of_render_blocking_stylesheets >= 20,
      20,
      number_of_render_blocking_stylesheets
    ) AS number_of_render_blocking_stylesheets,
    opportunity_ms
  FROM (
    SELECT
      device,
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
    number_of_render_blocking_scripts > 0 AND (
      LOWER(protocol) = 'http/2' OR
      LOWER(protocol) = 'http/3' OR
      LOWER(protocol) = 'h3'
    )
)
GROUP BY
  device,
  number_of_render_blocking_stylesheets
ORDER BY
  device,
  number_of_render_blocking_stylesheets
