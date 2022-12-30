#standardSQL
# Distribution of opportunity_ms
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
    JSON_EXTRACT_SCALAR(payload, '$._protocol') AS protocol,
    CAST(JSON_EXTRACT(payload, '$._created') AS BIGNUMERIC) AS discovered,
    CAST(JSON_EXTRACT(payload, '$._load_start') AS BIGNUMERIC) AS request_start,
  FROM
    `httparchive.requests.2022_10_01_*` TABLESAMPLE SYSTEM (100 PERCENT)
)

SELECT
  device,
  percentile,
  APPROX_QUANTILES(opportunity_ms, 1000)[OFFSET(percentile * 10)] AS opportunity_ms,
  SUM(COUNT(0)) OVER (PARTITION BY device) AS total
FROM (
  SELECT
    device,
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
    number_of_render_blocking_scripts > 0 AND
    number_of_render_blocking_scripts + number_of_render_blocking_stylesheets > 1 AND (
      LOWER(protocol) = 'http/2' OR
      LOWER(protocol) = 'http/3' OR
      LOWER(protocol) = 'h3'
    )
), 
UNNEST([10, 25, 50, 75, 90]) AS percentile
GROUP BY
  device,
  percentile
ORDER BY
  device,
  percentile
