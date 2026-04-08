SELECT
  "Town" AS "Town",
  MAX("investment_score") AS "MAX(investment_score)"
FROM (
  WITH yearly AS (
    SELECT
      "Town",
      EXTRACT(YEAR FROM __time) AS yr,
      APPROX_QUANTILE_DS("Sale Amount", 0.5) AS median_price,
      COUNT(*) AS sales_volume,
      AVG("Sales Ratio") AS avg_ratio
    FROM "Real_Estate_Sales"
    WHERE
      __time >= CAST('2018-01-01' AS TIMESTAMP) AND NOT "Sale Amount" IS NULL
    GROUP BY
      1,
      2
  ), max_year AS (
    SELECT
      MAX(yr) AS max_yr
    FROM yearly
  ), pivot AS (
    SELECT
      y."Town",
      MAX(CASE WHEN y.yr = m.max_yr THEN median_price END) AS latest_price,
      MAX(CASE WHEN y.yr = m.max_yr - 5 THEN median_price END) AS past_price,
      AVG(sales_volume) AS avg_volume,
      AVG(avg_ratio) AS avg_ratio
    FROM yearly AS y
    CROSS JOIN max_year AS m
    GROUP BY
      y."Town"
  ), metrics AS (
    SELECT
      "Town",
      (
        (
          latest_price - past_price
        ) / NULLIF(past_price, 0)
      ) /* adjust growth by volume */ * avg_volume AS adj_growth,
      avg_volume,
      LN(1 + avg_ratio) AS log_ratio /* changed from LOG1P to LN(1 + x) */
    FROM pivot
    WHERE
      NOT past_price IS NULL
  ), normalized AS (
    SELECT
      "Town",
      (
        adj_growth - MIN(adj_growth) OVER ()
      ) / NULLIF(MAX(adj_growth) OVER () - MIN(adj_growth) OVER (), 0) AS norm_growth,
      (
        avg_volume - MIN(avg_volume) OVER ()
      ) / NULLIF(MAX(avg_volume) OVER () - MIN(avg_volume) OVER (), 0) AS norm_volume,
      (
        log_ratio - MIN(log_ratio) OVER ()
      ) / NULLIF(MAX(log_ratio) OVER () - MIN(log_ratio) OVER (), 0) AS norm_ratio
    FROM metrics
  )
  SELECT
    "Town",
    norm_growth,
    norm_volume,
    norm_ratio,
    ROUND((
      norm_growth * 0.5 + norm_volume * 0.3 + norm_ratio * 0.2
    ), 2) AS investment_score /* balanced investment score */
  FROM normalized
  ORDER BY
    investment_score DESC
  LIMIT 20
) AS "virtual_table"
GROUP BY
  "Town"
ORDER BY
  "MAX(investment_score)" DESC
LIMIT 1000