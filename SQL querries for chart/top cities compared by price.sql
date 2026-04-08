SELECT
  "Town" AS "Town",
  MAX("avg_price") AS "MAX(avg_price)"
FROM (
  WITH bounds AS (
    SELECT
      APPROX_QUANTILE_DS("Sale Amount", 0.01) AS lower_bound,
      APPROX_QUANTILE_DS("Sale Amount", 0.99) AS upper_bound
    FROM Real_Estate_Sales
  )
  SELECT
    "Town",
    AVG("Sale Amount") AS avg_price,
    COUNT(*) AS num_sales
  FROM Real_Estate_Sales
  CROSS JOIN bounds
  WHERE
    "Sale Amount" BETWEEN lower_bound AND upper_bound
  GROUP BY
    "Town"
  HAVING
    COUNT(*) >= 10
  ORDER BY
    avg_price DESC
  LIMIT 25
) AS "virtual_table"
GROUP BY
  "Town"
ORDER BY
  "MAX(avg_price)" DESC
LIMIT 250