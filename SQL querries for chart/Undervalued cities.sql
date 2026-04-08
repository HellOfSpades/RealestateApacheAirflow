SELECT
  "Town" AS "Town",
  MIN("avg_sales_ratio") AS "MIN(avg_sales_ratio)"
FROM (
  SELECT
    "Town",
    AVG("Sales Ratio") AS avg_sales_ratio,
    COUNT(*) AS num_sales
  FROM Real_Estate_Sales
  WHERE
    "Sales Ratio" BETWEEN 0.5 AND 2
  GROUP BY
    "Town"
  HAVING
    COUNT(*) >= 10 AND AVG("Sales Ratio") < 0.733 --this value is 1s quantile point which was calculated earlier
  ORDER BY
    avg_sales_ratio ASC
  LIMIT 10
) AS "virtual_table"
GROUP BY
  "Town"
ORDER BY
  "MIN(avg_sales_ratio)" DESC
LIMIT 10