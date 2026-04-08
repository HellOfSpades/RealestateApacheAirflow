SELECT
  "yr" AS "yr",
  MAX("num_sales") AS "MAX(num_sales)"
FROM (
  SELECT
    COUNT(*) AS num_sales,
    EXTRACT(YEAR FROM __time) AS yr
  FROM Real_Estate_Sales
  GROUP BY
    EXTRACT(YEAR FROM __time)
  ORDER BY
    yr ASC
) AS "virtual_table"
GROUP BY
  "yr"
ORDER BY
  "MAX(num_sales)" DESC
LIMIT 1000