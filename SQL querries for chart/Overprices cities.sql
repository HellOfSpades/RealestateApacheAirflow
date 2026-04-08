SELECT
  "Town" AS "Town",
  MAX("avg_sales_ratio") AS "MAX(avg_sales_ratio)"
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
    COUNT(*) >= 10 AND AVG("Sales Ratio") > 0.796 --this value is 3rd quantile point that was calculated earlier
  ORDER BY
    avg_sales_ratio DESC
) AS "virtual_table"
GROUP BY
  "Town"
ORDER BY
  "MAX(avg_sales_ratio)" DESC
LIMIT 10