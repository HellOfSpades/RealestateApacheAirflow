Apache Superset
Dashboards
Charts
Datasets
SQL
Development
Settings

Dima's Dashboard
Dima's Dashboard
Draft
Superset Admin
2 days ago
Cities in Connecticut in overprices area (above third qunatile)
Cities in the undervalues area (below first qunatile)
Top 5 cities by sales volume over time
Top cities by sales value over time
Top yearly growth over time (Willington excluded)
Top 5 cities by growth with Willington
Top town compared by prices
Transactions volume over time
Correlation between unemployment and sales over time
Correaltion between sales and unemployment rate
Cities ranked

View query
SELECT
  "yearRec" AS "yearRec",
  MAX("unemployment_ct") AS "MAX(unemployment_ct)",
  MAX("median_sales_ratio") AS "MAX(median_sales_ratio)",
  MAX("transaction_count") AS "MAX(transaction_count)"
FROM (
  SELECT
    TIME_FORMAT(TIME_FLOOR(r.__time, 'P1Y'), 'yyyy') AS yearRec,
    AVG(u.UNEMPLOYCT) AS unemployment_ct,
    AVG(d.high) AS avg_debt_high,
    AVG(r."Sales Ratio") AS median_sales_ratio,
    COUNT(*) AS transaction_count
  FROM Real_Estate_Sales AS r
  LEFT JOIN Unemployement AS u
    ON TIME_FLOOR(r.__time, 'P1Y') = TIME_FLOOR(u.__time, 'P1Y')
  LEFT JOIN household_debt AS d
    ON TIME_FLOOR(r.__time, 'P1Y') = TIME_FLOOR(d.__time, 'P1Y')
  WHERE
    r.__time BETWEEN CAST('2000-01-01' AS TIMESTAMP) AND CURRENT_TIMESTAMP
    AND r."Sales Ratio" BETWEEN 0.5 AND 2.0
  GROUP BY
    1
  ORDER BY
    yearRec
) AS "virtual_table"
GROUP BY
  "yearRec"
LIMIT 1000