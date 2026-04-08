SELECT
  "yearRec" AS "yr",
  "Town" AS "Town",
  MAX("sales_volume") AS "MAX(sales_volume)"
FROM (
  WITH top_towns AS (
    SELECT
      "Town"
    FROM "Real_Estate_Sales"
    GROUP BY
      "Town"
    ORDER BY
      COUNT(*) DESC
    LIMIT 5
  )
  SELECT
    r."Town",
    EXTRACT(YEAR FROM r.__time) AS yr,
    COUNT(*) AS sales_volume,
    AVG(r."Sale Amount") AS avg_price
  FROM "Real_Estate_Sales" AS r
  JOIN top_towns AS t
    ON r."Town" = t."Town"
  GROUP BY
    r."Town",
    EXTRACT(YEAR FROM r.__time)
  ORDER BY
    r."Town",
    yr
) AS "virtual_table"
GROUP BY
  "yearRec",
  "Town"
ORDER BY
  "MAX(sales_volume)" DESC
LIMIT 1000