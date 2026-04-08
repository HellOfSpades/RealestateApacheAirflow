SELECT
  "yearRec" AS "yearRec",
  "Town" AS "Town",
  SUM("total_value") AS "SUM(total_value)"
FROM (
  WITH top_towns AS (
    SELECT
      "Town"
    FROM "Real_Estate_Sales"
    GROUP BY
      "Town"
    ORDER BY
      SUM("Sale Amount") DESC
    LIMIT 5
  )
  SELECT
    r."Town",
    EXTRACT(YEAR FROM r.__time) AS yearRec,
    SUM(r."Sale Amount") AS total_value
  FROM "Real_Estate_Sales" AS r
  JOIN top_towns AS t
    ON r."Town" = t."Town"
  GROUP BY
    r."Town",
    EXTRACT(YEAR FROM r.__time)
  ORDER BY
    r."Town",
    yearRec
) AS "virtual_table"
GROUP BY
  "yearRec",
  "Town"
ORDER BY
  "SUM(total_value)" DESC
LIMIT 1000