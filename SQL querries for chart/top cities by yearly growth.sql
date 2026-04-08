SELECT
  "yearRec" AS "yearRec",
  "Town" AS "Town",
  SUM("yoy_growth") AS "SUM(yoy_growth)"
FROM (
  WITH yearly_prices AS (
    SELECT
      "Town",
      EXTRACT(YEAR FROM __time) AS yr,
      AVG("Sale Amount") AS avg_price,
      COUNT(*) AS cnt
    FROM "Real_Estate_Sales"
    GROUP BY
      "Town",
      EXTRACT(YEAR FROM __time)
    HAVING
      COUNT(*) >= 20
  ), growth AS (
    SELECT
      "Town",
      yr AS yearRec,
      avg_price,
      LAG(avg_price) OVER (PARTITION BY "Town" ORDER BY yr) AS prev_avg_price
    FROM yearly_prices
  ), yoy AS (
    SELECT
      "Town",
      yearRec,
      (
        avg_price - prev_avg_price
      ) / prev_avg_price AS yoy_growth
    FROM growth
    WHERE
      NOT prev_avg_price IS NULL
  ), top_towns AS (
    SELECT
      "Town"
    FROM yoy
    WHERE
      "Town" <> 'Willington' --this line can be removed to include Willingotn in search, however it's an extreme outlier
    GROUP BY
      "Town"
    ORDER BY
      AVG(yoy_growth) DESC
    LIMIT 5
  )
  SELECT
    y."Town",
    y.yearRec,
    y.yoy_growth
  FROM yoy AS y
  JOIN top_towns AS t
    ON y."Town" = t."Town"
  ORDER BY
    y."Town",
    y.yearRec
) AS "virtual_table"
GROUP BY
  "yearRec",
  "Town"
ORDER BY
  "SUM(yoy_growth)" DESC
LIMIT 1000