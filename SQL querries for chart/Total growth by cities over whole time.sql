WITH yearly_prices AS (
  SELECT
    "Town",
    EXTRACT(YEAR FROM __time) AS yr,
    AVG("Sale Amount") AS avg_price,
    COUNT(*) AS cnt
  FROM "Real_Estate_Sales"
  GROUP BY "Town", EXTRACT(YEAR FROM __time)
  HAVING COUNT(*) >= 20
),

first_last AS (
  SELECT
    y1."Town",
    y1.avg_price AS first_price,
    y2.avg_price AS last_price
  FROM (
    SELECT "Town", yr, avg_price
    FROM yearly_prices
  ) y1
  JOIN (
    SELECT "Town", yr, avg_price
    FROM yearly_prices
  ) y2
    ON y1."Town" = y2."Town"
  WHERE y1.yr = (SELECT MIN(yr) FROM yearly_prices yp WHERE yp."Town" = y1."Town")
    AND y2.yr = (SELECT MAX(yr) FROM yearly_prices yp WHERE yp."Town" = y2."Town")
),

top_towns AS (
  SELECT "Town"
  FROM first_last
  WHERE "Town" <> 'Willington'
  ORDER BY (last_price - first_price) / first_price DESC
)

SELECT
  f."Town",
  (f.last_price - f.first_price) / f.first_price AS total_growth
FROM first_last f
JOIN top_towns t
  ON f."Town" = t."Town"
ORDER BY total_growth DESC;