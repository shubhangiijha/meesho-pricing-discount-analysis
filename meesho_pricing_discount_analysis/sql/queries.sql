-- GMV and orders by discount bracket
SELECT 
  CASE 
    WHEN discount_percent = 0 THEN '0%'
    WHEN discount_percent BETWEEN 1 AND 10 THEN '1-10%'
    WHEN discount_percent BETWEEN 11 AND 20 THEN '11-20%'
    WHEN discount_percent BETWEEN 21 AND 30 THEN '21-30%'
    ELSE '31-50%'
  END AS discount_bracket,
  COUNT(*) AS orders,
  SUM(final_price) AS gmv,
  AVG(final_price) AS aov
FROM transactions
GROUP BY 1
ORDER BY 1;

-- Campaign-wise revenue and average discount
SELECT c.campaign_id, c.campaign_type, c.start_date, c.end_date,
       COUNT(t.order_id) AS orders,
       ROUND(SUM(t.final_price), 2) AS gmv,
       ROUND(AVG(t.discount_percent), 2) AS avg_discount_pct
FROM campaigns c
LEFT JOIN transactions t ON c.campaign_id = t.campaign_id
GROUP BY 1,2,3,4
ORDER BY gmv DESC;

-- Top categories by GMV within 11-20% discount bracket
SELECT category, COUNT(*) AS orders, ROUND(SUM(final_price), 2) AS gmv
FROM transactions
WHERE discount_percent BETWEEN 11 AND 20
GROUP BY category
ORDER BY gmv DESC;
