SELECT
  category,
  COUNT(*) AS total_orders,
  SUM(quantity) AS total_quantity,
  ROUND(SUM(total_amount), 2) AS total_sales
FROM `qwiklabs-gcp-04-fdff43c11093.retail_data.orders`
GROUP BY category
ORDER BY total_sales DESC;
