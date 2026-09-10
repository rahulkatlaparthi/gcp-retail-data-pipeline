SELECT
  COUNT(*) AS total_orders,
  COUNTIF(total_amount IS NULL) AS null_total_amount,
  COUNTIF(quantity <= 0) AS invalid_quantity,
  COUNTIF(price < 0) AS invalid_price
FROM `qwiklabs-gcp-04-fdff43c11093.retail_data.orders`;
