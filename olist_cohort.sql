
WITH order_items AS (
    SELECT order_id, COUNT(order_id) AS order_quantity, MIN(shipping_limit_date) AS min_shipping_limit_date
          ,SUM(price) AS purchase_amount, SUM(freight_value) AS sum_freight_value
    FROM ecommerce_by_olist.olist_order_items_dataset 
    GROUP BY order_id
)
-- SELECT * FROM order_items LIMIT 5;
-- SELECT order_id, COUNT(order_id) order_id_cnt FROM order_items GROUP BY order_id HAVING COUNT(order_id) > 1 LIMIT 5;
-- order_id 중목 0

, orders AS (
    SELECT order_id, customer_id, order_status
          ,order_purchase_timestamp, order_approved_at
          ,order_delivered_carrier_date, order_delivered_customer_date
          ,order_estimated_delivery_date
    FROM ecommerce_by_olist.olist_orders_dataset 
--    WHERE order_status = 'delivered'
)
-- SELECT * FROM orders LIMIT 5;
-- SELECT order_id, COUNT(order_id) order_id_cnt FROM orders GROUP BY order_id HAVING COUNT(order_id) > 1 LIMIT 5;
/*
SELECT DISTINCT EXTRACT(YEAR FROM order_delivered_customer_date) order_completed_year, 
       EXTRACT(MONTH FROM order_delivered_customer_date) order_completed_month
FROM orders
ORDER BY order_completed_year, order_completed_month;
*/
-- order_id 중목 0

, customers AS (
    SELECT customer_unique_id, customer_id
    FROM ecommerce_by_olist.olist_customers_dataset
)
-- SELECT * FROM customers LIMIT 5;

, order_payments AS (
    SELECT order_id, SUM(payment_value) AS payment
    FROM ecommerce_by_olist.olist_order_payments_dataset
    GROUP BY order_id
)

, order_rewiews AS (
    SELECT order_id, AVG(review_score) AS avg_review_score
    FROM ecommerce_by_olist.olist_order_reviews_dataset
    GROUP BY order_id
)


--- 작성 다시
          
, order_all AS (
    SELECT o.order_id, o.customer_id, o.order_status
          ,o.order_purchase_timestamp, o.order_approved_at
          ,o.order_delivered_carrier_date, o.order_delivered_customer_date
--          ,EXTRACT(YEAR FROM o.order_delivered_customer_date) order_completed_year, 
--          ,EXTRACT(MONTH FROM o.order_delivered_customer_date) order_completed_month, 
          ,o.order_estimated_delivery_date
          ,c.customer_unique_id
          ,oi.order_quantity, oi.purchase_amount
          ,oi.min_shipping_limit_date, oi.sum_freight_value
          ,op.payment
          ,or_.avg_review_score
    FROM orders AS o
    LEFT JOIN order_items AS oi
    ON o.order_id = oi.order_id
    LEFT JOIN customers AS c
    ON o.customer_id = c.customer_id
    LEFT JOIN order_payments AS op
    ON o.order_id = op.order_id
    LEFT JOIN order_rewiews AS or_
    ON o.order_id = or_.order_id
)


, order_cohort1 AS (
    SELECT *
          ,FIRST_VALUE(order_purchase_timestamp) OVER (PARTITION BY customer_unique_id 
                                                       ORDER BY order_purchase_timestamp) AS first_order_purchase_timestamp   
          ,LAST_VALUE(order_purchase_timestamp) OVER (PARTITION BY customer_unique_id 
                                                       ORDER BY order_purchase_timestamp) AS latest_order_purchase_timestamp    
    FROM order_all
    WHERE CAST(order_purchase_timestamp AS DATE) 
          BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                           INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                              PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 9 DAY)  
          AND PARSE_DATE("%Y%m%d", @DS_END_DATE)
)
-- SELECT *, CAST(first_order_purchase_timestamp AS DATE) AS first_order_purchase_date FROM order_cohort1;
-- SELECT * FROM order_cohort1 
-- WHERE cohort_order_period1 + cohort_order_period2 + cohort_order_period3
--     + cohort_order_period4 + cohort_order_period5 + cohort_order_period6
--     + cohort_order_period7 + cohort_order_period8 + cohort_order_period9 + cohort_order_period10 <> 1;


, order_cohort2 AS (
    SELECT *
          ,CASE WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN PARSE_DATE("%Y%m%d", @DS_START_DATE)
                     AND PARSE_DATE("%Y%m%d", @DS_END_DATE) THEN 'first_order10'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) DAY) THEN 'first_order09'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 2 DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 2 DAY) THEN 'first_order08'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 3 DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 3 DAY) THEN 'first_order07'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 4 DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 4 DAY) THEN 'first_order06'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 5 DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 5 DAY) THEN 'first_order05'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 6 DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 6 DAY) THEN 'first_order04'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 7 DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 7 DAY) THEN 'first_order03'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 8 DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 8 DAY) THEN 'first_order02'
                WHEN CAST(first_order_purchase_timestamp AS DATE) 
                     BETWEEN DATE_SUB(PARSE_DATE("%Y%m%d", @DS_START_DATE), 
                                      INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                         PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 9 DAY) 
                     AND DATE_SUB(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                  INTERVAL DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), 
                                                     PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 9 DAY) THEN 'first_order01'
                ELSE NULL END AS first_order_period
          ,CASE WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) THEN 'Cohort01'
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 2 THEN 'Cohort02'
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 3 THEN 'Cohort03'  
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 4 THEN 'Cohort04'  
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 5 THEN 'Cohort05'  
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 6 THEN 'Cohort06'  
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 7 THEN 'Cohort07'  
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 8 THEN 'Cohort08'  
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 9 THEN 'Cohort09'  
                WHEN DATE_DIFF(CAST(latest_order_purchase_timestamp AS DATE), CAST(first_order_purchase_timestamp AS DATE), DAY)
                     <= DATE_DIFF(PARSE_DATE("%Y%m%d", @DS_END_DATE), PARSE_DATE("%Y%m%d", @DS_START_DATE), DAY) * 10 THEN 'Cohort10' 
                ELSE NULL END AS cohort_order_period
    FROM order_cohort1
)
SELECT * FROM order_cohort2;

