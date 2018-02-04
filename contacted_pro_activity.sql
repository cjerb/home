#standardsql
WITH index_dates AS (
  SELECT DATE_TRUNC(index_date,MONTH) AS index_month
  FROM UNNEST(GENERATE_DATE_ARRAY('2015-12-01',CURRENT_DATE())) AS index_date
  GROUP BY 1
  ORDER BY 1
)

, agg_contacts AS (
SELECT 
        DATE_TRUNC(DATE(c.first_customer_contact_time), MONTH) AS month,
        c.pro_user_id,
        IF(em.enabled_timestamp IS NOT NULL,1,0) AS im_enabled_market,
        COUNT(c.first_customer_contact_time) AS contacts,
        SUM(c.revenue) AS revenue
FROM `tt-dp-prod.a.contacts` c
LEFT JOIN `tt-dp-prod.b.pa_enabled_markets` em ON em.category_id = c.category_id AND em.cbsa_id = c.cbsa_id
WHERE request_create_time >= '2015-12-01'
GROUP BY 1,2,3
ORDER BY pro_user_id, month, im_enabled_market
)

, first_contact AS (
SELECT DISTINCT
        c.pro_user_id,
        IF(em.enabled_timestamp IS NOT NULL,1,0) AS im_enabled_market,
        MIN(DATE_TRUNC(DATE(c.first_customer_contact_time),MONTH)) OVER (PARTITION BY c.pro_user_id, IF(em.enabled_timestamp IS NOT NULL,1,0)) AS first_contact_month
FROM `tt-dp-prod.a.contacts` c
LEFT JOIN `tt-dp-prod.b.pa_enabled_markets` em ON em.category_id = c.category_id AND em.cbsa_id = c.cbsa_id
ORDER BY pro_user_id, im_enabled_market
)

SELECT 
        i.index_month AS month,
        p.pro_user_id AS pro_user_id,
        p.im_enabled_market,
        COALESCE(ac.contacts,0) AS contacts,
        COALESCE(ac.revenue,0) AS revenue,
        COALESCE(SUM(ac.contacts) OVER (PARTITION BY p.pro_user_id, p.im_enabled_market ORDER BY i.index_month),0) AS running_contacts,
        COALESCE(SUM(ac.contacts) OVER (PARTITION BY p.pro_user_id, p.im_enabled_market),0) AS total_contacts,
        COALESCE(LAG(ac.contacts,1) OVER (PARTITION BY p.pro_user_id, p.im_enabled_market ORDER BY i.index_month),0) AS contacts_prev_month,
        IF(i.index_month <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 1 MONTH), COALESCE(SUM(ac.contacts) OVER (PARTITION BY p.pro_user_id, p.im_enabled_market ORDER BY i.index_month ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING),0), NULL) AS contacts_next_1months,
        IF(i.index_month <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 2 MONTH), COALESCE(SUM(ac.contacts) OVER (PARTITION BY p.pro_user_id, p.im_enabled_market ORDER BY i.index_month ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING),0), NULL) AS contacts_next_2months,
        IF(i.index_month <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(),MONTH),INTERVAL 5 MONTH), COALESCE(SUM(ac.contacts) OVER (PARTITION BY p.pro_user_id, p.im_enabled_market ORDER BY i.index_month ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING),0), NULL) AS contacts_next_5months,
        fc.first_contact_month
FROM index_dates i
CROSS JOIN (SELECT DISTINCT pro_user_id, im_enabled_market FROM agg_contacts) p
LEFT JOIN agg_contacts ac ON i.index_month = ac.month AND p.pro_user_id = ac.pro_user_id AND p.im_enabled_market = ac.im_enabled_market
LEFT JOIN first_contact fc ON fc.pro_user_id = p.pro_user_id AND fc.im_enabled_market = p.im_enabled_market
-- ORDER BY p.pro_user_id, i.index_month, p.im_enabled_market
