#standardsql

WITH quotes_contacts_revenue AS (
SELECT
        pro_user_id,
        COALESCE(COUNT(q.bid_id),0) AS quotes,
        COALESCE(SUM(CASE WHEN IF(q.pay_per_customer_contact = TRUE, q.first_intentful_ppc_contact_time, q.first_positive_customer_interaction_time) IS NOT NULL THEN 1 ELSE 0 END),0) AS customer_contacts,
        COALESCE(SUM(IF(q.refunded IS TRUE, 1,0)),0) AS refunded,
        COALESCE(SUM(q.revenue),0) AS revenue_all_time,
        COALESCE(SUM(q.revenue_ppq),0) AS revenue_ppq_all_time,
        COALESCE(SUM(q.revenue_ppc),0) AS revenue_ppc_all_time,
        COALESCE(SUM(IF(q.pay_per_customer_contact = FALSE AND q.revenue = 0 AND q.refunded IS FALSE,1,0)),0) AS free_quotes_all_time,
        COALESCE(SUM(IF(q.pay_per_customer_contact = TRUE AND q.first_intentful_ppc_contact_time IS NOT NULL AND q.revenue = 0 AND q.refunded IS FALSE,1,0)),0) AS free_customer_contacts_all_time,

        COALESCE(SUM(IF(DATE(q.sent_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR), q.revenue,0)),0) AS revenue_last_year,
        COALESCE(SUM(IF(DATE(q.sent_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR), q.revenue_ppq,0)),0) AS revenue_ppq_last_year,
        COALESCE(SUM(IF(DATE(q.sent_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR), q.revenue_ppc,0)),0) AS revenue_ppc_last_year,
        COALESCE(SUM(IF(DATE(q.sent_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR) AND q.pay_per_customer_contact = FALSE AND q.revenue = 0 AND q.refunded IS FALSE,1,0)),0) AS free_quotes_last_year,
        COALESCE(SUM(IF(DATE(q.sent_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR) AND q.pay_per_customer_contact = TRUE AND q.first_intentful_ppc_contact_time IS NOT NULL AND q.revenue = 0 AND q.refunded IS FALSE,1,0)),0) AS free_customer_contacts_last_year
        
FROM a.quotes q
GROUP BY 1
)

, support_cases AS (
SELECT 
        c.user_id AS user_id,
        COALESCE(COUNT(IF(first_contact_channel IN ('Phone','Email','Chat','SMS','Directly Question','In-Product'), c.case_number, NULL)),0) AS cases_all_time,
        COALESCE(COUNT(IF(first_contact_channel IN ('Phone'), c.case_number, NULL)),0) AS cases_phone_all_time,
        COALESCE(COUNT(IF(first_contact_channel IN ('Email','Chat','SMS','Directly Question','In-Product'), c.case_number, NULL)),0) AS cases_other_all_time,
        COALESCE(COUNT(IF(first_contact_channel IN ('Phone','Email','Chat','SMS','Directly Question','In-Product') AND DATE(c.created_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR), c.case_number, NULL)),0) AS cases_last_year,
        COALESCE(COUNT(IF(first_contact_channel IN ('Phone') AND DATE(c.created_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR), c.case_number, NULL)),0) AS cases_phone_last_year,
        COALESCE(COUNT(IF(first_contact_channel IN ('Email','Chat','SMS','Directly Question','In-Product') AND DATE(c.created_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR), c.case_number, NULL)),0) AS cases_other_last_year
FROM ops.cases c
WHERE c.auto_response IS FALSE
  AND COALESCE(case_owner_agent_team,"") NOT IN ('Noise')
GROUP BY 1
)

,free_quote_contact_price AS (
SELECT 
      AVG(IF(pay_per_customer_contact IS TRUE AND revenue > 0 AND first_intentful_ppc_contact_time IS NOT NULL, revenue,NULL)) AS avg_ppc_price_all_time,
      AVG(IF(pay_per_customer_contact IS FALSE AND revenue > 0, revenue,NULL)) AS avg_ppq_price_all_time,
      AVG(IF(DATE(sent_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR) AND pay_per_customer_contact IS TRUE AND revenue > 0 AND first_intentful_ppc_contact_time IS NOT NULL, revenue,NULL)) AS avg_ppc_price_last_year,
      AVG(IF(DATE(sent_time) >= DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR) AND pay_per_customer_contact IS FALSE AND revenue > 0, revenue,NULL)) AS avg_ppq_price_last_year,
      60 AS cost_phone_support_contact,
      30 AS cost_other_support_contact
FROM a.quotes
)

, revenue_profitability_cost_data AS (
SELECT 
        p.pro_user_id,
        p.first_service_create_time,
        COALESCE(qcr.quotes,0) AS quotes,
        COALESCE(qcr.customer_contacts,0) AS customer_contacts,
        COALESCE(qcr.refunded,0) AS refunded,
        COALESCE(qcr.revenue_all_time,0) AS revenue_all_time,
        COALESCE(qcr.revenue_ppq_all_time,0) AS revenue_ppq_all_time,
        COALESCE(qcr.revenue_ppc_all_time,0) AS revenue_ppc_all_time,
        COALESCE(qcr.free_quotes_all_time,0) AS free_quotes_all_time,
        COALESCE(qcr.free_customer_contacts_all_time,0) AS free_customer_contacts_all_time,
        fqcp.avg_ppq_price_all_time,
        fqcp.avg_ppc_price_all_time,
        COALESCE((fqcp.avg_ppq_price_all_time * qcr.free_quotes_all_time + fqcp.avg_ppc_price_all_time * qcr.free_customer_contacts_all_time),0) AS est_free_all_time,
        
        COALESCE(qcr.revenue_all_time,0) 
            - COALESCE((fqcp.cost_phone_support_contact * sc.cases_phone_all_time + fqcp.cost_other_support_contact * sc.cases_other_all_time),0)  
          AS profitability_all_time,
        
        COALESCE(qcr.revenue_all_time,0)
            + COALESCE((fqcp.avg_ppq_price_all_time * qcr.free_quotes_all_time + fqcp.avg_ppc_price_all_time * qcr.free_customer_contacts_all_time),0) 
            - COALESCE((fqcp.cost_phone_support_contact * sc.cases_phone_all_time + fqcp.cost_other_support_contact * sc.cases_other_all_time),0) 
          AS profitability_incl_free_all_time,
        
        COALESCE(qcr.revenue_last_year,0) AS revenue_last_year,
        COALESCE(qcr.revenue_ppq_last_year,0) AS revenue_ppq_last_year,
        COALESCE(qcr.revenue_ppc_last_year,0) AS revenue_ppc_last_year,
        COALESCE(qcr.free_quotes_last_year,0) AS free_quotes_last_year,
        COALESCE(qcr.free_customer_contacts_last_year,0) AS free_customer_contacts_last_year,
        fqcp.avg_ppq_price_last_year,
        fqcp.avg_ppc_price_last_year,
        COALESCE((fqcp.avg_ppq_price_last_year * qcr.free_quotes_last_year + fqcp.avg_ppc_price_last_year * qcr.free_customer_contacts_last_year),0) AS est_free_last_year,

        
        COALESCE(qcr.revenue_last_year,0) 
            - COALESCE((fqcp.cost_phone_support_contact * sc.cases_phone_last_year + fqcp.cost_other_support_contact * sc.cases_other_last_year),0)  
          AS profitability_last_year,
        
        COALESCE(qcr.revenue_last_year,0)
            + COALESCE((fqcp.avg_ppq_price_last_year * qcr.free_quotes_last_year + fqcp.avg_ppc_price_last_year * qcr.free_customer_contacts_last_year),0) 
            - COALESCE((fqcp.cost_phone_support_contact * sc.cases_phone_last_year + fqcp.cost_other_support_contact * sc.cases_other_last_year),0) 
          AS profitability_incl_free_last_year,

        COALESCE((fqcp.cost_phone_support_contact * sc.cases_phone_all_time + fqcp.cost_other_support_contact * sc.cases_other_all_time),0) AS cost_all_time,
        COALESCE(sc.cases_all_time,0) AS cases_all_time,
        COALESCE(sc.cases_phone_all_time,0) AS cases_phone_all_time,
        COALESCE(sc.cases_other_all_time,0) AS cases_other_all_time,

        COALESCE((fqcp.cost_phone_support_contact * sc.cases_phone_last_year + fqcp.cost_other_support_contact * sc.cases_other_last_year),0) AS cost_last_year,
        COALESCE(sc.cases_last_year,0) AS cases_last_year,
        COALESCE(sc.cases_phone_last_year,0) AS cases_phone_last_year,
        COALESCE(sc.cases_other_last_year,0) AS cases_other_last_year
        
        
FROM a.pros p
LEFT JOIN quotes_contacts_revenue qcr ON p.pro_user_id = qcr.pro_user_id
LEFT JOIN support_cases sc ON p.pro_user_id = sc.user_id
CROSS JOIN free_quote_contact_price fqcp
)

SELECT 
        prc.pro_user_id,
        CASE WHEN SUBSTR(CAST(prc.pro_user_id AS STRING),-1) IN ('1') 
              AND IF(DATE(prc.first_service_create_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND seg.group_id NOT IN (1,2,3) AND prc.profitability_incl_free_all_time < 0, 1, 0) = 1 
              THEN 1
             WHEN SUBSTR(CAST(prc.pro_user_id AS STRING),-1) IN ('2','3','4') 
              AND IF(DATE(prc.first_service_create_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND seg.group_id NOT IN (1,2,3) AND prc.profitability_incl_free_all_time < 0, 1, 0) = 1
              THEN 2
             WHEN SUBSTR(CAST(prc.pro_user_id AS STRING),-1) IN ('5','6','7','8','9','0') 
              AND IF(DATE(prc.first_service_create_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND seg.group_id NOT IN (1,2,3) AND prc.profitability_incl_free_all_time < 0, 1, 0) = 1
              THEN 3
             ELSE 0
             END AS unprofitable_rollout_wave,
        seg.group_id,
        IF(DATE(prc.first_service_create_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND seg.group_id NOT IN (1,2,3) AND prc.profitability_all_time < 0, 1, 0) AS unprofitable_flag_all_time,
        IF(DATE(prc.first_service_create_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND seg.group_id NOT IN (1,2,3) AND prc.profitability_last_year < 0, 1, 0) AS unprofitable_flag_last_year,
        IF(DATE(prc.first_service_create_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND seg.group_id NOT IN (1,2,3) AND prc.profitability_incl_free_all_time < 0, 1, 0) AS unprofitable_flag_incl_free_all_time,
        IF(DATE(prc.first_service_create_time) < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND seg.group_id NOT IN (1,2,3) AND prc.profitability_incl_free_last_year < 0, 1, 0) AS unprofitable_flag_incl_free_last_year,
        prc.profitability_all_time,
        prc.profitability_last_year,
        prc.profitability_incl_free_all_time,
        prc.profitability_incl_free_last_year,
        prc.revenue_all_time,
        prc.revenue_last_year,
        prc.est_free_all_time,
        prc.est_free_last_year,
        prc.cost_all_time,
        prc.cost_last_year
        
FROM revenue_profitability_cost_data prc
LEFT JOIN ops.pro_user_estimate seg ON prc.pro_user_id = seg.pro_user_id
