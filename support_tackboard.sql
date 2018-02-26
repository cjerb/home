#standardsql

WITH visitors AS (
SELECT
  DATE_ADD(DATE_TRUNC(DATE_SUB(visit_date, INTERVAL 1 DAY), WEEK), INTERVAL 1 DAY) AS week,
  COUNT(DISTINCT visitor_pk)/10000 AS visitors
FROM `tt-dp-prod.a.customer_visits`
WHERE ip_sessions < 100
  AND _PARTITIONTIME >= '2017-01-01'
GROUP BY 1 
ORDER BY 1 DESC
)

, requests AS (
SELECT 
  DATE_ADD(DATE_TRUNC(DATE_SUB(DATE(create_time), INTERVAL 1 DAY), WEEK), INTERVAL 1 DAY) AS week,
  COUNT(request_id) AS requests
FROM `tt-dp-prod.a.requests` 
WHERE create_time >= '2017-01-01'
GROUP BY 1
ORDER BY 1 DESC
)

, contacts AS (
SELECT
  DATE_ADD(DATE_TRUNC(DATE_SUB(DATE(first_customer_contact_time), INTERVAL 1 DAY), WEEK), INTERVAL 1 DAY) AS week,
  COUNT(bid_id) AS contacts
FROM `tt-dp-prod.a.contacts` 
WHERE first_customer_contact_time >= '2017-01-01'
GROUP BY 1
ORDER BY 1 DESC
)

, cases AS (
SELECT   
        DATE_ADD(DATE_TRUNC(DATE_SUB(DATE(created_time),INTERVAL 1 DAY),WEEK), INTERVAL 1 DAY) AS week,
        COUNT(1) AS cases,
        COUNT(IF(first_contact_channel = 'Phone',1,NULL)) AS cases_phone,
        COUNT(IF(first_contact_channel = 'Chat',1,NULL)) AS cases_chat,
        COUNT(IF(first_contact_channel = 'SMS',1,NULL)) AS cases_sms,
        COUNT(IF(first_contact_channel = 'Email' AND auto_response IS FALSE,1,NULL)) AS cases_email,
        COUNT(IF(first_contact_channel = 'Email' AND auto_response IS TRUE,1,NULL)) AS cases_auto_response,
        COUNT(IF(first_contact_channel = 'Directly Question',1,NULL)) AS cases_directly,
        COUNT(IF(first_contact_channel = 'In-Product',1,NULL)) AS cases_in_product

FROM `tt-dp-prod.ops.cases`
WHERE COALESCE(case_category,' ') NOT IN ('Marketplace Integrity') 
      AND first_contact_channel IN ('Chat','Email','Phone','SMS','Directly Question','In-Product')
      AND created_time >= '2017-01-01'
GROUP BY 1
ORDER BY 1 DESC
)

,ltc AS (
SELECT   
        DATE_ADD(DATE_TRUNC(DATE_SUB(DATE(csat_response_time),INTERVAL 1 DAY),WEEK), INTERVAL 1 DAY) AS week,
        COUNT(likely_continue_using_score) AS ltc_surveys,
        SAFE_DIVIDE(COUNT(IF(likely_continue_using_score IN (4,5),1,NULL)) , COUNT(likely_continue_using_score)) AS ltc,
        SAFE_DIVIDE(COUNT(IF(first_contact_channel = 'Phone' AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(first_contact_channel = 'Phone', likely_continue_using_score,NULL))) AS ltc_phone,
        SAFE_DIVIDE(COUNT(IF(first_contact_channel = 'Chat' AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(first_contact_channel = 'Chat', likely_continue_using_score,NULL))) AS ltc_chat,
        SAFE_DIVIDE(COUNT(IF(first_contact_channel = 'SMS' AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(first_contact_channel = 'SMS', likely_continue_using_score,NULL))) AS ltc_sms,
        SAFE_DIVIDE(COUNT(IF(first_contact_channel = 'Email' AND auto_response IS FALSE AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(first_contact_channel = 'Email' AND auto_response IS FALSE, likely_continue_using_score,NULL))) AS ltc_email,
        SAFE_DIVIDE(COUNT(IF(first_contact_channel = 'Directly Question' AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(first_contact_channel = 'Directly Question', likely_continue_using_score,NULL))) AS ltc_directly,
        SAFE_DIVIDE(COUNT(IF(first_contact_channel = 'In-Product' AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(first_contact_channel = 'In-Product', likely_continue_using_score,NULL))) AS ltc_in_product,
        
        SAFE_DIVIDE(COUNT(IF(pro_segment_group_id = 1 AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(pro_segment_group_id = 1, likely_continue_using_score,NULL))) AS ltc_strategic,
        SAFE_DIVIDE(COUNT(IF(pro_segment_group_id = 2 AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(pro_segment_group_id = 2, likely_continue_using_score,NULL))) AS ltc_hvp_revenue,
        SAFE_DIVIDE(COUNT(IF(pro_segment_group_id = 3 AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(pro_segment_group_id = 3, likely_continue_using_score,NULL))) AS ltc_hvp_potential,
        SAFE_DIVIDE(COUNT(IF(pro_segment_group_id = 4 AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(pro_segment_group_id = 4, likely_continue_using_score,NULL))) AS ltc_occasional,
        SAFE_DIVIDE(COUNT(IF(pro_segment_group_id IS NULL AND likely_continue_using_score IN (4,5),1,NULL)) , COUNT(IF(pro_segment_group_id IS NULL , likely_continue_using_score,NULL))) AS ltc_unknown

FROM `tt-dp-prod.ops.cases`
WHERE COALESCE(case_category,' ') NOT IN ('Marketplace Integrity') 
      AND COALESCE(first_contact_channel,'') IN ('Chat','Email','Phone','SMS','Directly Question','In-Product')
      AND csat_response_time >= '2017-06-01'
GROUP BY 1
ORDER BY 1 DESC
)

,tsl_agg AS (
SELECT 
        IF(manager_name = "IBEX","IBEX","TSL Phone") AS team_name,
        index_date,
        SUM(closed_cases) AS closed_cases,
        SUM(total_lops_time - lunch_time)/60 AS hours_worked
FROM `tt-dp-prod.reference.jerb_tsl_scorecard_dataset` 
WHERE emp_lvl = "1"
  AND email_address NOT IN ('jerb@thumbtack.com','stholen@thumbtack.com')
GROUP BY 1,2
ORDER BY 1,2
)


, tph_agg AS (
SELECT 
        team_name AS team_name,
        index_date,
        SUM(closed_cases) AS closed_cases,
        SUM(total_omni_time - lunch_time)/3600 AS hours_worked
FROM `tt-dp-prod.reference.jerb_tph_scorecard_dataset` 
WHERE email_address NOT IN ('jerb@thumbtack.com','stholen@thumbtack.com')
GROUP BY 1,2
ORDER BY 1,2
)

,combined AS (
SELECT * FROM tsl_agg
UNION ALL SELECT * FROM tph_agg
ORDER BY team_name, index_date
)

,cases_per_hour AS (
SELECT
        DATE_TRUNC(index_date,WEEK(MONDAY)) AS week,
        SAFE_DIVIDE(SUM(closed_cases), SUM(hours_worked)) AS cph,
        SAFE_DIVIDE(SUM(IF(team_name = 'TSL Phone',closed_cases,0)), SUM(IF(team_name = 'TSL Phone',hours_worked,0))) AS cph_tsl,
        SAFE_DIVIDE(SUM(IF(team_name = 'IBEX',closed_cases,0)), SUM(IF(team_name = 'IBEX',hours_worked,0))) AS cph_ibex,
        SAFE_DIVIDE(SUM(IF(team_name IN ('TPH Chat','TPH SMS','TPH Email'),closed_cases,0)), SUM(IF(team_name IN ('TPH Chat','TPH SMS','TPH Email'),hours_worked,0))) AS cph_tph,
        
        COALESCE(SUM(closed_cases),0) AS cases_total_team,
        SUM(IF(team_name = 'TSL Phone',closed_cases,0)) AS cases_tsl,
        SUM(IF(team_name = 'IBEX',closed_cases,0)) AS cases_ibex,
        SUM(IF(team_name IN ('TPH Chat','TPH SMS','TPH Email'),closed_cases,0)) AS cases_tph

FROM combined
GROUP BY 1
ORDER BY 1 DESC
)

SELECT 
        v.week,
        v.visitors,
        r.requests,
        cc.contacts,
        c.cases,
        c.cases_phone,
        c.cases_chat,
        c.cases_sms,
        c.cases_email,
        c.cases_auto_response,
        c.cases_directly,
        c.cases_in_product,
        
        cph.cases_total_team,
        cph.cases_tsl,
        cph.cases_ibex,
        cph.cases_tph,
        
        l.ltc_surveys,
        l.ltc,
        l.ltc_phone,
        l.ltc_chat,
        l.ltc_sms,
        l.ltc_email,
        l.ltc_directly,
        l.ltc_in_product,
        l.ltc_strategic,
        l.ltc_hvp_revenue,
        l.ltc_hvp_potential,
        l.ltc_occasional,
        l.ltc_unknown,
        
        cph.cph,
        cph.cph_tsl,
        cph.cph_ibex,
        cph.cph_tph,
        
        COALESCE(sc.tsl_costs,0) + COALESCE(sc.ibex_costs,0) + COALESCE(sc.tph_costs,0) AS total_costs,
        sc.tsl_costs,
        sc.ibex_costs,
        sc.tph_costs
        
FROM visitors v
LEFT JOIN requests r ON v.week = r.week
LEFT JOIN contacts cc ON v.week = cc.week
LEFT JOIN cases c ON v.week = c.week
LEFT JOIN ltc l ON v.week = l.week
LEFT JOIN cases_per_hour cph ON v.week = cph.week
LEFT JOIN `tt-dp-prod.sandbox.jerb_support_costs` sc ON v.week = sc.week
WHERE v.week >= '2017-01-01' AND v.week < DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
ORDER BY 1
