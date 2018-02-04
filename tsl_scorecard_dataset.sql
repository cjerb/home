WITH agents AS (

SELECT 
        CONCAT(preferred_first_name," ",preferred_last_name) AS specialist_name,
        email_address,
        CAST(liveops_id AS INT64) AS liveops,
        tt_admin_id,
        salesforce_id_18,
        manager_name,
        PARSE_DATE('%Y-%m-%d',team_start_date) AS date_team_start,
        PARSE_DATE('%Y-%m-%d',team_end_date) AS date_team_end,
        emp_lvl,
        team_name
FROM `tt-dp-prod.reference.tsl_ops_roster`
WHERE team_id IN ('6','8','11','16')
ORDER BY specialist_name, date_team_start
)

, closed_cases AS (
SELECT 
        a.specialist_name,
        a.liveops,
        a.tt_admin_id,
        a.salesforce_id_18,
        a.manager_name,
        a.date_team_start,
        a.date_team_end,
        a.emp_lvl,
        a.team_name,
        DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", c.closed_time_mt)) AS closed_date_mt,
        COUNT(c.closed_time_mt) AS closed_cases,
        COUNT(c.csat_offered_time) AS pushed_surveys
        
FROM agents a 
JOIN `tt-dp-prod.ops.cases` c ON a.salesforce_id_18 = c.case_owner_agent_id AND DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", closed_time_mt)) BETWEEN a.date_team_start AND a.date_team_end
WHERE (DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", c.created_time_mt)) >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY) OR  DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", c.closed_time_mt)) >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY))
  AND first_contact_channel IN ('Phone','Email','Chat','SMS','Directly Question','In-Product')
  AND auto_response IS FALSE
  AND duplicate_case_id IS NULL
  AND COALESCE(case_category,'') NOT IN ('Marketplace Integrity')
  AND c.closed_time_mt IS NOT NULL

GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,10
)

, lops_time AS (
SELECT 
        a.specialist_name,
        a.liveops,
        a.tt_admin_id,
        a.salesforce_id_18,
        a.manager_name,
        a.date_team_start,
        a.date_team_end,
        a.emp_lvl,
        a.team_name,
        EXTRACT(DATE FROM sl.event_time AT TIME ZONE "America/Denver") AS event_date_mt,
        SUM(sl.total_time) AS total_lops_time,
        SUM(IF(time_bucket IN ('Break'), sl.total_time,0)) AS break_time,
        SUM(IF(time_bucket IN ('Chat'), sl.total_time,0)) AS chat_time,
        SUM(IF(time_bucket IN ('Coaching'), sl.total_time,0)) AS coaching_time,
        SUM(IF(time_bucket IN ('Hold'), sl.total_time,0)) AS hold_time,
        SUM(IF(time_bucket IN ('Idle'), sl.total_time,0)) AS idle_time,
        SUM(IF(time_bucket IN ('Idle Dialout'), sl.total_time,0)) AS idle_dialout_time,
        SUM(IF(time_bucket IN ('Lunch'), sl.total_time,0)) AS lunch_time,
        SUM(IF(time_bucket IN ('Meeting'), sl.total_time,0)) AS meeting_time,
        SUM(IF(time_bucket IN ('Other - Unavailable'), sl.total_time,0)) AS other_unavailable_time,
        SUM(IF(time_bucket IN ('Special_Projects'), sl.total_time,0)) AS special_projects_time,
        SUM(IF(time_bucket IN ('Talk'), sl.total_time,0)) AS talk_time,
        SUM(IF(time_bucket IN ('Training'), sl.total_time,0)) AS training_time,
        SUM(IF(time_bucket IN ('Wrap'), sl.total_time,0)) AS wrap_time
       
FROM agents a 
JOIN 
  (SELECT 
          *,
             CASE WHEN agent_presence_state = 'BUSY' AND agent_state_modifier = 'None' AND unavailable_reason IS NULL THEN 'Talk'
               WHEN agent_state_modifier IN ('WrapUp','Paused','WrapUp+Hold','WrapUp+Pinned') OR unavailable_reason = 'Logged On as Unavailable' THEN 'Wrap'
               WHEN agent_state_modifier IN ('Hold','Hold+Pinned') THEN 'Hold'
               WHEN agent_presence_state IN ('IDLE') AND work_type IN ('Inbound') THEN 'Idle'
               WHEN agent_presence_state IN ('IDLE') AND work_type IN ('Dial-out','Outbound') THEN 'Idle Dialout'
               WHEN agent_presence_state IN ('BUSY') AND agent_state_modifier IN ('Pinned') THEN 'Idle Dialout'
               WHEN agent_presence_state IN ('NOTREADY') AND unavailable_reason IN ('Last Call','Terminating Duplicate Agent Connection','System: Presence Server Recycle','No Phone Connection','Set Offline by Supervisor') THEN 'Other - Unavailable'
               WHEN agent_presence_state IN ('OFFLINE','SIGNON') AND unavailable_reason IS NULL THEN 'Idle Dialout'
               WHEN unavailable_reason IN ('Break','Meeting','Coaching','Training','Chat','Special_Projects','Lunch') THEN unavailable_reason
               ELSE 'Other - Unavailable'
          END AS time_bucket
   FROM `tt-dp-prod.ops.agent_status_log` 
   WHERE EXTRACT(DATE FROM event_time AT TIME ZONE "America/Denver") >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY)
  
  ) sl ON a.liveops = sl.agent_id AND EXTRACT(DATE FROM event_time AT TIME ZONE "America/Denver") BETWEEN a.date_team_start AND a.date_team_end
  
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,10
)

, csat_ltc AS (
SELECT 
        a.specialist_name,
        a.liveops,
        a.tt_admin_id,
        a.salesforce_id_18,
        a.manager_name,
        a.date_team_start,
        a.date_team_end,
        a.emp_lvl,
        a.team_name,
        DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", c.csat_response_time_mt)) AS csat_response_date_mt,
        COUNT(c.csat_score) AS csat_responses,
        COALESCE(SUM(IF(c.csat_score IN (4,5),1,NULL)),0) AS csat_top2,
        COALESCE(SUM(IF(c.csat_score IN (5),1,NULL)),0) AS csat_5,
        COALESCE(SUM(IF(c.csat_score IN (4),1,NULL)),0) AS csat_4,
        COALESCE(SUM(IF(c.csat_score IN (3),1,NULL)),0) AS csat_3,
        COALESCE(SUM(IF(c.csat_score IN (2),1,NULL)),0) AS csat_2,
        COALESCE(SUM(IF(c.csat_score IN (1),1,NULL)),0) AS csat_1,
        COUNT(c.likely_continue_using_score) AS ltc_responses,
        COALESCE(SUM(IF(c.likely_continue_using_score IN (4,5),1,NULL)),0) AS ltc_top2,
        COALESCE(SUM(IF(c.likely_continue_using_score IN (5),1,NULL)),0) AS ltc_5,
        COALESCE(SUM(IF(c.likely_continue_using_score IN (4),1,NULL)),0) AS ltc_4,
        COALESCE(SUM(IF(c.likely_continue_using_score IN (3),1,NULL)),0) AS ltc_3,
        COALESCE(SUM(IF(c.likely_continue_using_score IN (2),1,NULL)),0) AS ltc_2,
        COALESCE(SUM(IF(c.likely_continue_using_score IN (1),1,NULL)),0) AS ltc_1
FROM agents a 
JOIN `tt-dp-prod.ops.csat` c ON a.salesforce_id_18 = c.csat_owner_id AND DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", csat_response_time_mt)) BETWEEN a.date_team_start AND a.date_team_end
WHERE (DATE(c.csat_created_time) >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY)
  OR  DATE(c.csat_response_time) >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY))
  AND COALESCE(c.csat_case_origin,'') NOT IN ('In-Product')
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,10
)

, refunds_grants AS (
SELECT 
        a.specialist_name,
        a.liveops,
        a.tt_admin_id,
        a.salesforce_id_18,
        a.manager_name,
        a.date_team_start,
        a.date_team_end,
        a.emp_lvl,
        a.team_name,
        EXTRACT(DATE FROM TIMESTAMP_MILLIS(c.cre_timestamp) AT TIME ZONE "America/Denver") AS cre_date_mt,
        COUNT(IF(cre_transaction_type = 3,cre_credit_log_id,NULL)) AS num_refunds,
        SUM(IF(cre_transaction_type = 3, cre_adjustment_paid_cents + cre_adjustment_promotional_cents,0)) AS refunds_amount,
        COUNT(IF(cre_transaction_type = 4,cre_credit_log_id,NULL)) AS num_grants,
        SUM(IF(cre_transaction_type = 4, cre_adjustment_paid_cents + cre_adjustment_promotional_cents,0)) AS grants_amount,
        COUNT(IF(cre_transaction_type = 5,cre_credit_log_id,NULL)) AS num_monetary_refunds,
        SUM(IF(cre_transaction_type = 5, -1*(cre_adjustment_paid_cents + cre_adjustment_promotional_cents),0)) AS monetary_refunds_amount,
        COUNT(IF(cre_transaction_type = 8,cre_credit_log_id,NULL)) AS num_cc_refunds,
        SUM(IF(cre_transaction_type = 8, cre_adjustment_paid_cents + cre_adjustment_promotional_cents,0)) AS cc_refunds_amount

FROM agents a 
JOIN `tt-dp-prod.website.cre_credit_log` c 
  ON SAFE_CAST(a.tt_admin_id AS INT64) = c.cre_performing_usr_user_id 
    AND EXTRACT(DATE FROM TIMESTAMP_MILLIS(c.cre_timestamp) AT TIME ZONE "America/Denver") BETWEEN a.date_team_start AND a.date_team_end 
    AND EXTRACT(DATE FROM TIMESTAMP_MILLIS(c.cre_timestamp) AT TIME ZONE "America/Denver") >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY)
WHERE cre_usr_user_id != cre_performing_usr_user_id
  AND cre_transaction_type IN (3,4,5,8)
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 1,10
)

, agent_index AS (
SELECT
      *
FROM agents,
UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY), CURRENT_DATE())) AS index_date
WHERE index_date BETWEEN date_team_start AND date_team_end
ORDER BY specialist_name, index_date
)

, real_data AS (
SELECT 
        a.*,
        COALESCE(c.closed_cases,0) AS closed_cases,
        COALESCE(c.pushed_surveys,0) AS pushed_surveys,
        
        COALESCE(lt.total_lops_time,0) AS total_lops_time,
        COALESCE(lt.break_time,0) AS break_time,
        COALESCE(lt.chat_time,0) AS chat_time,
        COALESCE(lt.coaching_time,0) AS coaching_time,
        COALESCE(lt.hold_time,0) AS hold_time,
        COALESCE(lt.idle_time,0) AS idle_time,
        COALESCE(lt.idle_dialout_time,0) AS idle_dialout_time,
        COALESCE(lt.lunch_time,0) AS lunch_time,
        COALESCE(lt.meeting_time,0) AS meeting_time,
        COALESCE(lt.other_unavailable_time,0) AS other_unavailable_time,
        COALESCE(lt.special_projects_time,0) AS special_projects_time,
        COALESCE(lt.talk_time,0) AS talk_time,
        COALESCE(lt.training_time,0) AS training_time,
        COALESCE(lt.wrap_time,0) AS wrap_time,

        COALESCE(cl.csat_responses,0) AS csat_responses,
        COALESCE(cl.csat_top2,0) AS csat_top2,
        COALESCE(cl.csat_5,0) AS csat_5,
        COALESCE(cl.csat_4,0) AS csat_4,
        COALESCE(cl.csat_3,0) AS csat_3,
        COALESCE(cl.csat_2,0) AS csat_2,
        COALESCE(cl.csat_1,0) AS csat_1,
        COALESCE(cl.ltc_responses,0) AS ltc_responses,
        COALESCE(cl.ltc_top2,0) AS ltc_top2,
        COALESCE(cl.ltc_5,0) AS ltc_5,
        COALESCE(cl.ltc_4,0) AS ltc_4,
        COALESCE(cl.ltc_3,0) AS ltc_3,
        COALESCE(cl.ltc_2,0) AS ltc_2,
        COALESCE(cl.ltc_1,0) AS ltc_1,

        COALESCE(rg.num_refunds,0) AS num_refunds,
        COALESCE(rg.refunds_amount,0) AS refunds_amount,
        COALESCE(rg.num_grants,0) AS num_grants,
        COALESCE(rg.grants_amount,0) AS grants_amount,
        COALESCE(rg.num_monetary_refunds,0) AS num_monetary_refunds,
        COALESCE(rg.monetary_refunds_amount,0) AS monetary_refunds_amount,
        COALESCE(rg.num_cc_refunds,0) AS num_cc_refunds,
        COALESCE(rg.cc_refunds_amount,0) AS cc_refunds_amount
        
FROM agent_index a
LEFT JOIN closed_cases c ON a.salesforce_id_18 = c.salesforce_id_18 AND a.index_date = c.closed_date_mt
LEFT JOIN lops_time lt ON a.liveops = lt.liveops AND a.index_date = lt.event_date_mt
LEFT JOIN csat_ltc cl ON a.salesforce_id_18 = cl.salesforce_id_18 AND a.index_date = cl.csat_response_date_mt
LEFT JOIN refunds_grants rg ON a.tt_admin_id = rg.tt_admin_id AND a.index_date = rg.cre_date_mt
ORDER BY specialist_name, index_date
)

,fake_data AS (
SELECT 
        'Jack Erb' AS specialist_name,
        'jerb@thumbtack.com' AS email_address,
        liveops,
        tt_admin_id,
        salesforce_id_18,
        'Marcus Bertilson' AS manager_name,
        date_team_start,
        date_team_end,
        emp_lvl,
        team_name,
        index_date,
        closed_cases,
        pushed_surveys,
        total_lops_time,
        break_time,
        chat_time,
        coaching_time,
        hold_time,
        idle_time,
        idle_dialout_time,
        lunch_time,
        meeting_time,
        other_unavailable_time,
        special_projects_time,
        talk_time,
        training_time,
        wrap_time,
        csat_responses,
        csat_top2,
        csat_5,
        csat_4,
        csat_3,
        csat_2,
        csat_1,
        ltc_responses,
        ltc_top2,
        ltc_5,
        ltc_4,
        ltc_3,
        ltc_2,
        ltc_1,
        num_refunds,
        refunds_amount,
        num_grants,
        grants_amount,
        num_monetary_refunds,
        monetary_refunds_amount,
        num_cc_refunds,
        cc_refunds_amount
FROM real_data
WHERE email_address = 'ryan@thumbtack.com'
)

,fake_data2 AS (
SELECT 
        'Servaes Tholen' AS specialist_name,
        'stholen@thumbtack.com' AS email_address,
        liveops,
        tt_admin_id,
        salesforce_id_18,
        'Marcus Bertilson' AS manager_name,
        date_team_start,
        date_team_end,
        emp_lvl,
        team_name,
        index_date,
        closed_cases,
        pushed_surveys,
        total_lops_time,
        break_time,
        chat_time,
        coaching_time,
        hold_time,
        idle_time,
        idle_dialout_time,
        lunch_time,
        meeting_time,
        other_unavailable_time,
        special_projects_time,
        talk_time,
        training_time,
        wrap_time,
        csat_responses,
        csat_top2,
        csat_5,
        csat_4,
        csat_3,
        csat_2,
        csat_1,
        ltc_responses,
        ltc_top2,
        ltc_5,
        ltc_4,
        ltc_3,
        ltc_2,
        ltc_1,
        num_refunds,
        refunds_amount,
        num_grants,
        grants_amount,
        num_monetary_refunds,
        monetary_refunds_amount,
        num_cc_refunds,
        cc_refunds_amount
FROM real_data
WHERE email_address = 'ryan@thumbtack.com'
)

SELECT * FROM real_data
-- UNION ALL SELECT * FROM fake_data2
-- UNION ALL SELECT * FROM fake_data
