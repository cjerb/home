#standardsql
WITH agents AS (
 SELECT
    r1.empID AS tt_admin_id,
    r1.name AS specialist_name,
    r1.email AS email_address,
    u.id AS salesforce_id_18,
    r1.hired_date,
    COALESCE(r.manager,r1.manager_three) AS manager_name,
    r1.tier AS team_name,
    LOWER(r1.upwork) AS upwork_lower,
    MIN(r1.week_start) AS date_team_start,
    DATE_ADD(MAX(r1.week_start),INTERVAL 6 DAY) AS date_team_end
  FROM `tt-dp-prod.sandbox.gunnar_tph_roster_static` r1
  LEFT JOIN `tt-dp-prod.tack.user` u ON u.email = r1.email
  LEFT JOIN `tt-dp-prod.sandbox.gunnar_tph_roster_static` r ON r.week_start = r1.week_start AND r.shortname = r1.manager
  WHERE u.isactive = '1'
  GROUP BY 1,2,3,4,5,6,7,8
),
upwork AS (
SELECT 
PARSE_DATE('%m/%d/%Y',date) AS date,
LOWER(full_name) AS name_lower,
SUM(total_hours) AS total_hours
FROM `tt-dp-prod.sandbox.cs_upwork`
group by 1,2
)
,closed_cases AS (
  SELECT
    DATE(c.closed_time) AS closed_date,
    c.case_owner_agent_id,
    COUNT(c.closed_time) AS closed_cases,
    COUNT(c.csat_offered_time) AS pushed_surveys

  FROM `tt-dp-prod.ops.cases` c 
  WHERE
    (DATE(c.created_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
      OR DATE(c.closed_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY))
    AND first_contact_channel IN ('Phone', 'Email', 'Chat', 'SMS', 'Directly Question','In-Product')
    AND auto_response IS FALSE
    AND duplicate_case_id IS NULL
    AND COALESCE(case_category, '') NOT IN ('Marketplace Integrity')
    AND c.closed_time_mt IS NOT NULL

  GROUP BY 1, 2 
)
  
,omni_time AS (
  SELECT
    sl.event_date,
    sl.salesforce_id_18,
    SUM(sl.total_time) AS total_omni_time,
    SUM(sl.total_time) - SUM(IF(time_bucket IN ('Lunch' /*,'Away','Away (Deprecated)','Offline','Break'*/), sl.total_time, 0)) AS total_omni_denominator,
    SUM(IF(time_bucket IN ('Offline'), sl.total_time, 0)) AS offline_time,
    SUM(IF(time_bucket LIKE ('Online%'), sl.total_time, 0)) AS online_time,
    SUM(IF(time_bucket IN ('Coaching'), sl.total_time, 0)) AS coaching_time,
    SUM(IF(time_bucket IN ('Lunch'), sl.total_time, 0)) AS lunch_time,
    SUM(IF(time_bucket IN ('Break'), sl.total_time, 0)) AS break_time,
    SUM(IF(time_bucket IN ('Away', 'Away (Deprecated)'), sl.total_time, 0)) AS away_time,
    SUM(IF(time_bucket IN ('Training'), sl.total_time, 0)) AS training_time,
    SUM(IF(time_bucket IN ('Meeting'), sl.total_time, 0)) AS meeting_time,
    SUM(IF(time_bucket IN ('Follow Up'), sl.total_time, 0)) AS follow_up_time,
    SUM(IF(time_bucket IN ('Wrap Up (Break)'), sl.total_time, 0)) AS wrap_break_time,
    SUM(IF(time_bucket IN ('Wrap Up (End of shift)'), sl.total_time, 0)) AS wrap_shift_end_time,
    SUM(IF(time_bucket IS NULL, sl.total_time, 0)) AS null_status_time
  FROM (
    SELECT
      DATE(PARSE_TIMESTAMP("%m/%d/%Y %T", p.createddate)) event_date,
      p.ownerid AS salesforce_id_18,
      s.masterlabel AS time_bucket,
      CAST(p.statusduration AS FLOAT64) AS total_time
    FROM `tt-dp-prod.tack.user_service_presence` p
    LEFT JOIN `tt-dp-prod.tack.service_presence_status` s
      ON s.id = p.servicepresencestatusid
    WHERE
      DATE(PARSE_TIMESTAMP("%m/%d/%Y %T", p.createddate)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY) ) sl
  GROUP BY 1, 2
),


csat_ltc AS (
  SELECT
    c.csat_owner_id,
    DATE(c.csat_response_time) AS csat_response_date,
    COUNT(c.csat_score) AS csat_responses,
    COALESCE(SUM(IF(c.csat_score IN (4, 5), 1, NULL)), 0) AS csat_top2,
    COALESCE(SUM(IF(c.csat_score IN (5), 1, NULL)), 0) AS csat_5,
    COALESCE(SUM(IF(c.csat_score IN (4), 1, NULL)), 0) AS csat_4,
    COALESCE(SUM(IF(c.csat_score IN (3), 1, NULL)), 0) AS csat_3,
    COALESCE(SUM(IF(c.csat_score IN (2), 1, NULL)), 0) AS csat_2,
    COALESCE(SUM(IF(c.csat_score IN (1), 1, NULL)), 0) AS csat_1,
    COUNT(c.likely_continue_using_score) AS ltc_responses,
    COALESCE(SUM(IF(c.likely_continue_using_score IN (4, 5), 1, NULL)), 0) AS ltc_top2,
    COALESCE(SUM(IF(c.likely_continue_using_score IN (5), 1, NULL)), 0) AS ltc_5,
    COALESCE(SUM(IF(c.likely_continue_using_score IN (4), 1, NULL)), 0) AS ltc_4,
    COALESCE(SUM(IF(c.likely_continue_using_score IN (3), 1, NULL)), 0) AS ltc_3,
    COALESCE(SUM(IF(c.likely_continue_using_score IN (2), 1, NULL)), 0) AS ltc_2,
    COALESCE(SUM(IF(c.likely_continue_using_score IN (1), 1, NULL)), 0) AS ltc_1
  FROM `tt-dp-prod.ops.csat` c  
  WHERE DATE(c.csat_created_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
    OR DATE(c.csat_response_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
    AND COALESCE(c.csat_case_origin,'') NOT IN ('In-Product')

  GROUP BY 1, 2
),


refunds_grants AS (
  SELECT
    c.cre_performing_usr_user_id,
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(c.cre_timestamp)) AS cre_date,
    COUNT(IF(cre_transaction_type = 3, cre_credit_log_id, NULL)) AS num_refunds,
    SUM(IF(cre_transaction_type = 3, cre_adjustment_paid_cents + cre_adjustment_promotional_cents, 0)) AS refunds_amount,
    COUNT(IF(cre_transaction_type = 4, cre_credit_log_id, NULL)) AS num_grants,
    SUM(IF(cre_transaction_type = 4, cre_adjustment_paid_cents + cre_adjustment_promotional_cents, 0)) AS grants_amount,
    COUNT(IF(cre_transaction_type = 5, cre_credit_log_id, NULL)) AS num_monetary_refunds,
    SUM(IF(cre_transaction_type = 5, -1*(cre_adjustment_paid_cents + cre_adjustment_promotional_cents), 0)) AS monetary_refunds_amount,
    COUNT(IF(cre_transaction_type = 8, cre_credit_log_id, NULL)) AS num_cc_refunds,
    SUM(IF(cre_transaction_type = 8, cre_adjustment_paid_cents + cre_adjustment_promotional_cents, 0)) AS cc_refunds_amount
  FROM `tt-dp-prod.website.cre_credit_log` c 
  WHERE cre_usr_user_id != cre_performing_usr_user_id
    AND cre_transaction_type IN (3, 4, 5, 8)
    AND EXTRACT(DATE FROM TIMESTAMP_MILLIS(c.cre_timestamp)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
  GROUP BY 1, 2
),

agent_index AS (
  SELECT
    *
  FROM agents,
    UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY), CURRENT_DATE())) AS index_date
  WHERE index_date BETWEEN date_team_start AND date_team_end
  ORDER BY specialist_name, index_date 
)

, real_data AS (
SELECT
  a.*,
  COALESCE(c.closed_cases, 0) AS closed_cases,
  COALESCE(c.pushed_surveys, 0) AS pushed_surveys,
  COALESCE(u.total_hours, 0) AS total_upwork_hours,
  COALESCE(lt.total_omni_denominator, 0)/3600 AS total_omni_denominator_hours,
  COALESCE(lt.total_omni_denominator, 0) AS total_omni_denominator,
  COALESCE(lt.total_omni_time, 0)/3600 AS total_omni_time_hours,
  COALESCE(lt.total_omni_time, 0) AS total_omni_time,
  COALESCE(lt.offline_time, 0) AS offline_time,
  COALESCE(lt.online_time, 0) AS online_time,
  COALESCE(lt.coaching_time, 0) AS coaching_time,
  COALESCE(lt.lunch_time, 0) AS lunch_time,
  COALESCE(lt.break_time, 0) AS break_time,
  COALESCE(lt.away_time, 0) AS away_time,
  COALESCE(lt.training_time, 0) AS training_time,
  COALESCE(lt.meeting_time, 0) AS meeting_time,
  COALESCE(lt.follow_up_time, 0) AS follow_up_time,
  COALESCE(lt.wrap_break_time, 0) AS wrap_break_time,
  COALESCE(lt.wrap_shift_end_time, 0) AS wrap_shift_end_time,
  COALESCE(lt.null_status_time, 0) AS null_status_time,
  COALESCE(cl.csat_responses, 0) AS csat_responses,
  COALESCE(cl.csat_top2, 0) AS csat_top2,
  COALESCE(cl.csat_5, 0) AS csat_5,
  COALESCE(cl.csat_4, 0) AS csat_4,
  COALESCE(cl.csat_3, 0) AS csat_3,
  COALESCE(cl.csat_2, 0) AS csat_2,
  COALESCE(cl.csat_1, 0) AS csat_1,
  COALESCE(cl.ltc_responses, 0) AS ltc_responses,
  COALESCE(cl.ltc_top2, 0) AS ltc_top2,
  COALESCE(cl.ltc_5, 0) AS ltc_5,
  COALESCE(cl.ltc_4, 0) AS ltc_4,
  COALESCE(cl.ltc_3, 0) AS ltc_3,
  COALESCE(cl.ltc_2, 0) AS ltc_2,
  COALESCE(cl.ltc_1, 0) AS ltc_1,
  COALESCE(rg.num_refunds, 0) AS num_refunds,
  COALESCE(rg.refunds_amount, 0) AS refunds_amount,
  COALESCE(rg.num_grants, 0) AS num_grants,
  COALESCE(rg.grants_amount, 0) AS grants_amount,
  COALESCE(rg.num_monetary_refunds, 0) AS num_monetary_refunds,
  COALESCE(rg.monetary_refunds_amount, 0) AS monetary_refunds_amount,
  COALESCE(rg.num_cc_refunds, 0) AS num_cc_refunds,
  COALESCE(rg.cc_refunds_amount, 0) AS cc_refunds_amount

FROM agent_index a
LEFT JOIN closed_cases c ON a.salesforce_id_18 = c.case_owner_agent_id
    AND a.index_date = c.closed_date
LEFT JOIN omni_time lt ON a.salesforce_id_18 = lt.salesforce_id_18
    AND a.index_date = lt.event_date
LEFT JOIN csat_ltc cl ON a.salesforce_id_18 = cl.csat_owner_id
    AND a.index_date = cl.csat_response_date
LEFT JOIN refunds_grants rg ON a.tt_admin_id = rg.cre_performing_usr_user_id
    AND a.index_date = rg.cre_date
LEFT JOIN upwork u ON u.name_lower = a.upwork_lower
    AND a.index_date = u.date
WHERE team_name IN ('TPH Email','TPH Chat','TPH SMS')
ORDER BY specialist_name, index_date
)

,fake_data AS (
SELECT  
        tt_admin_id,
        'Jack Erb' AS specialist_name,
        'jerb@thumbtack.com' AS email_address,
        salesforce_id_18,
        hired_date,
        'Marcus Bertilson' AS manager_name,
        team_name,
        upwork_lower,
        date_team_start,
        date_team_end,
        index_date,
        closed_cases,
        pushed_surveys,
        total_upwork_hours,
        total_omni_denominator_hours,
        total_omni_denominator,
        total_omni_time_hours,
        total_omni_time,
        offline_time,
        online_time,
        coaching_time,
        lunch_time,
        break_time,
        away_time,
        training_time,
        meeting_time,
        follow_up_time,
        wrap_break_time,
        wrap_shift_end_time,
        null_status_time,
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
WHERE email_address = 'llmendoza@ttc.thumbtack.com'
)

SELECT * FROM real_data
UNION ALL SELECT * FROM fake_data
ORDER BY index_date DESC, specialist_name
