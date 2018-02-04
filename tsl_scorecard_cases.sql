#standardsql
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

, real_data AS (
SELECT 
        a.specialist_name,
        a.email_address,
        a.liveops,
        a.tt_admin_id,
        a.salesforce_id_18,
        a.manager_name,
        a.date_team_start,
        a.date_team_end,
        a.emp_lvl,
        a.team_name,
        c.case_id,
        c.case_number,
        c.first_contact_channel,
        c.case_category,
        c.case_subcategory,
        c.duplicate_case_id,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.created_time_mt) AS created_time_mt,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.closed_time_mt) AS closed_time_mt,
        c.csat_id,
        c.csat_owner_agent_id,
        c.csat_survey_offered,
        c.csat_survey_not_offered_reason,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.csat_created_time_mt) AS csat_created_time_mt,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.csat_offered_time_mt) AS csat_offered_time_mt,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.csat_response_time_mt) AS csat_response_time_mt,
        c.csat_score,
        c.likely_continue_using_score,
        c.easy_to_handle_issue_score,
        c.less_time_than_expected_score,
        c.issue_resolved_score,
        c.number_interactions_to_resolution,
        c.csat_audit_exception_reason,
        c.pro_contact_info_captured,
        c.csat_comments,
        t.caseconsolelink__c AS case_link
        
        
FROM agents a 
JOIN `tt-dp-prod.ops.cases` c ON a.salesforce_id_18 = c.case_owner_agent_id AND DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", created_time_mt)) BETWEEN a.date_team_start AND a.date_team_end
LEFT JOIN `tt-dp-prod.tack.sf_case` t ON c.case_id = t.id
WHERE (DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", c.created_time_mt)) >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY) OR  DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", c.closed_time_mt)) >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY))
  AND first_contact_channel IN ('Phone','Email','Chat','SMS','Directly Question','In-Product')
  AND auto_response IS FALSE
  AND duplicate_case_id IS NULL
  AND COALESCE(case_category,'') NOT IN ('Marketplace Integrity')
ORDER BY specialist_name, created_time_mt DESC
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
        case_id,
        case_number,
        first_contact_channel,
        case_category,
        case_subcategory,
        duplicate_case_id,
        created_time_mt,
        closed_time_mt,
        csat_id,
        csat_owner_agent_id,
        csat_survey_offered,
        csat_survey_not_offered_reason,
        csat_created_time_mt,
        csat_offered_time_mt,
        csat_response_time_mt,
        csat_score,
        likely_continue_using_score,
        easy_to_handle_issue_score,
        less_time_than_expected_score,
        issue_resolved_score,
        number_interactions_to_resolution,
        csat_audit_exception_reason,
        pro_contact_info_captured,
        csat_comments,
        case_link
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
        case_id,
        case_number,
        first_contact_channel,
        case_category,
        case_subcategory,
        duplicate_case_id,
        created_time_mt,
        closed_time_mt,
        csat_id,
        csat_owner_agent_id,
        csat_survey_offered,
        csat_survey_not_offered_reason,
        csat_created_time_mt,
        csat_offered_time_mt,
        csat_response_time_mt,
        csat_score,
        likely_continue_using_score,
        easy_to_handle_issue_score,
        less_time_than_expected_score,
        issue_resolved_score,
        number_interactions_to_resolution,
        csat_audit_exception_reason,
        pro_contact_info_captured,
        csat_comments,
        case_link
FROM real_data 
WHERE email_address = 'ryan@thumbtack.com'
)

SELECT * FROM real_data
-- UNION ALL SELECT * FROM fake_data2
-- UNION ALL SELECT * FROM fake_data
