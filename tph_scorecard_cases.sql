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
  FROM `tt-dp-prod.sandbox.gunnar_tph_roster_static`r1
  LEFT JOIN `tt-dp-prod.tack.user` u ON u.email = r1.email
  LEFT JOIN `tt-dp-prod.sandbox.gunnar_tph_roster_static` r on r.week_start = r1.week_start and r.shortname = r1.manager
  WHERE u.isactive = '1'
  GROUP BY 1,2,3,4,5,6,7,8
)

,real_data AS (    
SELECT 
        a.tt_admin_id,
        a.specialist_name,
        a.email_address,
        a.salesforce_id_18,
        a.hired_date,
        a.manager_name,
        a.team_name,
        a.upwork_lower,
        a.date_team_start,
        a.date_team_end,
        c.case_id,
        c.case_number,
        c.first_contact_channel,
        c.case_category,
        c.case_subcategory,
        c.duplicate_case_id,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.created_time_pht) AS created_time_pht,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.closed_time_pht) AS closed_time_pht,
        c.csat_id,
        c.csat_owner_agent_id,
        c.csat_survey_offered,
        c.csat_survey_not_offered_reason,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.csat_created_time_pht) AS csat_created_time_pht,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.csat_offered_time_pht) AS csat_offered_time_pht,
        PARSE_TIMESTAMP("%Y-%m-%d %T", c.csat_response_time_pht) AS csat_response_time_pht,
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
WHERE (DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", c.created_time_pht)) >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY) OR  DATE(PARSE_TIMESTAMP("%Y-%m-%d %T", c.closed_time_pht)) >= DATE_SUB(CURRENT_DATE(),INTERVAL 45 DAY))
  AND first_contact_channel IN ('Phone','Email','Chat','SMS','Directly Question','In-Product')
  AND auto_response IS FALSE
  AND duplicate_case_id IS NULL
  AND COALESCE(case_category,'') NOT IN ('Marketplace Integrity')
ORDER BY specialist_name, created_time_mt DESC

)

,fake_data AS (
SELECT 
        tt_admin_id,
        'Jack Erb' AS specialist_name,
        'jerb@thumbtack.com' AS email_address,
        salesforce_id_18,
        hired_date,
        manager_name,
        team_name,
        upwork_lower,
        date_team_start,
        date_team_end,
        case_id,
        case_number,
        first_contact_channel,
        case_category,
        case_subcategory,
        duplicate_case_id,
        created_time_pht,
        closed_time_pht,
        csat_id,
        csat_owner_agent_id,
        csat_survey_offered,
        csat_survey_not_offered_reason,
        csat_created_time_pht,
        csat_offered_time_pht,
        csat_response_time_pht,
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
WHERE email_address = 'llmendoza@ttc.thumbtack.com'
)

SELECT * FROM real_data
-- UNION ALL SELECT * FROM fake_data
