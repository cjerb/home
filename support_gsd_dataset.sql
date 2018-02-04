WITH tsl_agg AS (
SELECT 
        IF(manager_name = "IBEX","IBEX","TSL Phone") AS team_name,
        index_date,
        SUM(closed_cases) AS closed_cases,
        SUM(total_lops_time - lunch_time)/60 AS hours_worked,
        SUM(ltc_responses) AS ltc_responses,
        SUM(ltc_top2) AS ltc_top2
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
        SUM(total_omni_time - lunch_time)/3600 AS hours_worked,
        SUM(ltc_responses) AS ltc_responses,
        SUM(ltc_top2) AS ltc_top2
FROM `tt-dp-prod.reference.jerb_tph_scorecard_dataset` 
WHERE email_address NOT IN ('jerb@thumbtack.com','stholen@thumbtack.com')
GROUP BY 1,2
ORDER BY 1,2
)

SELECT * FROM tsl_agg
UNION ALL SELECT * FROM tph_agg
ORDER BY team_name, index_date

