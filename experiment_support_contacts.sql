#standardsql
WITH exper_stop_times AS (
SELECT 
        experiment_id, 
        MIN(TIMESTAMP_MILLIS(assignment_time)) AS experiment_start_time,
        MAX(TIMESTAMP_MILLIS(experiment_stop_time)) AS experiment_stop_time
FROM `tt-dp-prod.assignments.user` 
GROUP BY 1
ORDER BY 1
)

,pro_assignments_bucket AS (
SELECT
        pro.experiment_id,
        bucket_id,
        user_id,
        MIN(TIMESTAMP_MILLIS(assignment_time)) AS min_assignment_time,
        MAX(TIMESTAMP_MILLIS(assignment_time)) AS max_assignment_time,
        COUNT(1) AS num_rec,
        MIN(est.experiment_start_time) AS experiment_start_time,
        MAX(est.experiment_stop_time) AS experiment_stop_time
FROM `tt-dp-prod.assignments.user` pro
LEFT JOIN exper_stop_times est ON pro.experiment_id = est.experiment_id
WHERE TIMESTAMP_MILLIS(pro.assignment_time) <= COALESCE(est.experiment_stop_time,'2999-01-01 00:00:00')
GROUP BY 1,2,3
)

, pro_assignments_num_buckets AS (
SELECT 
        experiment_id,
        bucket_id,
        user_id,
        min_assignment_time,
        max_assignment_time,
        experiment_start_time,
        experiment_stop_time,
        COUNT(user_id) OVER (PARTITION BY experiment_id, user_id) AS num_buckets
FROM pro_assignments_bucket
)

, pro_assignments AS (
SELECT 
        experiment_id,
        IF(num_buckets > 1, 'multiple',bucket_id) AS bucket_id,
        user_id,
        MIN(min_assignment_time) AS min_assignment_time,
        MAX(max_assignment_time) AS max_assignment_time,
        experiment_start_time,
        experiment_stop_time,
        AVG(num_buckets) AS num_buckets
FROM pro_assignments_num_buckets
GROUP BY 1,2,3,6,7
)

, final_user_experiments AS (
SELECT
        a.experiment_id,
        a.bucket_id,
        a.experiment_start_time,
        a.experiment_stop_time,
        COUNT(DISTINCT a.user_id) AS tot_bucket_users,
        COUNT(DISTINCT IF(case_id IS NOT NULL,a.user_id,NULL)) AS contact_bucket_users,
        COUNT(IF(case_id IS NOT NULL,1,NULL)) AS num_cases
FROM pro_assignments a
LEFT JOIN 
      (
        SELECT user_id, created_time, case_id
        FROM `tt-dp-prod.ops.cases` 
        WHERE first_contact_channel IN ('Phone','Chat','Email','SMS','Directly Question','In-Product')
          AND auto_response IS FALSE
          AND COALESCE(case_subcategory,'') <> 'Noise'
      ) c 
   ON a.user_id = c.user_id AND c.created_time BETWEEN min_assignment_time AND COALESCE(experiment_stop_time, '2999-01-01')
GROUP BY 1,2,3,4
ORDER BY experiment_id DESC, bucket_id
)



, request_pros AS (
SELECT 
        experiment_id,
        bucket_id,
        MIN(assignment_time) OVER (PARTITION BY experiment_id, qp.pro_user_id) AS min_assignment_time,
        MAX(assignment_time) OVER (PARTITION BY experiment_id, qp.pro_user_id) AS max_assignment_time,
        MIN(assignment_time) OVER (PARTITION BY experiment_id) AS experiment_start_time,
        MAX(experiment_stop_time) OVER (PARTITION BY experiment_id) AS experiment_stop_time,
        r.request_id,
        qp.pro_user_id AS user_id
FROM `tt-dp-prod.assignments.request` r
LEFT JOIN `tt-dp-prod.a.quotes` qp ON r.request_id = qp.request_id
WHERE qp.pro_user_id IS NOT NULL
)

, request_customers AS (
SELECT 
        experiment_id,
        bucket_id,
        MIN(assignment_time) OVER (PARTITION BY experiment_id, qc.customer_id) AS min_assignment_time,
        MAX(assignment_time) OVER (PARTITION BY experiment_id, qc.customer_id) AS max_assignemnt_time,
        MIN(assignment_time) OVER (PARTITION BY experiment_id) AS experiment_start_time,
        MAX(experiment_stop_time) OVER (PARTITION BY experiment_id) AS experiment_stop_time,
        r.request_id,
        qc.customer_id AS user_id
FROM `tt-dp-prod.assignments.request` r
LEFT JOIN `tt-dp-prod.a.quotes` qc ON r.request_id = qc.request_id
WHERE qc.customer_id IS NOT NULL
)

, union_pros_customers AS (
SELECT 
        experiment_id,
        bucket_id,
        MIN(min_assignment_time) OVER (PARTITION BY experiment_id, user_id) AS min_assignment_time,
        MAX(max_assignment_time) OVER (PARTITION BY experiment_id, user_id) AS max_assignment_time,
        experiment_start_time AS min_experiment_start_time,
        experiment_stop_time AS max_experiment_stop_time,
        request_id,
        user_id
FROM (
    SELECT * FROM request_pros
    UNION ALL SELECT * FROM request_customers
    )
)

, grouped_assignments AS (
SELECT 
        experiment_id,
        bucket_id,
        MIN(min_assignment_time) AS min_assignment_time,
        MAX(max_assignment_time) AS max_assignment_time,
        MIN(min_experiment_start_time) AS experiment_start_time,
        MAX(max_experiment_stop_time) AS experiment_stop_time,
        request_id,
        user_id
FROM union_pros_customers
GROUP BY 1,2,7,8
)

, num_user_buckets AS (
SELECT
      experiment_id,
      bucket_id,
      min_assignment_time,
      max_assignment_time,
      experiment_start_time,
      experiment_stop_time,
      request_id,
      user_id,
      COUNT(DISTINCT bucket_id)  OVER (PARTITION BY experiment_id, user_id) AS num_user_buckets,
      COUNT(DISTINCT request_id) OVER (PARTITION BY experiment_id, user_id) AS num_user_exp_requests,
      COUNT(DISTINCT request_id) OVER (PARTITION BY experiment_id, bucket_id, user_id) num_user_bucket_requests
FROM grouped_assignments
)


, final_request_experiments_0 AS (
SELECT
      experiment_id,
      bucket_id,
      TIMESTAMP_MILLIS(experiment_start_time) AS experiment_start_time,
      TIMESTAMP_MILLIS(experiment_stop_time) AS experiment_stop_time,
      request_id,
      ROUND((COUNT(DISTINCT ga.user_id) / MAX(num_user_exp_requests)),0) AS tot_bucket_users,
      ROUND((COUNT(DISTINCT c.user_id) / MAX(num_user_exp_requests)),0) AS contact_bucket_users,
      ROUND((COUNT(DISTINCT c.case_id) / MAX(num_user_exp_requests)),0) AS num_cases
      
FROM num_user_buckets ga
LEFT JOIN 
      (
        SELECT user_id, created_time, case_id
        FROM `tt-dp-prod.ops.cases` 
        WHERE first_contact_channel IN ('Phone','Chat','Email','SMS','Directly Question','In-Product')
          AND auto_response IS FALSE
          AND COALESCE(case_subcategory,'') <> 'Noise'
      ) c
      
    ON ga.user_id = c.user_id AND c.created_time BETWEEN TIMESTAMP_MILLIS(min_assignment_time) AND COALESCE(TIMESTAMP_MILLIS(experiment_stop_time), '2999-01-01')

GROUP BY 1,2,3,4,5
)

, final_request_experiments AS (
SELECT
      experiment_id,
      bucket_id,
      experiment_start_time,
      experiment_stop_time,
      SUM(tot_bucket_users) AS tot_bucket_users,
      SUM(contact_bucket_users) AS contact_bucket_users,
      SUM(num_cases) AS num_cases
      
FROM final_request_experiments_0 ga
GROUP BY 1,2,3,4
)

SELECT *, 'request' AS assignment_type FROM final_request_experiments
UNION ALL SELECT *, 'user' AS assignment_type FROM final_user_experiments
