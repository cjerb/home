#standardsql
# owner: jerb@thumbtack.com, gwoolsey@thumbtack.com

SELECT
      id AS action_plan_id,
      actionplannumber__c AS action_plan_number,
      PARSE_TIMESTAMP('%m/%d/%Y %T', createddate) AS action_plan_created_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', planstartdate__c) AS action_plan_start_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', planenddate__c) AS action_plan_end_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', expirationdate__c) AS action_plan_expiration_time,
      
      actionplantemplate__c AS action_plan_template_id,
      actionplantype__c AS action_plan_type,
      playbookurl__c AS playbook_url,
      
      status__c AS action_plan_status,

      createdbyid AS created_by_tack_id,
      CONCAT(u1.user_first_name, " ", u1.user_last_name) AS created_by_tack_user_name,
      ownerid AS owner_tack_id,
      CONCAT(u2.user_first_name, " ", u2.user_last_name) AS owner_tack_user_name,

      contact__c AS tack_contact_id,
      parentaccount__c AS tack_parent_account_id,
      account__c AS tack_account_id,
      lead__c AS tack_lead_id,
      service__c AS tack_service_id,
      
      SAFE_CAST(totalnumberofactionitems__c AS FLOAT64) AS total_action_plan_items,
      SAFE_CAST(numberofcompleteactionitems__c AS FLOAT64) AS number_completed_action_plan_items,
      SAFE_CAST(percentcomplete__c AS FLOAT64) AS percent_action_plan_complete,
      
      PARSE_TIMESTAMP('%m/%d/%Y %T', lastcheckindate__c) AS last_checkin_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', lastattempt__c) AS last_attempt_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', nextcallscheduled__c) AS next_call_scheduled_time,

-- Commenting out these fields until we better understand what they are, if they're accurate, and whether we need them

--       engagement__c AS pro_engagement,
--       availablebudget__c AS pro_available_budget,
--       SAFE_CAST(quotes__c AS FLOAT64) AS pro_number_quotes,
--       SAFE_CAST(contactrate__c AS FLOAT64) AS pro_contact_rate,  
--       SAFE_CAST(responsetime__c AS FLOAT64) AS pro_response_time_minutes,
--       SAFE_CAST(revenueforthistimeperiod__c AS FLOAT64) AS pro_revenue_this_time_period,
--       revenuestatus__c AS pro_revenue_status,

--       SAFE_CAST(basescore__c AS FLOAT64) AS pro_base_score,
--       SAFE_CAST(accountscore__c AS FLOAT64) AS pro_account_score,
--       SAFE_CAST(accounttypescore__c AS FLOAT64) AS pro_account_type_score,
--       SAFE_CAST(occcatscore__c AS FLOAT64) AS pro_occupation_category_score,
--       SAFE_CAST(requestvolumescore__c AS FLOAT64) AS pro_request_volume_score,
--       SAFE_CAST(subjectivescore__c AS FLOAT64) AS pro_subjective_score,
--       accountmanager__c AS tack_account_manager_name,

      lastmodifiedbyid AS last_modified_by_id,
      CONCAT(u3.user_first_name, " ", u3.user_last_name) AS last_modified_by_tack_user_name,
      PARSE_TIMESTAMP('%m/%d/%Y %T', lastmodifieddate) AS last_modified_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', lastactivitydate) AS last_activity_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', lastreferenceddate) AS last_referenced_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', lastvieweddate) AS last_viewed_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', systemmodstamp) AS system_modstamp_time,
      recordtypeid AS tack_record_type_id,

      SAFE_CAST(isdeleted AS INT64) AS action_plan_is_deleted

FROM tack.action_plan a
LEFT JOIN ops.tack_user u1 ON a.createdbyid = u1.tack_user_id
LEFT JOIN ops.tack_user u2 ON a.ownerid = u2.tack_user_id
LEFT JOIN ops.tack_user u3 ON a.lastmodifiedbyid = u3.tack_user_id

WHERE istestdata__c = '0'
  AND actionplantemplate__c IS NOT NULL


-- NOTE: Following fields excluded for PII reasons

--       name AS action_plan_name, -- Exclude for PII
--       changespromisesmade__c, -- Exclude for PII
--       opportunities__c, -- Excluded for PII
--       otherrelevantinfo__c, -- Excluded for PII
--       description__c, -- Exclude for PII
--       progoals__c, -- Exclude for PII
--       strengths__c, -- Exclude for PII
--       weaknesses__c -- Exclude for PII
--       threats__c, -- Exclude for PII
--       thumbtackgoals__c, -- Exclude for PII
