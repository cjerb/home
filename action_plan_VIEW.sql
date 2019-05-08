#standardsql
# owner: jerb@thumbtack.com, gwoolsey@thumbtack.com
SELECT
      a.id AS action_plan_id,
      a.actionplannumber__c AS action_plan_number,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.createddate) AS action_plan_created_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.planstartdate__c) AS action_plan_start_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.planenddate__c) AS action_plan_end_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.expirationdate__c) AS action_plan_expiration_time,
      
      a.actionplantemplate__c AS action_plan_template_id,
      a.actionplantemplatename__c AS action_plan_template_name,
      a.actionplantype__c AS action_plan_type,
      a.playbookurl__c AS playbook_url,

      a.status__c AS action_plan_status,

      a.campaign__c AS campaign_id,
      cam.name AS campaign_name,
      cm.id AS campaign_member_id,

      a.createdbyid AS created_by_tack_id,
      CONCAT(u1.user_first_name, " ", u1.user_last_name) AS created_by_tack_user_name,
      a.ownerid AS owner_tack_id,
      CONCAT(u2.user_first_name, " ", u2.user_last_name) AS owner_tack_user_name,

      a.contact__c AS tack_contact_id,
      SAFE_CAST(SAFE_CAST(c.usr_user_id__c AS FLOAT64) AS INT64) AS tack_contact_user_id,
      SAFE_CAST(c.user_pk_id__c AS INT64) AS tack_contact_user_pk,
      
      a.parentaccount__c AS tack_parent_account_id,
      SAFE_CAST(SAFE_CAST(tpa.usr_user_id__c AS FLOAT64) AS INT64) AS tack_parent_account_user_id,
      SAFE_CAST(tpa.user_pk_id__c AS INT64) AS tack_parent_account_user_pk,
      
      a.account__c AS tack_account_id,
      SAFE_CAST(SAFE_CAST(ta.usr_user_id__c AS FLOAT64) AS INT64) AS tack_account_user_id,
      SAFE_CAST(ta.user_pk_id__c AS INT64) AS tack_account_user_pk,
      
      a.lead__c AS tack_lead_id,
      a.service__c AS tack_service_id,
      
      SAFE_CAST(a.totalnumberofactionitems__c AS FLOAT64) AS total_action_plan_items,
      SAFE_CAST(a.numberofcompleteactionitems__c AS FLOAT64) AS number_completed_action_plan_items,
      SAFE_CAST(a.percentcomplete__c AS FLOAT64) AS percent_action_plan_complete,
      
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.lastcheckindate__c) AS last_checkin_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.lastattempt__c) AS last_attempt_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.nextcallscheduled__c) AS next_call_scheduled_time,

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

      a.lastmodifiedbyid AS last_modified_by_id,
      CONCAT(u3.user_first_name, " ", u3.user_last_name) AS last_modified_by_tack_user_name,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.lastmodifieddate) AS last_modified_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.lastactivitydate) AS last_activity_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.lastreferenceddate) AS last_referenced_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.lastvieweddate) AS last_viewed_time,
      PARSE_TIMESTAMP('%m/%d/%Y %T', a.systemmodstamp) AS system_modstamp_time,
      a.recordtypeid AS tack_record_type_id,

      SAFE_CAST(a.isdeleted AS INT64) AS action_plan_is_deleted

FROM `tt-dp-prod`.tack.action_plan a
LEFT JOIN `tt-dp-prod`.ops.tack_user u1 ON a.createdbyid = u1.tack_user_id
LEFT JOIN `tt-dp-prod`.ops.tack_user u2 ON a.ownerid = u2.tack_user_id
LEFT JOIN `tt-dp-prod`.ops.tack_user u3 ON a.lastmodifiedbyid = u3.tack_user_id
LEFT JOIN `tt-dp-prod`.tack.contact c ON a.contact__c = c.id
LEFT JOIN `tt-dp-prod`.tack.account tpa ON a.parentaccount__c = tpa.id
LEFT JOIN `tt-dp-prod`.tack.account ta ON a.account__c = ta.id
LEFT JOIN `tt-dp-prod`.tack.campaign cam ON a.campaign__c = cam.id
LEFT JOIN `tt-dp-prod`.tack.campaign_member cm ON a.contact__c = cm.contactid AND cam.id = cm.campaignid

WHERE istestdata__c = '0'
  AND actionplantemplate__c IS NOT NULL
  AND actionplantemplate__c NOT IN ('a1q0Z000006HosAQAS','a1q0Z000006HosjQAC','a1q0Z000006HostQAC','a1q0Z000006HoupQAC')


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
