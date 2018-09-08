getwd()
setwd("D:/R")
getwd()

package_list <- c("bigrquery", "googlesheets", "httpuv", "dplyr", "data.table","gmailr")
for(package in package_list){
  if(!require(package, character.only = TRUE)){
    install.packages(package, repos='http://cran.us.r-project.org');
  }
}


library("gmailr")
library("data.table")
library("bigrquery")
library("googlesheets")

p <- "D:/R/temp_files/"
setwd(p)

message_ids <- messages(search = "from:reports-noreply@five9.com", num_results = NULL, user_id = "me")

mlist <- rbindlist(message_ids[[1]][[1]], fill=TRUE)
mlist$id
mlist$id[1]


for(m in 1:length(mlist$id)) { 
  print(m) 
  m
  mes <- message(mlist$id[m])
  print(mes)
  subject <- print(mes$payload$headers[[18]]$value)
  z <- save_attachments(mes,path = p)
  if(subject == "Scheduled Report: Status Log - 20180215 - Export Test") {
    default_name <- "Status Log - 20180215 - Export Test.csv"
    new_name <- paste("Status Log - 20180215 - Export Test - ",as.character(m),".csv", sep = "")
    file.rename(default_name, new_name)
  }
  if(subject == "Scheduled Report: Call Log - BQ - 20180209 - 2 - Export Test") {
    default_name <- "Call Log - BQ - 20180209 - 2 - Export Test.csv"
    new_name <- paste("Call Log - BQ - 20180209 - 2 - Export Test - ",as.character(m),".csv", sep = "")
    file.rename(default_name, new_name)
  }
}



filelist <- c(list.files(p,all.files = FALSE, full.names = FALSE))

var_names_status <- c(
  "agent"
  ,"agent_email"
  ,"agent_first_name"
  ,"agent_group"
  ,"agent_id"
  ,"agent_last_name"
  ,"agent_name"
  ,"date"
  ,"timestamp"
  ,"agent_states"
  ,"available_for_all"
  ,"available_for_call"
  ,"available_for_vm"
  ,"media_availability"
  ,"reason_code"
  ,"skill_availability"
  ,"state"
  ,"unavailable_for_calls"
  ,"unavailable_for_vm"
  ,"agent_state_time"
  ,"login_timestamp"
  ,"logout_timestamp"
  ,"on_call_time"
  ,"on_voicemail_time"
  ,"paid_time"
  ,"ready_time"
  ,"ringing_time"
  ,"unpaid_time"
  ,"video_time"
  ,"vm_in_progress_time"
  ,"wait_time"
  ,"agent_disconnects_first"
  ,"call_id"
  ,"call_type"
  ,"campaign"
  ,"campaign_type"
  ,"handle_time"
  ,"extension"
  ,"on_acw_time"
  ,"not_ready_time"
  ,"manual_time"
  ,"talk_time"
  ,"after_call_work_time"
)


var_names_call <- c(
  "date",
  "date_hour",
  "day_of_month",
  "day_of_week",
  "half_hour",
  "hour",
  "hour_of_day",
  "month",
  "quarter_hour",
  "time",
  "time_interval",
  "timestamp",
  "year",
  "abandoned",
  "abandon_rate",
  "ani",
  "ani_area_code",
  "ani_country",
  "ani_country_code",
  "ani_state",
  "call_id",
  "call_survey_result",
  "call_type",
  "calls",
  "calls_completed_in_ivr",
  "calls_timed_out_in_ivr",
  "campaign",
  "campaign_type",
  "contacted",
  "customer_name",
  "disconnected_from_hold",
  "disposition",
  "dnis",
  "dnis_area_code",
  "dnis_country",
  "dnis_country_code",
  "dnis_state",
  "ivr_path",
  "list_name",
  "live_connect",
  "no_party_contact",
  "notes",
  "parent_session_id",
  "recordings",
  "service_level",
  "session_id",
  "skill",
  "speed_of_answer",
  "third_party_talk_time",
  "after_call_work_time",
  "bill_time",
  "call_time",
  "conference_time",
  "conferences",
  "consult_time",
  "cost",
  "dial_time",
  "handle_time",
  "hold_time",
  "holds",
  "ivr_time",
  "manual_time",
  "park_time",
  "parks",
  "preview_interrupted",
  "preview_interrupted_by_call",
  "preview_interrupted_by_skill_vm",
  "preview_time",
  "queue_callback_wait_time",
  "queue_wait_time",
  "rate",
  "ring_time",
  "talk_time",
  "talk_time_less_hold_park",
  "time_to_abandon",
  "total_queue_time",
  "transfers",
  "video_time",
  "voicemails",
  "voicemails_declined",
  "voicemails_deleted",
  "voicemails_handle_time",
  "voicemails_handled",
  "voicemails_returned_call",
  "voicemails_transferred",
  "agent",
  "agent_email",
  "agent_first_name",
  "agent_group",
  "agent_last_name",
  "agent_name",
  "dest_agent",
  "dest_agent_email",
  "dest_agent_extension",
  "dest_agent_first_name",
  "dest_agent_group",
  "dest_agent_last_name",
  "dest_agent_name",
  "extension",
  "call_back",
  "city",
  "company",
  "contact_create_timestamp",
  "contact_id",
  "contact_modified_timestamp",
  "email",
  "first_name",
  "last_agent",
  "last_call_date",
  "last_name",
  "number_1",
  "number_2",
  "number_3",
  "state",
  "street",
  "zip",
  "custom_caller_ani",
  "custom_campaign",
  "custom_queue"
)



i <- 0
j <- 0
for(f in filelist) {
  
  
  if(substr(f,1,4) == "Call") {
    i <- i+1
    data <- read.csv(paste(p,f, sep = ""), header = TRUE, colClasses = "character", col.names = var_names_call, na.strings=c(""))
    if(i == 1) {
      data_app_call <- data
    }
    if(i > 1) {
      data_app_call <- rbind(data_app_call,data)
    }
    
  }
  
  if(substr(f,1,4) == "Stat") {
    j <- j+1
    data <- read.csv(paste(p,f, sep = ""), header = TRUE, colClasses = "character", col.names = var_names_status, na.strings = c(""))
    if(j == 1) {
      data_app_status <- data
    }
    if(j > 1) {
      data_app_status <- rbind(data_app_status,data)
    }
    
  }
}




str(data_app_call)
str(data_app_status)



my_project <- "tt-dp-prod"

insert_upload_job(
  project=my_project
  , dataset="sandbox"
  , table="jerb_five9_call_data"
  , values=data_app_call
  , create_disposition = "CREATE_IF_NEEDED"
  , write_disposition = "WRITE_APPEND"
)


insert_upload_job(
  project=my_project
  , dataset="sandbox"
  , table="jerb_five9_agent_data"
  , values=data_app_status
  , create_disposition = "CREATE_IF_NEEDED"
  , write_disposition = "WRITE_APPEND"
)


# data_app$start_time <- test_time
# colnames(test_time)[1] <- "start_time"
# test_time <- data.frame(strptime(data_app$start_time,"%Y-%m-%d %H:%M:%S",tz="GMT"))

# x <- query_exec(
#   "SELECT * FROM `sandbox.jerb_test` LIMIT 10",
#   my_project,
#   use_legacy_sql = FALSE
# )
# x


for(m in 1:length(mlist$id)) { 
  print(m) 
  m
  print(mes)
  trash_message(mlist$id[m])
}

for(f in filelist) {
  print(f)
  file.remove(f)
}

