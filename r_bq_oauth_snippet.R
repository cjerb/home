token <- gs_auth(token = NULL, new_user = FALSE,
			  key = getOption("googlesheets.client_id"),
			  secret = getOption("googlesheets.client_secret"),
			  cache = getOption("googlesheets.httr_oauth_cache"), verbose = TRUE)
