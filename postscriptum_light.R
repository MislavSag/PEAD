library(data.table)
library(mlr3verse)
library(AzureStor)




# SETUP -------------------------------------------------------------------
# creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)


# BACKEND -----------------------------------------------------------------
# utils
monnb <- function(d) {
  lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
  lt$year*12 + lt$mon }
mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }
snakeToCamel <- function(snake_str) {
  # Replace underscores with spaces
  spaced_str <- gsub("_", " ", snake_str)

  # Convert to title case using tools::toTitleCase
  title_case_str <- tools::toTitleCase(spaced_str)

  # Remove spaces and make the first character lowercase
  camel_case_str <- gsub(" ", "", title_case_str)
  camel_case_str <- sub("^.", tolower(substr(camel_case_str, 1, 1)), camel_case_str)

  # I haeve added this to remove dot
  camel_case_str <- gsub("\\.", "", camel_case_str)

  return(camel_case_str)
}

# define backends
data_tbl = fread("./pead-predictors-update.csv")
DT = as.data.table(data_tbl)
DT[, date_rolling := as.IDate(date_rolling)]
DT[, yearmonthid := round(date_rolling, digits = "month")]
DT[, .(date, date_rolling, yearmonthid)]
DT[, yearmonthid := as.integer(yearmonthid)]
DT[, .(date, date_rolling, yearmonthid)]
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "bmo_return", "amc_return",
                       "open", "high", "low", "close", "volume", "returns",
                       "yearmonthid", "date_rolling"
)
targets <- c(colnames(DT)[grep("ret_excess", colnames(DT))])
cols_features <- setdiff(colnames(DT), c(cols_non_features, targets))
cols_features_new = vapply(cols_features, snakeToCamel, FUN.VALUE = character(1L), USE.NAMES = FALSE)
setnames(DT, cols_features, cols_features_new)
cols_features = cols_features_new
targets_new = vapply(targets, snakeToCamel, FUN.VALUE = character(1L), USE.NAMES = FALSE)
setnames(DT, targets, targets_new)
targets = targets_new
cols_features_ <- gsub('[\\"/]', '', cols_features) # Remove double quotes, backslashes, and forward slashes
cols_features_ <- gsub('[[:cntrl:]]', '', cols_features_) # Remove control characters
cols_features_ <- gsub('^\\w\\-\\.', '', cols_features_) # Remove control characters
chr_to_num_cols <- setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
print(chr_to_num_cols)
DT <- DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
features_ <- DT[, ..cols_features]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
cols_features <- setdiff(cols_features, remove_cols)
int_numbers = na.omit(DT[, ..cols_features])[, lapply(.SD, function(x) all(floor(x) == x))]
int_cols = colnames(DT[, ..cols_features])[as.matrix(int_numbers)[1,]]
factor_cols = DT[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
factor_cols = as.matrix(factor_cols)[1, ]
factor_cols = factor_cols[factor_cols <= 100]
DT = DT[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]
DT = na.omit(DT, cols = setdiff(targets, colnames(DT)[grep("xtreme", colnames(DT))]))
DT[, date := as.POSIXct(date, tz = "UTC")]
DT = DT[order(yearmonthid)]

# tasks
id_cols = c("symbol", "date", "yearmonthid")
DT[, date := as.POSIXct(date, tz = "UTC")]
target_ = colnames(DT)[grep("^ret.*xcess.*tand.*5", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_week <- as_task_regr(DT[, ..cols_],
                              id = "taskRetWeek",
                              target = target_)
target_ = colnames(DT)[grep("^ret.*xcess.*tand.*22", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_month <- as_task_regr(DT[, ..cols_],
                               id = "taskRetMonth",
                               target = target_)
target_ = colnames(DT)[grep("^ret.*xcess.*tand.*44", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_month2 <- as_task_regr(DT[, ..cols_],
                                id = "taskRetMonth2",
                                target = target_)
target_ = colnames(DT)[grep("^ret.*xcess.*tand.*66", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_quarter <- as_task_regr(DT[, ..cols_],
                                 id = "taskRetQuarter",
                                 target = target_)
task_ret_week$col_roles$feature = setdiff(task_ret_week$col_roles$feature,
                                          id_cols)
task_ret_month$col_roles$feature = setdiff(task_ret_month$col_roles$feature,
                                           id_cols)
task_ret_month2$col_roles$feature = setdiff(task_ret_month2$col_roles$feature,
                                            id_cols)
task_ret_quarter$col_roles$feature = setdiff(task_ret_quarter$col_roles$feature,
                                             id_cols)

# backends
ids_ = c("symbol", "date", "yearmonthid", "..row_id", "epsDiff", "nincr", "nincr2y", "nincr3y")
taskRetWeek    = task_ret_week$backend$data(rows = task_ret_week$backend$rownames, cols = ids_)
taskRetMonth   = task_ret_month$backend$data(rows = task_ret_month$backend$rownames, cols = ids_)
taskRetMonth2  = task_ret_month2$backend$data(rows = task_ret_month2$backend$rownames, cols = ids_)
taskRetQuarter = task_ret_quarter$backend$data(rows = task_ret_quarter$backend$rownames, cols = ids_)
test = all(c(identical(taskRetWeek, taskRetMonth), identical(taskRetWeek, taskRetMonth2), identical(taskRetWeek, taskRetQuarter)))
print(test)
if (test) {
  backend = copy(taskRetWeek)
  setnames(backend, "..row_id", "row_ids")
  rm(list = c("taskRetWeek", "taskRetMonth", "taskRetMonth2", "taskRetQuarter"))
}
rm(list = c("task_ret_week", "task_ret_month", "task_ret_month2", "task_ret_quarter"))

# measures
source("Linex.R")
source("AdjLoss2.R")
mlr_measures$add("linex", Linex)
mlr_measures$add("adjloss2", AdjLoss2)


# RESULTS -----------------------------------------------------------------
# utils
id_cols = c("symbol", "date", "yearmonthid", "..row_id")

# set files with benchmarks
bmr_files = list.files(list.files("F:", pattern = "^H4-v4", full.names = TRUE), full.names = TRUE)

# arrange files
cv_ = as.integer(gsub("\\d+-", "", gsub(".*/|-\\d+.rds", "", bmr_files)))
i_ = as.integer(gsub("-\\d+", "", gsub(".*/|-\\d+.rds", "", bmr_files)))
bmr_files = cbind.data.frame(bmr_files, cv = cv_, i = i_)
setorder(bmr_files, cv, i)

# extract needed information from banchmark objects
predictions_l = list()
aggs_l = list()
# imp_features_corr_l = list()
for (i in 1:nrow(bmr_files)) {
  # debug
  # i = 1
  print(i)

  # get bmr object
  bmr = readRDS(bmr_files$bmr_files[i])
  bmr_dt = as.data.table(bmr)

  # aggregate performances
  agg_ = bmr$aggregate(msrs(c("regr.mse", "regr.mae", "adjloss2", "linex")))
  cols = c("task_id", "learner_id", "iters", colnames(agg_)[7:length(colnames(agg_))])
  agg_ = agg_[, learner_id := gsub(".*regr\\.|\\.tuned", "", learner_id)][, ..cols]

  # get predictions
  task_names = lapply(bmr_dt$task, `[[`, "id")
  learner_names = lapply(bmr_dt$learner, `[[`, "id")
  learner_names = gsub(".*\\.regr\\.|\\.tuned", "", learner_names)
  predictions = lapply(bmr_dt$prediction, function(x) as.data.table(x))
  predictions = lapply(seq_along(predictions), function(j)
    cbind(task = task_names[[j]],
          learner = learner_names[[j]],
          predictions[[j]]))

  # merge backs and predictions
  predictions <- lapply(seq_along(predictions), function(j) {
    y = backend[predictions[[j]], on = c("row_ids")]
    y[, date := as.Date(date, origin = "1970-01-01")]
    y
  })
  predictions = rbindlist(predictions)

  # add meta
  predictions = cbind(cv = bmr_files$cv[i],
                      i = bmr_files$i[i],
                      predictions)

  # predictions
  predictions_l[[i]] = predictions
  aggs_l[[i]] = agg_
}

# aggregated results
aggregate_results = rbindlist(aggs_l, fill = TRUE)
cols = colnames(aggregate_results)[4:ncol(aggregate_results)]
aggregate_results[, lapply(.SD, function(x) mean(x)), by = .(task_id, learner_id), .SDcols = cols]

# hit ratio
# predictions_dt = rbindlist(lapply(bmrs, function(x) x$predictions), idcol = "fold")
predictions_dt = rbindlist(predictions_l)
predictions_dt[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign = as.factor(sign(response))
)]
setorderv(predictions_dt, c("cv", "i"))
predictions_dt[, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task", "learner")]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task", "learner")]
predictions_dt[response > 0.2, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task", "learner")]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task", "learner")]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign), by = c("cv", "task", "learner")]

# hit ratio for ensamble
predictions_dt_ensemble = predictions_dt[, .(mean_response = mean(response),
                                             median_response = median(response),
                                             sign_response = sum(sign(response)),
                                             sd_response = sd(response),
                                             truth = mean(truth),
                                             symbol = symbol,
                                             date = date,
                                             yearmonthid = yearmonthid,
                                             epsDiff = epsDiff),
                                         by = c("task", "row_ids")]
predictions_dt_ensemble[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign_median = as.factor(sign(median_response)),
  response_sign_mean = as.factor(sign(mean_response))
  # response_sign_sign_pos = sign_response > 15,
  # response_sign_sign_neg = sign_response < -15
)]
sign_response_max = predictions_dt_ensemble[, max(sign_response)]
sign_response_seq = seq(as.integer(sign_response_max / 2), sign_response_max - 1)
cols_sign_response_pos = paste0("response_sign_sign_pos", sign_response_seq)
predictions_dt_ensemble[, (cols_sign_response_pos) := lapply(sign_response_seq, function(x) sign_response > x)]

# check only sign ensamble performance
lapply(cols_sign_response_pos, function(x) {
  predictions_dt_ensemble[get(x) == TRUE][, mlr3measures::acc(truth_sign, factor(as.integer(get(x)), levels = c(-1, 1))), by = "task"]
})


# predictions_dt_ensemble[, response_sign_sd_q := quantile(sd_response, probs = 0.05), by = "task"]
# predictions_dt_ensemble[, mfd := as.factor(ifelse(sd_response < response_sign_sd_q, 1, -1))] # machine forecast dissagreement
#
# ids_ = c("task")
# predictions_dt_ensemble[, mlr3measures::acc(truth_sign, response_sign_median), by = ids_]
# predictions_dt_ensemble[, mlr3measures::acc(truth_sign, response_sign_mean), by = ids_]
# predictions_dt_ensemble[, mlr3measures::acc(truth_sign, mfd), by = ids_]
# predictions_dt_ensemble[response_sign_sign_pos == TRUE][, mlr3measures::acc(truth_sign, factor(as.integer(response_sign_sign_pos), levels = c(-1, 1))), by = ids_]
# predictions_dt_ensemble[response_sign_sign_neg == TRUE][, mlr3measures::acc(truth_sign, factor(-as.integer(response_sign_sign_neg), levels = c(-1, 1))), by = ids_]
#
# predictions_dt_ensemble[median_response > 0.1, mlr3measures::acc(truth_sign, response_sign_median), by = ids_]
# predictions_dt_ensemble[mean_response > 0.1, mlr3measures::acc(truth_sign, response_sign_mean), by = ids_]
# predictions_dt_ensemble[median_response > 0.5, mlr3measures::acc(truth_sign, response_sign_median), by = ids_]
# predictions_dt_ensemble[mean_response > 0.5, mlr3measures::acc(truth_sign, response_sign_mean), by = ids_]
# predictions_dt_ensemble[median_response > 1, mlr3measures::acc(truth_sign, response_sign_median), by = ids_]
# predictions_dt_ensemble[mean_response > 1, mlr3measures::acc(truth_sign, response_sign_mean), by = ids_]
# predictions_dt_ensemble[sd_response > 1, mlr3measures::acc(truth_sign, response_sign_mean), by = ids_]

# best
# predictions_dt_ensemble[response_sign_sign_pos == TRUE][, mlr3measures::acc(truth_sign, factor(as.integer(response_sign_sign_pos), levels = c(-1, 1))), by = ids_]
# predictions_dt_ensemble[response_sign_sign_pos == TRUE & epsDiff > 0][, mlr3measures::acc(truth_sign, factor(as.integer(response_sign_sign_pos), levels = c(-1, 1))), by = ids_]
# predictions_dt_ensemble[response_sign_sign_pos == TRUE & epsDiff > 0 & sd_response > 2][, mlr3measures::acc(truth_sign, factor(as.integer(response_sign_sign_pos), levels = c(-1, 1))), by = ids_]
#
# predictions_dt_ensemble[response_sign_sign_neg == TRUE][, mlr3measures::acc(truth_sign, factor(-as.integer(response_sign_sign_neg), levels = c(-1, 1))), by = ids_]
# predictions_dt_ensemble[response_sign_sign_neg == TRUE & epsDiff < 0][, mlr3measures::acc(truth_sign, factor(-as.integer(response_sign_sign_neg), levels = c(-1, 1))), by = ids_]


# save to azure for QC backtest
cont = storage_container(BLOBENDPOINT, "qc-backtest")
lapply(unique(predictions_dt_ensemble$task), function(x) {
  # debug
  # x = "taskRetQuarter"

  # prepare data
  y = predictions_dt_ensemble[task == x]
  cols = colnames(y)[grep("response_sign", colnames(y))]
  cols = c("symbol", "date", "epsDiff", cols)
  y = y[, ..cols]
  y = unique(y)

  # remove where all false
  y = y[response_sign_sign_pos9 == TRUE]

  # by date
  # cols_ = setdiff(cols, "date")
  # y = y[, lapply(.SD, function(x) paste0(x, collapse = "|")), by = date]
  # y[, date := as.character(date)]
  # setorder(y, date)

  # y = y[, .(
  #   symbol = paste0(symbol, collapse = "|"),
  #   response = paste0(response, collapse = "|"),
  #   epsdiff = paste0(epsDiff, collapse = "|"),
  #
  # ), by = date]

  # order
  setorder(y, date)

  # save to azure blob
  print(colnames(y))
  file_name_ =  paste0("pead-", x, ".csv")
  storage_write_csv(y, cont, file_name_)
  # universe = y[, .(date, symbol)]
  # storage_write_csv(universe, cont, "pead_task_ret_week_universe.csv", col_names = FALSE)
})

# systemic risk
predictors_pos = predictions_dt_ensemble[, .(response_sign_sign_pos_agg = sum(response_sign_sign_pos)), by = "date"]
predictors_neg = predictions_dt_ensemble[, .(response_sign_sign_pos_agg = sum(response_sign_sign_neg)), by = "date"]
setorder(predictors_pos, date)
setorder(predictors_neg, date)
predictors_diff = as.xts.data.table(predictors_pos) - as.xts.data.table(predictors_neg)
plot(as.xts.data.table(na.omit(predictors_pos)))
plot(as.xts.data.table(na.omit(predictors_neg)))
plot(predictors_diff)
