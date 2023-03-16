library(mlr3)
library(data.table)
library(AzureStor)




# linex measure
source("Linex.R")
mlr_measures$add("linex", Linex)

# setup
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
mlr3_save_path = "D:/mlfin/cvresults-pead"

# read predictors
DT <- fread("D:/features/pead-predictors.csv")

# create group variable
DT[, monthid := paste0(data.table::year(as.Date(date, origin = "1970-01-01")),
                       data.table::month(as.Date(date, origin = "1970-01-01")))]
DT[, monthid := as.integer(monthid)]
setorder(DT, monthid)

# define predictors
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "bmo_return", "amc_return",
                       "open", "high", "low", "close", "volume", "returns",
                       "monthid"
)
targets <- c(colnames(DT)[grep("ret_excess", colnames(DT))])
cols_features <- setdiff(colnames(DT), c(cols_non_features, targets))

# convert columns to numeric. This is important only if we import existing features
chr_to_num_cols <- setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
print(chr_to_num_cols)
DT <- DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]

# remove constant columns in set and remove same columns in test set
features_ <- DT[, ..cols_features]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
cols_features <- setdiff(cols_features, remove_cols)

# convert variables with low number of unique values to factors
int_numbers = DT[, ..cols_features][, lapply(.SD, function(x) all(as.integer(x)==x) & x > 0.99)]
int_cols = na.omit(colnames(DT[, ..cols_features])[as.matrix(int_numbers)[1,]])
factor_cols = DT[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
factor_cols = as.matrix(factor_cols)[1, ]
factor_cols = factor_cols[factor_cols <= 100]
DT = DT[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]

# remove observations with missing target
DT = na.omit(DT, cols = setdiff(targets, colnames(DT)[grep("extreme", colnames(DT))]))

# sort
setorder(DT, date)

# add rowid column
DT[, row_ids := 1:.N]

# check saved files
res_files = file.info(list.files(mlr3_save_path, full.names = TRUE))
res_files = res_files[order(res_files$ctime), ]

### REGRESSION
# task with future week returns as target
target_ = colnames(DT)[grep("^ret_excess_stand_5", colnames(DT))]
cols_ = c(target_, "symbol", "monthid", cols_features)
task_ret_week <- as_task_regr(DT[, ..cols_],
                              id = "task_ret_week",
                              target = target_)

# outer custom rolling window resampling
outer_split <- function(task, train_length = 36, test_length = 2, test_length_out = 1) {
  customo = rsmp("custom")
  task_ <- task$clone()
  groups = cbind(id = 1:task_$nrow, task_$data(cols = "monthid"))
  groups_v = groups[, unique(monthid)]
  rm(task_)
  insample_length = train_length + test_length
  train_groups_out <- lapply(1:(length(groups_v)-train_length-test_length), function(x) groups_v[x:(x+insample_length)])
  test_groups_out <- lapply(1:(length(groups_v)-train_length-test_length),
                            function(x) groups_v[(x+insample_length):(x+insample_length+test_length_out-1)])
  train_sets_out <- lapply(train_groups_out, function(mid) groups[monthid %in% mid, id])
  test_sets_out <- lapply(test_groups_out, function(mid) groups[monthid %in% mid, id])
  customo$instantiate(task, train_sets_out, test_sets_out)
}
customo = outer_split(task_ret_week)


# function to check infividual benchmark result
bmrs = lapply(rownames(res_files[2:nrow(res_files),]), readRDS)

# get predictions function
get_predictions_by_task = function(bmr, DT) {
  bmr_dt = as.data.table(bmr)
  task_names = lapply(bmr_dt$task, function(x) x$id)
  # task_names = lapply(bmr_dt$task, function(x) x$id)
  predictions = lapply(bmr_dt$prediction, function(x) as.data.table(x))
  names(predictions) <- task_names
  predictions <- lapply(predictions, function(x) {
    x = DT[, .(row_ids, symbol, date)][x, on = "row_ids"]
    x[, date := as.Date(date, origin = "1970-01-01")]
  })
  return(predictions)
}
predictions = lapply(bmrs, get_predictions_by_task, DT = DT)

# choose task
task_name = "task_ret_week"
predictions_task = lapply(predictions, function(x) {
  x[[task_name]]
})
predictions_task <- rbindlist(predictions_task)

# save to azure for QC backtest
predictions_qc <- predictions_task[, .(symbol, date, response)]
predictions_qc = predictions_qc[, .(symbol = paste0(symbol, collapse = "|"),
                                    response = paste0(response, collapse = "|")), by = date]
predictions_qc[, date := as.character(date)]
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(predictions_qc, cont, "pead_task_ret_week.csv")
universe = predictions_qc[, .(date, symbol)]
storage_write_csv(universe, cont, "pead_task_ret_week_universe.csv", col_names = FALSE)

# performance by varioues measures
bmr_$aggregate(msrs(c("regr.mse", "linex", "regr.mae")))
predicitons = as.data.table(as.data.table(bmr_)[, "prediction"][1][[1]][[1]])

# prrformance by hit ratio
predicitons[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign = as.factor(sign(response))
)]
mlr3measures::acc(predicitons$truth_sign, predicitons$response_sign)

# hiy ratio for high predicted returns
predicitons_sample = predicitons[response > 0.1]
mlr3measures::acc(predicitons_sample$truth_sign, predicitons_sample$response_sign)

# cumulative returns for same sample
predicitons_sample[, .(
  benchmark = mean(predicitons$truth),
  strategy  = mean(truth)
)]

# important predictors
bmr_ = bmrs[[18]]
lapply(1:2, function(i) {
  resample_res = as.data.table(bmr_$resample_result(i))
  resample_res$learner[[1]]$state$model$learner$state$model$gausscov_f1st$features
})

# performance for every learner
resample_res$learner[[1]]$state$model$learner$state$model$ranger.ranger$model$predictions
as.data.table(bmr_)




