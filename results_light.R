library(fs)
library(data.table)
library(mlr3batchmark)
library(batchtools)


# load registry
reg = loadRegistry("F:/H4-v9", work.dir="F:/H4-v9")

# used memory
reg$status[!is.na(mem.used)]
reg$status[, max(mem.used, na.rm = TRUE)]

# done jobs
ids_done = findDone(reg=reg)
ids_notdone = findNotDone(reg=reg)

# get results
results = lapply(ids_done[, job.id], function(id_) {
  # bmr object
  bmrs = reduceResultsBatchmark(id_, store_backends = FALSE, reg = reg)
  bmrs_dt = as.data.table(bmrs)

  # get predictions
  task_names = vapply(bmrs_dt$task, `[[`, FUN.VALUE = character(1L), "id")
  resample_names = vapply(bmrs_dt$resampling, `[[`, FUN.VALUE = character(1L), "id")
  cv_names = gsub("custom_|_.*", "", resample_names)
  fold_names = gsub("custom_\\d+_", "", resample_names)
  learner_names = vapply(bmrs_dt$learner, `[[`, FUN.VALUE = character(1L), "id")
  learner_names = gsub(".*\\.regr\\.|\\.tuned", "", learner_names)
  predictions = as.data.table(bmrs_dt$prediction[[1]])
  setnames(predictions, "response.V1", "response", skip_absent=TRUE)
  predictions = cbind(task_names, learner_names, cv_names, fold_names, predictions)
  # predictions = lapply(seq_along(predictions), function(j)
  #   cbind(task = task_names[[j]],
  #         learner = learner_names[[j]],
  #         cv = cv_names[[j]],
  #         fold = fold_names[[j]],
  #         predictions[[j]]))

  return(predictions)
})
predictions = rbindlist(results, fill = TRUE)

# import tasks
tasks_files = dir_ls("F:/H4-v9/problems")
tasks = lapply(tasks_files, readRDS)
names(tasks) = lapply(tasks, function(t) t$data$id)
tasks

# backends
get_backend = function(task_name = "taskRetWeek") {
  task_ = tasks[names(tasks) == task_name][[1]]
  task_ = task_$data$backend
  task_ = task_$data(rows = task_$rownames, cols = id_cols)
  return(task_)
}
id_cols = c("symbol", "date", "yearmonthid", "..row_id", "epsDiff", "nincr", "nincr2y", "nincr3y")
taskRetWeek    = get_backend()
taskRetMonth   = get_backend("taskRetMonth")
taskRetMonth2  = get_backend("taskRetMonth2")
taskRetQuarter = get_backend("taskRetQuarter")
test = all(c(identical(taskRetWeek, taskRetMonth),
             identical(taskRetWeek, taskRetMonth2),
             identical(taskRetWeek, taskRetQuarter)))
print(test)
if (test) {
  backend = copy(taskRetWeek)
  setnames(backend, "..row_id", "row_ids")
  rm(list = c("taskRetWeek", "taskRetMonth", "taskRetMonth2", "taskRetQuarter"))
  rm(list = c("task_ret_week", "task_ret_month", "task_ret_month2", "task_ret_quarter"))
}

# measures
source("Linex.R")
source("AdjLoss2.R")
source("PortfolioRet.R")
mlr_measures$add("linex", Linex)
mlr_measures$add("adjloss2", AdjLoss2)
mlr_measures$add("portfolio_ret", PortfolioRet)

# merge backs and predictions
predictions = backend[predictions, on = c("row_ids")]
predictions[, date := as.Date(date)]
setnames(predictions,
         c("task_names", "learner_names", "cv_names"),
         c("task", "learner", "cv"),
         skip_absent = TRUE)


# PREDICTIONS RESULTS -----------------------------------------------------

# predictions
# predictions_l_naomit = predictions_l[-which(na_test)]
predictions[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign = as.factor(sign(response))
)]

# number of predictions by task and cv
unique(predictions, by = c("task", "learner", "cv", "row_ids"))[, .N, by = c("task")]
unique(predictions, by = c("task", "learner", "cv", "row_ids"))[, .N, by = c("task", "cv")]

# accuracy by ids
measures = function(t, res) {
  list(acc   = mlr3measures::acc(t, res),
       fbeta = mlr3measures::fbeta(t, res, positive = "1"),
       tpr   = mlr3measures::tpr(t, res, positive = "1"),
       tnr   = mlr3measures::tnr(t, res, positive = "1"))
}
predictions[, measures(truth_sign, response_sign), by = c("cv")]
predictions[, measures(truth_sign, response_sign), by = c("task")]
predictions[, measures(truth_sign, response_sign), by = c("learner")]
predictions[, measures(truth_sign, response_sign), by = c("cv", "task")]
predictions[, measures(truth_sign, response_sign), by = c("cv", "learner")]
# predictions[, measures(truth_sign, response_sign), by = c("cv", "task", "learner")][order(V1)]

# hit ratio for ensamble
predictions_ensemble = predictions[, .(
  mean_response = mean(response),
  median_response = median(response),
  sign_response = sum(sign(response)),
  sd_response = sd(response),
  truth = mean(truth),
  symbol = symbol,
  date = date,
  yearmonthid = yearmonthid,
  epsDiff = epsDiff
),
by = c("task", "row_ids")]
predictions_ensemble[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign_median = as.factor(sign(median_response)),
  response_sign_mean = as.factor(sign(mean_response))
  # response_sign_sign_pos = sign_response > 15,
  # response_sign_sign_neg = sign_response < -15
)]
predictions_ensemble = unique(predictions_ensemble, by = c("task", "row_ids"))
sign_response_max = predictions_ensemble[, max(sign_response)]
sign_response_seq = seq(as.integer(sign_response_max / 2), sign_response_max - 1)
cols_sign_response_pos = paste0("response_sign_sign_pos", sign_response_seq)
predictions_ensemble[, (cols_sign_response_pos) := lapply(sign_response_seq, function(x) sign_response > x)]
cols_sign_response_neg = paste0("response_sign_sign_neg", sign_response_seq)
predictions_ensemble[, (cols_sign_response_neg) := lapply(sign_response_seq, function(x) sign_response < -x)]
# cols_ = colnames(predictions_dt_ensemble)[24:ncol(predictions_dt_ensemble)]
# predictions_dt_ensemble[, lapply(.SD, function(x) sum(x == TRUE)), .SDcols = cols_]

# check only sign ensamble performance
res = lapply(cols_sign_response_pos, function(x) {
  predictions_ensemble[get(x) == TRUE][, mlr3measures::acc(truth_sign, factor(as.integer(get(x)), levels = c(-1, 1))), by = c("task")]
})
names(res) = cols_sign_response_pos
res

# check only sign ensamble performance all
res = lapply(cols_sign_response_pos, function(x) {
  predictions_ensemble[get(x) == TRUE][, mlr3measures::acc(truth_sign, factor(as.integer(get(x)), levels = c(-1, 1)))]
})
names(res) = cols_sign_response_pos
res


# IMPORTANT VARIABLES -----------------------------------------------------
# gausscov files
gausscov_files = dir_ls("F:/H4-v9-gausscov")

# arrange files
task_ = gsub(".*f3-|-\\d+.rds", "", gausscov_files)
gausscov_dt = cbind.data.frame(gausscov_files, task = task_)
setorder(gausscov_dt, task)
gausscov_dt[gausscov_dt$task == "taskRetWeek",]
gausscov_dt[gausscov_dt$task == "taskRetMonth",]
gausscov_dt[gausscov_dt$task == "taskRetMonth2",]
gausscov_dt[gausscov_dt$task == "taskRetQuarter",]

# import gausscov vars
gausscov_l = lapply(gausscov_dt[, "gausscov_files"], readRDS)
gausscov = lapply(gausscov_l, function(x) x[x > 0])
names(gausscov) = gausscov_dt[, "task"]
gausscov = lapply(gausscov, function(x) as.data.frame(as.list(x)))
gausscov = lapply(gausscov, melt)
gausscov = rbindlist(gausscov, idcol = "task")

# most important vars across all tasks
gausscov[, sum(value), by = variable][order(V1)][, tail(.SD, 10)]
gausscov[, sum(value), by = .(task, variable)][order(V1)][, tail(.SD, 5), by = task]



