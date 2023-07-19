library(mlr3)
library(data.table)
library(AzureStor)




# setup
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
mlr3_save_path = "D:/mlfin/cvresults-pead-v4"

# utils
id_cols = c("symbol", "date", "yearmonthid", "..row_id")

# set files with benchmarks
results_files = file.info(list.files(mlr3_save_path, full.names = TRUE))
# res_files = res_files[order(res_files$ctime), ]
bmr_files = rownames(results_files)

# import bmrs
bmrs = lapply(bmr_files, readRDS)

# get predictions function
get_predictions = function(bmr) {

  # debug
  # bmr = bmrs[[1]]

  # bmr dt
  bmr_dt = as.data.table(bmr)

  # get backends
  task_names = unlist(lapply(bmr_dt$task, function(x) x$id))
  backs = lapply(bmr_dt$task, function(task) {
    task$backend$data(cols = c(id_cols, "eps_diff", "nincr", "nincr_2y", "nincr_3y"),
                      rows = 1:bmr_dt$task[[1]]$nrow)
  })
  names(backs) = task_names
  lapply(backs, setnames, "..row_id", "row_ids")

  # get predictions
  predictions = lapply(bmr_dt$prediction, function(x) as.data.table(x))
  names(predictions) <- task_names

  # merge backs and predictions
  predictions <- lapply(task_names, function(x) {
    y = backs[[x]][predictions[[x]], on = "row_ids"]
    y[, date := as.Date(date, origin = "1970-01-01")]
    cbind(task_name = x, y)
  })
  predictions = rbindlist(predictions)

  return(predictions)
}
predictions = lapply(bmrs, get_predictions)

# hit ratio
predictions_dt = rbindlist(predictions, idcol = "fold")
predictions_dt[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign = as.factor(sign(response))
)]
predictions_dt[, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 0.01, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 0.2, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 1.5, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 2, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response < -0.01, mlr3measures::acc(truth_sign, response_sign), by = "task_name"]
predictions_dt[response > 1]

# hit ratio with positive surprise
predictions_dt[, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, eps_diff > 0)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, eps_diff > 0)]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, eps_diff > 0)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, eps_diff > 0)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, eps_diff > 0)]

# hit ratio with positive surprise nincr
predictions_dt[, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) == TRUE)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) == TRUE)]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) == TRUE)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) == TRUE)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) == TRUE)]

# hit ratio with positive surprise nincr_2y
predictions_dt[, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y) > 6)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y) > 6)]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y)  > 6)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y)  > 6)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_2y) > 6)]

# hit ratio with positive surprise nincr_2y
predictions_dt[, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 2)]
predictions_dt[response > 0.1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 2)]
predictions_dt[response > 0.3, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 2)]
predictions_dt[response > 0.5, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr_3y) > 2)]
predictions_dt[response > 1, mlr3measures::acc(truth_sign, response_sign),
               by = .(task_name, as.integer(nincr) > 2)]

# best model
bestparams = lapply(bmrs, function(bmr) {

  # bmr dt
  bmr_dt = as.data.table(bmr)

  # get instances
  a = lapply(bmr_dt$learner, function(x) as.data.table(x$state$model$tuning_instance$result_x_domain))
  a = rbindlist(a, use.names = FALSE, idcol = "task", fill = TRUE)
  a
})
bestparams

# anlyse archives
archives = lapply(bmrs, function(bmr) {

  # bmr dt
  bmr_dt = as.data.table(bmr)

  # get instances
  a = lapply(bmr_dt$learner, function(x) as.data.table(x$state$model$tuning_instance$archive))
  a = rbindlist(a, use.names = FALSE, idcol = "task")
  a
})
archives_dt = rbindlist(archives, fill = TRUE)
archives_dt[, mean(linex ), by = prep_branch.selection]
archives_dt[, median(linex ), by = prep_branch.selection]



# compare models
# predictions_models = lapply(bmrs, function(bmr) {
#
#   # bmr dt
#   bmr_dt = as.data.table(bmr)
#
#   # get predictions
#   bmr$learners$learner[[1]]$learner$graph_model$pipeops$lm.lm$predict(list(bmr$tasks$task[[1]]))
#   bmr_dt$learner[[1]]
#
#   at$model$learner$graph_model$pipeops$ranger.ranger$predict(list(task))
#
#   at$model$learner$graph_model$pipeops$xgboost.xgboost$predict(list(task))
#
#   a = lapply(bmr_dt$learner, function(x) as.data.table(x$state$model$tuning_instance$result_x_domain))
#   a = rbindlist(a, use.names = FALSE, idcol = "task")
#   a
# })
# archives_dt[, mean(linex), by = .(task, branch_learners.selection)]
# archives_dt[, median(linex), by = .(task, branch_learners.selection)]

# save to azure for QC backtest
cont = storage_container(BLOBENDPOINT, "qc-backtest")
lapply(unique(predictions_dt$task_name), function(x) {
  # prepare data
  y = predictions_dt[task_name == x]
  y = y[, .(symbol, date, response, eps_diff)]
  y = y[, .(
    symbol = paste0(symbol, collapse = "|"),
    response = paste0(response, collapse = "|"),
    epsdiff = paste0(eps_diff, collapse = "|")
  ), by = date]
  y[, date := as.character(date)]

  # save to azure blob
  print(y)
  storage_write_csv(y, cont, paste0("pead_", x, ".csv"))
  # universe = y[, .(date, symbol)]
  # storage_write_csv(universe, cont, "pead_task_ret_week_universe.csv", col_names = FALSE)
})
