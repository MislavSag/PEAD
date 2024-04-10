library(fs)
library(data.table)
library(mlr3verse)
library(mlr3batchmark)
library(batchtools)
library(duckdb)
library(PerformanceAnalytics)
library(AzureStor)
library(future.apply)
library(matrixStats)
library(finautoml)


# Creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)

# globals
# PATH = "F:/padobran/pre_live"
PATH = "./experiments_pread_live"

# load registry
reg = loadRegistry(PATH, work.dir=PATH)

# used memory
reg$status[!is.na(mem.used)]
reg$status[, max(mem.used, na.rm = TRUE)]

# Check jobs
results_files = fs::path_ext_remove(fs::path_file(dir_ls(fs::path(PATH, "results"))))
ids_done = findDone(reg=reg)
ids_done = ids_done[job.id %in% results_files]
ids_notdone = findNotDone(reg=reg)

# get results
tabs = batchtools::getJobTable(ids_done, reg = reg)[
  , c("job.id", "job.name", "repl", "prob.pars", "algo.pars"), with = FALSE]
predictions_meta = cbind.data.frame(
  id = tabs[, job.id],
  task = vapply(tabs$prob.pars, `[[`, character(1L), "task_id"),
  learner = gsub(".*regr.|.tuned", "", vapply(tabs$algo.pars, `[[`, character(1L), "learner_id")),
  cv = gsub("custom_|_.*", "", vapply(tabs$prob.pars, `[[`, character(1L), "resampling_id")),
  fold = gsub("custom_\\d+_", "", vapply(tabs$prob.pars, `[[`, character(1L), "resampling_id"))
)
predictions_l = lapply(unlist(ids_done), function(id_) {
  # id_ = 10035
  x = tryCatch({readRDS(fs::path(PATH, "results", id_, ext = "rds"))},
               error = function(e) NULL)
  if (is.null(x)) {
    print(id_)
    return(NULL)
  }
  x["id"] = id_
  x
})
predictions = lapply(predictions_l, function(x) {
  cbind.data.frame(
    id = x$id,
    row_ids = x$prediction$test$row_ids,
    truth = x$prediction$test$truth,
    response = x$prediction$test$response
  )
})
predictions = rbindlist(predictions)
predictions = merge(predictions_meta, predictions, by = "id")
predictions = as.data.table(predictions)

# Import tasks
tasks_files = dir_ls(fs::path(PATH, "problems"))
tasks = lapply(tasks_files, readRDS)
names(tasks) = lapply(tasks, function(t) t$data$id)
tasks

# Backends
id_cols = c("symbol", "date", "..row_id", "target")
backend = tasks[[1]]$data$backend
backend$data(rows = backend$rownames, cols = id_cols)
backend = backend$data(rows = backend$rownames, cols = id_cols)
setnames(backend, "..row_id", "row_ids")

# Measures
source("AdjLoss2.R")
source("PortfolioRet.R")
mlr_measures$add("linex", finautoml::Linex)
mlr_measures$add("adjloss2", AdjLoss2)
mlr_measures$add("portfolio_ret", PortfolioRet)

# Merge backs and predictions
predictions = backend[predictions, on = c("row_ids")]
predictions[, date := as.Date(date)]
setnames(predictions,
         c("task_names", "learner_names", "cv_names"),
         c("task", "learner", "cv"),
         skip_absent = TRUE)

# Get start and end date
start_date = predictions[, min(date)]
end_date = predictions[, max(date)]
