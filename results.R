library(fs)
library(data.table)
library(mlr3batchmark)


# load registry
reg = loadRegistry("F:/H4-v9", work.dir="F:/H4-v9")

# add status values mannually - it will trow error below if there is no staus
ids_ = as.integer(fs::path_ext_remove(fs::path_file(dir_ls(fs::path(reg$file.dir, "results")))))
setdiff(reg$status$job.id, ids_)
reg$status[job.id %in% ids_, done := batchtools:::ustamp()]

# predictions
bmrs = reduceResultsBatchmark(ids = ids_, store_backends = FALSE, reg = reg)
bmrs_dt = as.data.table(bmrs)

# get predictions
task_names = vapply(bmrs_dt$task, `[[`, FUN.VALUE = character(1L), "id")
learner_names = vapply(bmrs_dt$learner, `[[`, FUN.VALUE = character(1L), "id")
learner_names = gsub(".*\\.regr\\.|\\.tuned", "", learner_names)
predictions = lapply(bmrs_dt$prediction, function(x) as.data.table(x))
predictions = lapply(seq_along(predictions), function(j)
  cbind(task = task_names[[j]],
        learner = learner_names[[j]],
        predictions[[j]]))

# import tasks
tasks_files = dir_ls("F:/H4-v9/problems")
tasks = lapply(tasks_files, readRDS)
names(tasks) = lapply(tasks, function(t) t$data$id)
tasks

# backends
get_backend = function(task_name = "taskRetWeek") {
  task_ = tasks[names(tasks) == task_name][[1]]
  task_ = task_$data$backend
  task_ = task_$data(rows = task_$rownames, cols = ids_)
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

# merge tasks and predictions
# merge backs and predictions
predictions <- lapply(seq_along(predictions), function(j) {
  y = backend[predictions[[j]], on = c("row_ids")]
  y[, date := as.Date(date, origin = "1970-01-01")]
  y
})
predictions = rbindlist(predictions)


# AGGREGATED RESULTS ------------------------------------------------------
# aggregated results
agg = bmrs$aggregate(msrs(c("regr.mse", "regr.mae", "linex", "adjloss2", "portfolio_ret")))
agg[, learner_id := gsub(".*\\.regr\\.|\\.tuned", "", learner_id)]
cols = colnames(agg)[7:ncol(agg)]
agg[, lapply(.SD, function(x) mean(x)), by = .(task_id, learner_id), .SDcols = cols]



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



