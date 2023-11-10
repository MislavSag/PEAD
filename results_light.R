library(fs)
library(data.table)
library(mlr3batchmark)
library(batchtools)
library(duckdb)
library(PerformanceAnalytics)
library(AzureStor)
library(future.apply)


# creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)

# globals
PATH = "F:/H4week"

# load registry
reg = loadRegistry(PATH, work.dir=PATH)

# used memory
reg$status[!is.na(mem.used)]
reg$status[, max(mem.used, na.rm = TRUE)]

# done jobs
results_files = fs::path_ext_remove(fs::path_file(dir_ls(fs::path(PATH, "results"))))
ids_done = findDone(reg=reg)
ids_done = ids_done[job.id %in% results_files]
ids_notdone = findNotDone(reg=reg)
rbind(ids_notdone, ids_done[job.id %in% results_files])

# import already saved predictions
# fs::dir_ls("predictions")
# predictions = readRDS("predictions/predictions-20231025215620.rds")

# get results
plan("multisession", workers = 4L)
start_time = Sys.time()
results = future_lapply(ids_done[, job.id], function(id_) {
  # id_ = 100
  print(id_)
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
  #         predictions[[j]]))j

  return(predictions)
})
end_time = Sys.time()
end_time - start_time
# 1: Time difference of 40.88229 secs
# 2: Time difference of 19.56966 secs
# 4 1572: Time difference of 39.13002 mins
# 8 6378:Time difference of 2.479138 hours
predictions = rbindlist(results, fill = TRUE)

# save predictions
time_ = strftime(Sys.time(), format = "%Y%m%d%H%M%S")
file_name = paste0("predictions/predictions-", time_, ".rds")
if (!fs::dir_exists("predictions")) fs::dir_create("predictions")
saveRDS(predictions, file_name)

# import tasks
tasks_files = dir_ls("F:/H4/problems")
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
predictions[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign = as.factor(sign(response))
)]

# remove na value
predictions_dt = na.omit(predictions)

# number of predictions by task and cv
unique(predictions_dt, by = c("task", "learner", "cv", "row_ids"))[, .N, by = c("task")]
unique(predictions_dt, by = c("task", "learner", "cv", "row_ids"))[, .N, by = c("task", "cv")]

# accuracy by ids
measures = function(t, res) {
  list(acc   = mlr3measures::acc(t, res),
       fbeta = mlr3measures::fbeta(t, res, positive = "1"),
       tpr   = mlr3measures::tpr(t, res, positive = "1"),
       tnr   = mlr3measures::tnr(t, res, positive = "1"))
}
predictions_dt[, measures(truth_sign, response_sign), by = c("cv")]
predictions_dt[, measures(truth_sign, response_sign), by = c("task")]
predictions_dt[, measures(truth_sign, response_sign), by = c("learner")]
predictions_dt[, measures(truth_sign, response_sign), by = c("cv", "task")]
predictions_dt[, measures(truth_sign, response_sign), by = c("cv", "learner")]
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
)]
predictions_ensemble = unique(predictions_ensemble, by = c("task", "row_ids"))
sign_response_max = predictions_ensemble[, max(sign_response, na.rm = TRUE)]
sign_response_seq = seq(as.integer(sign_response_max / 2), sign_response_max - 1)
cols_sign_response_pos = paste0("response_sign_sign_pos", sign_response_seq)
predictions_ensemble[, (cols_sign_response_pos) := lapply(sign_response_seq, function(x) sign_response > x)]
cols_sign_response_neg = paste0("response_sign_sign_neg", sign_response_seq)
predictions_ensemble[, (cols_sign_response_neg) := lapply(sign_response_seq, function(x) sign_response < -x)]
# cols_ = colnames(predictions_dt_ensemble)[24:ncol(predictions_dt_ensemble)]
# predictions_dt_ensemble[, lapply(.SD, function(x) sum(x == TRUE)), .SDcols = cols_]

# check only sign ensamble performance
res = lapply(cols_sign_response_pos, function(x) {
  predictions_ensemble[get(x) == TRUE][
    , mlr3measures::acc(truth_sign, factor(as.integer(get(x)), levels = c(-1, 1))), by = c("task")]
})
names(res) = cols_sign_response_pos
res

# check only sign ensamble performance all
res = lapply(cols_sign_response_pos, function(x) {
  predictions_ensemble[get(x) == TRUE][
    , mlr3measures::acc(truth_sign, factor(as.integer(get(x)), levels = c(-1, 1)))]
})
names(res) = cols_sign_response_pos
res

# check only sign ensamble performance
res = lapply(cols_sign_response_neg[1:5], function(x) { # TODO: REMOVE indexing later
  predictions_ensemble[get(x) == TRUE][
    , mlr3measures::acc(truth_sign, factor(as.integer(get(x)), levels = c(-1, 1))), by = c("task")]
})
names(res) = cols_sign_response_neg[1:5]
res

# save to azure for QC backtest
cont = storage_container(BLOBENDPOINT, "qc-backtest")
lapply(unique(predictions_ensemble$task), function(x) {
  # debug
  # x = "taskRetWeek"

  # prepare data
  y = predictions_ensemble[task == x]
  y = na.omit(y)
  cols = colnames(y)[grep("response_sign", colnames(y))]
  cols = c("symbol", "date", "epsDiff", cols)
  y = y[, ..cols]
  y = unique(y)

  # remove where all false
  # y = y[response_sign_sign_pos13 == TRUE]

  # min and max date
  y[, min(date)]
  y[, max(date)]

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


# SYSTEMIC RISK -----------------------------------------------------------
# import SPY data
con <- dbConnect(duckdb::duckdb())
query <- sprintf("
    SELECT *
    FROM 'F:/lean_root/data/all_stocks_daily.csv'
    WHERE Symbol = 'spy'
")
spy <- dbGetQuery(con, query)
dbDisconnect(con)
spy = as.data.table(spy)
spy = spy[, .(date = Date, close = `Adj Close`)]
spy[, returns := close / shift(close) - 1]
spy = na.omit(spy)
plot(spy[, close])

# systemic risk
task_ = "taskRetWeek"
sample_ = predictions_ensemble[task == task_]
sample_ = na.omit(sample_)
sample_ = unique(sample_)
setorder(sample_, date)
pos_cols = colnames(sample_)[grep("pos", colnames(sample_))]
neg_cols = colnames(sample_)[grep("neg", colnames(sample_))]
new_dt = sample_[, ..pos_cols] - sample_[, ..neg_cols]
setnames(new_dt, gsub("pos", "net", pos_cols))
sample_ = cbind(sample_, new_dt)
sample_[, max(date)]
sample_ = sample_[date < sample_[, max(date)]]
sample_ = sample_[date > sample_[, min(date)]]
plot(as.xts.data.table(sample_[, .N, by = date]))

# calculate indicator
indicator = sample_[, .(ind = median(median_response),
                        ind_sd = sd(sd_response)), by = "date"]
indicator = na.omit(indicator)
indicator[, ind_ema := TTR::EMA(ind, 2, na.rm = TRUE)]
indicator[, ind_sd_ema := TTR::EMA(ind_sd, 2, na.rm = TRUE)]
indicator = na.omit(indicator)
plot(as.xts.data.table(indicator)[, 1])
plot(as.xts.data.table(indicator)[, 2])
plot(as.xts.data.table(indicator)[, 3])

# create backtest data
backtest_data =  merge(spy, indicator, by = "date", all.x = TRUE, all.y = FALSE)
backtest_data = backtest_data[date > indicator[, min(date)]]
backtest_data = backtest_data[date < indicator[, max(date)]]
backtest_data[, signal := 1]
backtest_data[shift(ind) < 0, signal := 0]          # 1
# backtest_data[shift(diff(mean_response_agg_ema, 5)) < -0.01, signal := 0] # 2
backtest_data_xts = as.xts.data.table(backtest_data[, .(date, benchmark = returns, strategy = ifelse(signal == 0, 0, returns * signal * 1))])
charts.PerformanceSummary(backtest_data_xts)
# backtest performance
Performance <- function(x) {
  cumRetx = Return.cumulative(x)
  annRetx = Return.annualized(x, scale=252)
  sharpex = SharpeRatio.annualized(x, scale=252)
  winpctx = length(x[x > 0])/length(x[x != 0])
  annSDx = sd.annualized(x, scale=252)

  DDs <- findDrawdowns(x)
  maxDDx = min(DDs$return)
  # maxLx = max(DDs$length)

  Perf = c(cumRetx, annRetx, sharpex, winpctx, annSDx, maxDDx) # , maxLx)
  names(Perf) = c("Cumulative Return", "Annual Return","Annualized Sharpe Ratio",
                  "Win %", "Annualized Volatility", "Maximum Drawdown") # "Max Length Drawdown")
  return(Perf)
}
Performance(backtest_data_xts[, 1])
Performance(backtest_data_xts[, 2])

# analyse indicator
library(forecast)
ndiffs(as.xts.data.table(indicator)[, 1])
plot(diff(as.xts.data.table(indicator)[, 1]))


# IMPORTANT VARIABLES -----------------------------------------------------
# gausscov files
gausscov_files = dir_ls("F:/H4-v9-gausscov/gausscov_f3")

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

