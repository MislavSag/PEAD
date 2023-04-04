library(data.table)
library(gausscov)
library(paradox)
library(mlr3)
library(mlr3pipelines)
library(mlr3tuning)
library(mlr3mbo)
library(mlr3misc)
library(mlr3filters)
library(mlr3learners)
library(mlr3viz)
library(AzureStor)
library(qlcal)
library(lubridate)
library(ggplot2)



# SETUP -------------------------------------------------------------------
# azure creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
qlcal::setCalendar("UnitedStates/NYSE")
mlr3_path = "D:/mlfin/pead_live"

# utils https://stackoverflow.com/questions/1995933/number-of-months-between-two-dates
monnb <- function(d) {
  lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
  lt$year*12 + lt$mon }
mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }



# PREPARE DATA ------------------------------------------------------------
# read predictors
files_ <- file.info(list.files("D:/features", full.names = TRUE, pattern = "pead|PEAD|Pead"))
files_[order(files_$ctime), ]
DT <- fread("D:/features/pead-predictors-20230402172611.csv")
setorder(DT, date)
DT[, .(date, date_rolling)]

# create group variable
DT[, yearmonthid := round(date_rolling, digits = "month")]
DT[, .(date, date_rolling, yearmonthid)]
DT[, yearmonthid := as.integer(yearmonthid)]
DT[, .(date, date_rolling, yearmonthid)]

# define predictors
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "bmo_return", "amc_return",
                       "open", "high", "low", "close", "volume", "returns",
                       "yearmonthid", "date_rolling"
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
int_numbers = na.omit(DT[, ..cols_features])[, lapply(.SD, function(x) all(floor(x) == x))]
int_cols = colnames(DT[, ..cols_features])[as.matrix(int_numbers)[1,]]
factor_cols = DT[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
factor_cols = as.matrix(factor_cols)[1, ]
factor_cols = factor_cols[factor_cols <= 100]
DT = DT[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]

# remove observations with missing target
# if we want to keep as much data as possible an use only one predicitn horizont
# we can skeep this step
DT = na.omit(DT, cols = "ret_excess_stand_5")

# change IDate to date, because of error
# Assertion on 'feature types' failed: Must be a subset of
# {'logical','integer','numeric','character','factor','ordered','POSIXct'},
# but has additional elements {'IDate'}.
DT[, date := as.POSIXct(date, tz = "UTC")]
DT[, .(symbol,date, date_rolling, yearmonthid)]

# sort
setorder(DT, date)



# TASKS -------------------------------------------------------------------
# id coluns we always keep
id_cols = c("symbol", "date", "yearmonthid")

# task with future week returns as target
target_ = colnames(DT)[grep("^ret_excess_stand_5", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_week <- as_task_regr(DT[, ..cols_],
                              id = "task_ret_week",
                              target = target_)


# define tuning set
tune_end = floor_date(Sys.Date() - 7, unit = "month") - 1
tune_begin = floor_date(tune_end %m-% months(1), unit = "month")
tune_trading_days = getBusinessDays(tune_begin, tune_end)

# define train set
train_end = floor_date(tune_begin, unit = "month") - 1
train_begin = floor_date(train_end %m-% months(36), unit = "month")
train_trading_days = getBusinessDays(train_begin, train_end)

# define test set
dates_ = task_ret_week$backend$data(cols = "date", rows = 1:task_ret_week$nrow)
dates_ = as.Date(dates_[[1]])
test_end = max(unlist(dates_))
test_begin = floor_date(test_end, unit = "month")
test_trading_days = getBusinessDays(test_begin, test_end)

# define train and tune ids
ids = task_ret_week$backend$data(cols = c("symbol", "date", "..row_id"),
                                 rows = 1:task_ret_week$nrow)
ids[, date := as.Date(date)]
setnames(ids, "..row_id", "row_ids")
train_ids = ids[date %in% train_trading_days, row_ids]
tune_ids = ids[date %in% tune_trading_days, row_ids]
test_ids = ids[date %in% test_trading_days, row_ids]

# test
(tail(tune_ids, 1) + 1) == head(test_ids, 1)

# define test set data tabe for new data
# cols_ = c("symbol", "date", "monthid", cols_features)
# test_set = DT[date %between% c(test_begin, Sys.Date()), ..cols_]
# library(naniar)
# vis_miss(test_set[, 300:400])
# c(21:24, 26:34, 65, 100:191, 193:195, 196:197, 201, 203:205)



# ADD PIPELINES -----------------------------------------------------------
# source pipes, filters and other
source("mlr3_winsorization.R")
source("mlr3_uniformization.R")
source("mlr3_gausscov_f1st.R")
source("mlr3_dropna.R")
source("mlr3_dropnacol.R")
source("mlr3_filter_drop_corr.R")
source("mlr3_winsorizationsimple.R")
source("Linex.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", PipeOpUniform)
mlr_pipeops$add("winsorize", PipeOpWinsorize)
mlr_pipeops$add("winsorizesimple", PipeOpWinsorizeSimple)
mlr_pipeops$add("dropna", PipeOpDropNA)
mlr_pipeops$add("dropnacol", PipeOpDropNACol)
mlr_pipeops$add("dropcorr", PipeOpDropCorr)
mlr_filters$add("gausscov_f1st", FilterGausscovF1st)
mlr_measures$add("linex", Linex)



# GRAPH -------------------------------------------------------------------
print("Graph")
# learners
learners_l = list(
  ranger = lrn("regr.ranger", id = "ranger"),
  lm = lrn("regr.lm", id = "lm"),
  kknn = lrn("regr.kknn", id = "kknn"),
  # cv_glmnet = lrn("classif.cv_glmnet", predict_type = "prob", id = "cv_glmnet"),
  xgboost = lrn("regr.xgboost", id = "xgboost")
)

# create graph from list of learners
choices = c("ranger", "lm", "kknn", "xgboost")
learners = po("branch", choices, id = "branch_learners") %>>%
  gunion(learners_l) %>>%
  po("unbranch", choices, id = "unbranch_learners")

# create graph
graph = po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  po("uniformization") %>>%
  po("dropna", id = "dropna_v2") %>>%
  po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0) %>>%  # filter important predictors
  # modelmatrix
  po("branch", options = c("nop_filter", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(po("nop", id = "nop_filter"), po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0) %>>%
  # learners
  learners %>>%
  po("regravg", innum = length(learners_l))
plot(graph)
graph_learner = as_learner(graph)

# define search space
as.data.table(graph_learner$param_set)[1:100, .(id, class, lower, upper)]
as.data.table(graph_learner$param_set)[grep("drop", id), .(id, class, lower, upper)]
search_space = ps(
  # preprocessing
  dropcorr.cutoff = p_fct(
    levels = c("0.90", "0.99"),
    trafo = function(x, param_set) {
      switch(x,
             "0.90" = 0.90,
             "0.99" = 0.99)
    }
  ),
  interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix")),
  # winsorizesimple.probs_high = p_fct(levels = c("0.99", "0.98", "0.97", "0.90")),
  winsorizesimple.probs_high = p_fct(levels = c(0.99, 0.98, 0.97, 0.90)),
  # winsorizesimple.probs_low = p_dbl(lower = 0, upper = 1, ),
  # winsorizesimple.probs_low = p_fct(levels = c("0.01", "0.02", "0.03", "0.1")),
  winsorizesimple.probs_low = p_fct(levels = c(0.01, 0.02, 0.03, 0.1)),
  # ranger
  ranger.ranger.max.depth = p_fct(levels = c(2L, 10L)), # HERE p_int, not fct !!!!
  ranger.ranger.splitrule = p_fct(levels = c("variance", "extratrees")),
  ranger.ranger.mtry.ratio = p_dbl(0.5, 1),
  # kknn
  kknn.kknn.k = p_int(1, 15)
  # extra transformations
  # .extra_trafo = function(x, param_set) {
  #   x$winsorizesimple.probs_high = switch(
  #     x$winsorizesimple.probs_high,
  #     "0.99" = 0.99,
  #     "0.98" = 0.98,
  #     "0.97" = 0.97
  #   )
  #   x$winsorizesimple.probs_low = 1 - as.numeric(x$winsorizesimple.probs_high)
  #   x
  # }
)

# inspect search space
design = rbindlist(generate_design_grid(search_space, 3)$transpose(), fill = TRUE)
design



# NESTED CV BENCHMARK -----------------------------------------------------
print("Nested banchmark")
# nested for loop
# future::plan("multisession", workers = 4L)

# inner resampling
print("Inner resampling")
custom_ = rsmp("custom")
custom_$instantiate(task_ret_week, list(train_ids), list(tune_ids))

# tuning instance
instance = tune(
  tuner = tnr("mbo"),
  task = task_ret_week,
  learner = graph_learner,
  resampling = custom_,
  measures = msr("linex"),
  search_space = search_space,
  term_evals = 5,
  store_models = TRUE # keep this to true so we can inspect important variables
)

# save to azure
# time_ <- strftime(Sys.time(), format = "%Y%m%d%H%M%S")
# file_name = paste0("pead-", tune_end, "-", time_, ".rds")
# saveRDS(instance, file = file.path(mlr3_path, file_name))
# cont = storage_container(BLOBENDPOINT, "pead-live")
# storage_save_rds(instance, cont, file_name)

# import data
file.info(list.files(mlr3_path, full.names = TRUE))
# instance = readRDS(file.path(mlr3_path, "pead-2023-02-28-20230403150123.rds"))



# UP TO DATE PERFORMANCE --------------------------------------------------
# tuning results
instance$result

# check for warnings and errors
as.data.table(instance$archive)[, list(timestamp, runtime_learners, errors, warnings)]

# check different measures
m = c("linex", "regr.mse", "regr.mae")
ms = msrs(m[-1])
as.data.table(instance$archive, measures = ms)[, ..m]

# analysing results
names(as.data.table(instance$archive))
plot_param_sensitivity = function(param) {
  df = as.data.table(instance$archive)[, list(get(param), linex)]
  ggplot(df, aes(V1, linex)) +
    geom_bar(stat = "identity")
}
plot_param_sensitivity("interaction_branch.selection")
plot_param_sensitivity("ranger.ranger.splitrule")
plot_param_sensitivity("dropcorr.cutoff")
plot_param_sensitivity("winsorizesimple.probs_high")
plot_param_sensitivity("winsorizesimple.probs_low")
plot_param_sensitivity("ranger.ranger.max.depth")
plot_param_sensitivity("ranger.ranger.mtry.ratio")
plot_param_sensitivity("kknn.kknn.k")

# plot bivariate dependends of parameters
autoplot(instance,
         type = "surface",
         cols_x = c("x_domain_ranger.ranger.max.depth", "x_domain_winsorizesimple.probs_high"))
autoplot(instance,
         type = "surface",
         cols_x = c("x_domain_winsorizesimple.probs_high", "x_domain_kknn.kknn.k"))

# important variables
### HAVE TO RETRAIN TO ISNPECT IMPORTANT VARIABLES
instance$
resample_res = as.data.table(bmr_$resample_result(1))
resample_res$learner[[1]]$state$model$learner$state$model$gausscov_f1st

# create tuned graph learner that with best parameters
graph_learner_tuned = graph_learner$clone()
graph_learner_tuned$id = "tuned_graph"
graph_learner_tuned$param_set$values = instance$result_learner_param_vals

# retrain tuned model on all data
task_tuned = task_ret_week$filter(rows = c(train_ids, tune_ids))
graph_learner_tuned$train(task_tuned)

# save tuned graph learner
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
file_name = paste0("pead-final-", tune_end, "-", time_, ".rds")
saveRDS(graph_learner_tuned, file = file.path(mlr3_path, file_name))

# read
# list.files(mlr3_path)
# graph_learner_tuned = readRDS(file.path(mlr3_path, "pead-final-2023-02-28-20230401113908.rds"))

# performance on test set, if test set exists
predictions_test = graph_learner_tuned$predict(task_ret_week, row_ids = test_ids)
predictions_test = as.data.table(predictions_test)
predictions_test_long = predictions_test[response > 0.1]
hit_ratio = predictions_test_long[truth > 0 & response > 0, hit := 1]
nrow(hit_ratio[hit == 1]) / nrow(hit_ratio)

# save to azure for QC backtest
backend_ = task_ret_week$backend$data(cols = c(id_cols, "..row_id"),
                                      rows = 1:task_ret_week$nrow)
setnames(backend_, "..row_id", "row_ids")
predictions_qc = backend_[, .(row_ids, symbol, date)][hit_ratio, on = "row_ids"]
predictions_qc[, date := as.Date(date, origin = "1970-01-01")]
predictions_qc <- predictions_qc[, .(symbol, date, response)]
predictions_qc = predictions_qc[, .(symbol = paste0(symbol, collapse = "|"),
                                    response = paste0(response, collapse = "|")), by = date]
predictions_qc[, date := as.character(date)]
cont = storage_container(BLOBENDPOINT, "qc-backtest")
storage_write_csv(predictions_qc, cont, "pead_task_ret_week-test.csv")
universe = predictions_qc[, .(date, symbol)]
storage_write_csv(universe, cont, "pead_task_ret_week_universe-test.csv", col_names = FALSE)



# PREDICTIONS FOR LIVE TRADING --------------------------------------------
# predictions for last week data
predictions_holdout = graph_learner_tuned$predict_newdata(newdata = test_set[, .SD, .SDcols = !c("symbol", "date")])
predictions_holdout_dt = as.data.table(predictions_holdout)

# prepare predictions for trading
test_ids = test_set[predictions_holdout_dt[, row_ids], .(symbol, date)]
predictions = cbind(test_ids, predictions_holdout_dt)
predictions[date > (Sys.Date() - 11)]
