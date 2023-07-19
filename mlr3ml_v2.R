# because of Error: protect(): protection stack overflow.
# This happened PipeOp lm.lm's $train().
# solution: https://stackoverflow.com/questions/32826906/how-to-solve-protection-stack-overflow-issue-in-r-studio
options(expressions = 500000)

library(data.table)
library(gausscov)
library(paradox)
library(mlr3)
library(mlr3pipelines)
library(mlr3viz)
library(mlr3tuning)
library(mlr3mbo)
library(mlr3misc)
library(mlr3hyperband)
library(mlr3extralearners)
library(AzureStor)




# SETUP -------------------------------------------------------------------
# azure creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
mlr3_save_path = "D:/mlfin/cvresults-pead-v4"

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
DT = na.omit(DT, cols = setdiff(targets, colnames(DT)[grep("extreme", colnames(DT))]))

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

# # task with aroundzero bins and weekly target
# target_ = colnames(DT)[grep("around.*5", colnames(DT))]
# cols_ = c(target_, "monthid", cols_features)
# task_aroundzero_week <- as_task_classif(DT[, ..cols_],
#                                         id = "aroundzero_week",
#                                         target = target_)
#
# # task with aroundzero binsa and month target
# target_ = targets[grep("around.*22", targets)]
# cols_ = c(target_, "monthid", cols_features)
# task_aroundzero_month <- as_task_classif(DT[, ..cols_],
#                                          id = "aroundzero_month",
#                                          target = target_)
#
# # task with aroundzero binsa and quarter target
# target_ = targets[grep("around.*66", targets)]
# cols_ = c(target_, "monthid", cols_features)
# task_aroundzero_quarter <- as_task_classif(DT[, ..cols_],
#                                            id = "aroundzero_quarter",
#                                            target = target_)

### REGRESSION
# task with future week returns as target
target_ = colnames(DT)[grep("^ret_excess_stand_5", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_week <- as_task_regr(DT[, ..cols_],
                              id = "task_ret_week",
                              target = target_)

# task with future month returns as target
target_ = colnames(DT)[grep("^ret_excess_stand_22", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_month <- as_task_regr(DT[, ..cols_],
                               id = "task_ret_month",
                               target = target_)

# # task with future 2 months returns as target
# target_ = colnames(DT)[grep("^ret_excess_stand_44", colnames(DT))]
# cols_ = c(target_, "monthid", cols_features)
# task_ret_month2 <- as_task_regr(DT[, ..cols_],
#                                 id = "task_ret_month2",
#                                 target = target_)
#
# # task with future 2 months returns as target
# target_ = colnames(DT)[grep("^ret_excess_stand_66", colnames(DT))]
# cols_ = c(target_, "monthid", cols_features)
# task_ret_quarter <- as_task_regr(DT[, ..cols_],
#                                  id = "task_ret_quarter",
#                                  target = target_)

# set roles for symbol, date and yearmonth_id
task_ret_week$col_roles$feature = setdiff(task_ret_week$col_roles$feature,
                                          id_cols)
task_ret_month$col_roles$feature = setdiff(task_ret_month$col_roles$feature,
                                           id_cols)

# create train, tune and test set
nested_cv_split = function(task,
                           train_length = 36,
                           tune_length = 2,
                           test_length = 1) {

  # create cusom CV's for inner and outer sampling
  custom_inner = rsmp("custom")
  custom_outer = rsmp("custom")

  # get year month id data
  # task = task_ret_week$clone()
  task_ = task$clone()
  yearmonthid_ = task_$backend$data(cols = c("yearmonthid", "..row_id"),
                                    rows = 1:task_$nrow)
  stopifnot(all(task_$row_ids == yearmonthid_$`..row_id`))
  groups_v = yearmonthid_[, unlist(unique(yearmonthid))]

  # util vars
  start_folds = 1:(length(groups_v)-train_length-tune_length-test_length)
  get_row_ids = function(mid) unlist(yearmonthid_[yearmonthid %in% mid, 2], use.names = FALSE)

  # create train data
  train_groups <- lapply(start_folds,
                         function(x) groups_v[x:(x+train_length-1)])
  train_sets <- lapply(train_groups, get_row_ids)

  # create tune set
  tune_groups <- lapply(start_folds,
                        function(x) groups_v[(x+train_length):(x+train_length+tune_length-1)])
  tune_sets <- lapply(tune_groups, get_row_ids)

  # test train and tune
  test_1 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(train_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == 1))
  test_2 = vapply(seq_along(train_groups), function(i) {
    unlist(head(tune_sets[[i]], 1) - tail(train_sets[[i]], 1))
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_2 == 1))

  # create test sets
  insample_length = train_length + tune_length
  test_groups <- lapply(start_folds,
                        function(x) groups_v[(x+insample_length):(x+insample_length+test_length-1)])
  test_sets <- lapply(test_groups, get_row_ids)

  # test tune and test
  test_3 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(test_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == 1))
  test_4 = vapply(seq_along(train_groups), function(i) {
    unlist(head(test_sets[[i]], 1) - tail(tune_sets[[i]], 1))
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_2 == 1))

  # create inner and outer resamplings
  custom_inner$instantiate(task, train_sets, tune_sets)
  inner_sets = lapply(seq_along(train_groups), function(i) {
    c(train_sets[[i]], tune_sets[[i]])
  })
  custom_outer$instantiate(task, inner_sets, test_sets)
  return(list(custom_inner = custom_inner, custom_outer = custom_outer))
}
custom_cvs = nested_cv_split(task_ret_week)
custom_inner = custom_cvs$custom_inner
custom_outer = custom_cvs$custom_outer

# test set start after train set
all(vapply(1:custom_inner$iters, function(i) {
  (tail(custom_inner$train_set(i), 1) + 1) == custom_inner$test_set(i)[1]
}, FUN.VALUE = logical(1L)))

# train set in outersample contains ids in innersample 1
all(vapply(1:custom_inner$iters, function(i) {
  all(c(custom_inner$train_set(i),
        custom_inner$test_set(i)) == custom_outer$train_set(i))
}, FUN.VALUE = logical(1L)))



# ADD PIPELINES -----------------------------------------------------------
# source pipes, filters and other
source("mlr3_winsorization.R")
source("mlr3_uniformization.R")
source("mlr3_gausscov_f1st.R")
source("mlr3_dropna.R")
source("mlr3_dropnacol.R")
source("mlr3_filter_drop_corr.R")
source("mlr3_winsorizationsimple.R")
source("PipeOpPCAExplained.R")
source("Linex.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", PipeOpUniform)
mlr_pipeops$add("winsorize", PipeOpWinsorize)
mlr_pipeops$add("winsorizesimple", PipeOpWinsorizeSimple)
mlr_pipeops$add("dropna", PipeOpDropNA)
mlr_pipeops$add("dropnacol", PipeOpDropNACol)
mlr_pipeops$add("dropcorr", PipeOpDropCorr)
mlr_pipeops$add("pca_explained", PipeOpPCAExplained)
mlr_filters$add("gausscov_f1st", FilterGausscovF1st)
mlr_measures$add("linex", Linex)



# GRAPH -------------------------------------------------------------------
# xgboost hyperband
learners_l = list(
  ranger = lrn("regr.ranger", id = "ranger"),
  # lm = lrn("regr.lm", id = "lm"), # removed because of error: Error: protect(): protection stack overflow This happened PipeOp lm.lm's $train()
  kknn = lrn("regr.kknn", id = "kknn"),
  xgboost = lrn("regr.xgboost", id = "xgboost")
)

# create graph from list of learners
choices = c("ranger", "kknn", "xgboost")
learners = gunion(learners_l) %>>%
  po("regravg", innum = length(learners_l))

# create graph
graph =
  po("subsample") %>>%
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  po("uniformization") %>>%
  po("dropna", id = "dropna_v2") %>>%
  # dim reduction
  po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0) %>>%
  # po("branch", options = c("pca", "gausscov"), id = "prep_branch") %>>%
  # gunion(list(po("pca"), po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0))) %>>%
  # po("unbranch", id = "prep_unbranch") %>>%
  # modelmatrix
  po("branch", options = c("nop_filter", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(po("nop", id = "nop_filter"), po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0) %>>%
  # learners
  learners
plot(graph)
graph_learner = as_learner(graph)

# define search space
as.data.table(graph_learner$param_set)[1:100, .(id, class, lower, upper)]
as.data.table(graph_learner$param_set)[grep("drop", id), .(id, class, lower, upper)]
search_space = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.2, 1, tags = "budget"),
  # preprocessing
  dropcorr.cutoff = p_fct(
    levels = c("0.80", "0.90", "0.95", "0.99"),
    trafo = function(x, param_set) {
      switch(x,
             "0.80" = 0.80,
             "0.90" = 0.90,
             "0.95" = 0.95,
             "0.99" = 0.99)
    }
  ),
  winsorizesimple.probs_high = p_fct(levels = c(0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimple.probs_low = p_fct(levels = c(0.01, 0.02, 0.03, 0.1, 0.2)),
  # dim reduction
  # prep_branch.selection = p_fct(levels = c("pca", "gausscov")),
  # pca_explained.var. = p_fct(
  #   levels = c("0.99", "0.95", "0.90"),
  #   trafo = function(x, param_set) {
  #     switch(x,
  #            "0.90" = 0.90,
  #            "0.95" = 0.95,
  #            "0.99" = 0.99)
  #   },
  #   depends = prep_branch.selection == "pca"
  # ),
  # pca.rank. = p_fct(
  #   levels = c("4", "16", "32", "64", "128", "256", "512"),
  #   trafo = function(x, param_set) {
  #     switch(x,
  #            "4" = 4,
  #            "16" = 16,
  #            "32" = 32,
  #            "64" = 64,
  #            "128" = 128,
  #            "256" = 256)
  #   },
    # depends = prep_branch.selection == "pca"
  # ),
  # interaction terms
  interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix"))
)

# inspect search space
design = rbindlist(generate_design_grid(search_space, 3)$transpose(), fill = TRUE)
design



# BAYES OPT ---------------------------------------------------------------
# Gaussian Process, EI, FocusSearch
# surrogate = srlrn(lrn("regr.km",
#                       covtype = "matern3_2",
#                       optim.method = "gen",
#                       nugget.estim = TRUE,
#                       jitter = 1e-12,
#                       control = list(trace = FALSE)))
# acq_function = acqf("ei")
# acq_optimizer = acqo(opt("focus_search", n_points = 100L, maxit = 9),
#                      terminator = trm("evals", n_evals = 2000))
# tuner = tnr("mbo",
#             loop_function = bayesopt_ego,
#             surrogate = surrogate,
#             acq_function = acq_function,
#             acq_optimizer = acq_optimizer)



# NESTED CV BENCHMARK -----------------------------------------------------
# nested for loop
# future::plan("multisession", workers = 4L)
for (i in 16:custom_inner$iters) {

  # debug
  # i = 1
  print(i)

  # inner resampling
  custom_ = rsmp("custom")
  custom_$instantiate(task_ret_week,
                      list(custom_inner$train_set(i)),
                      list(custom_inner$test_set(i)))

  # auto tuner
  # at = auto_tuner(
  #   tuner = tnr("mbo"), # tnr("random_search", batch_size = 3),
  #   learner = graph_learner,
  #   resampling = custom_,
  #   measure = msr("linex"),
  #   search_space = search_space,
  #   term_evals = 5
  # )
  at = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_learner,
    resampling = custom_,
    measure = msr("linex"),
    search_space = search_space,
    terminator = trm("none")
  )

  # outer resampling
  customo_ = rsmp("custom")
  customo_$instantiate(task_ret_week, list(custom_outer$train_set(i)), list(custom_outer$test_set(i)))

  # nested CV for one round
  design = benchmark_grid(
    tasks = list(task_ret_week, task_ret_month), # task_ret_month, task_ret_month2
    learners = at,
    resamplings = customo_
  )
  bmr = benchmark(design, store_models = TRUE)

  # save locally and to list
  time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  saveRDS(bmr, file.path(mlr3_save_path, paste0(i, "-", time_, ".rds")))
}

