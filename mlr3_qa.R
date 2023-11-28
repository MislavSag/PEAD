library(data.table)
library(mlr3)
library(mlr3pipelines)
library(mlr3tuning)
library(mlr3learners)
library(mlr3hyperband)



# IMPORT DATA -------------------------------------------------------------
# import sample data
DT = fread("https://snpmarketdata.blob.core.windows.net/test/pead-sample.csv")

# define predictors
cols_features = colnames(DT)[13:(length(colnames(DT))-1)]


# TASKS -------------------------------------------------------------------
# id coluns we always keep
id_cols = c("symbol", "date", "yearmonthid")

# convert date to PosixCt because it is requireed by mlr3
DT[, date := as.POSIXct(date, tz = "UTC")]

# task with future week returns as target
target_ = colnames(DT)[grep("^ret.*xcess.*tand.*5", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_week <- as_task_regr(DT[, ..cols_],
                              id = "taskRetWeek",
                              target = target_)

# set roles for symbol, date and yearmonth_id
task_ret_week$col_roles$feature = setdiff(task_ret_week$col_roles$feature,
                                          id_cols)



# CROSS VALIDATIONS -------------------------------------------------------
# utils
monnb <- function(d) {
  lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
  lt$year*12 + lt$mon }
mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }

# create train, tune and test set
nested_cv_split = function(task,
                           train_length = 12,
                           tune_length = 1,
                           test_length = 1,
                           gap_tune = 3,
                           gap_test = 3,
                           id = task$id) {

  # get year month id data
  # task = task_ret_week$clone()
  task_ = task$clone()
  yearmonthid_ = task_$backend$data(cols = c("yearmonthid", "..row_id"),
                                    rows = 1:task_$nrow)
  stopifnot(all(task_$row_ids == yearmonthid_$`..row_id`))
  groups_v = yearmonthid_[, unlist(unique(yearmonthid))]

  # create cusom CV's for inner and outer sampling
  custom_inner = rsmp("custom", id = task$id)
  custom_outer = rsmp("custom", id = task$id)

  # util vars
  start_folds = 1:(length(groups_v)-train_length-tune_length-test_length-gap_test-gap_tune)
  get_row_ids = function(mid) unlist(yearmonthid_[yearmonthid %in% mid, 2], use.names = FALSE)

  # create train data
  train_groups <- lapply(start_folds,
                         function(x) groups_v[x:(x+train_length-1)])
  train_sets <- lapply(train_groups, get_row_ids)

  # create tune set
  tune_groups <- lapply(start_folds,
                        function(x) groups_v[(x+train_length+gap_tune):(x+train_length+gap_tune+tune_length-1)])
  tune_sets <- lapply(tune_groups, get_row_ids)

  # test train and tune
  test_1 = vapply(seq_along(train_groups), function(i) {
    mondf(
      tail(as.Date(train_groups[[i]], origin = "1970-01-01"), 1),
      head(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1)
    )
  }, FUN.VALUE = numeric(1L))
  stopifnot(all(test_1 == (1+gap_tune)))
  # test_2 = vapply(seq_along(train_groups), function(i) {
  #   unlist(head(tune_sets[[i]], 1) - tail(train_sets[[i]], 1))
  # }, FUN.VALUE = numeric(1L))
  # stopifnot(all(test_2 > ))

  # create test sets
  insample_length = train_length + gap_tune + tune_length + gap_test
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
  stopifnot(all(test_1 == 1 + gap_test))
  # test_4 = vapply(seq_along(train_groups), function(i) {
  #   unlist(head(test_sets[[i]], 1) - tail(tune_sets[[i]], 1))
  # }, FUN.VALUE = numeric(1L))
  # stopifnot(all(test_2 == 1))

  # test
  # as.Date(train_groups[[2]])
  # as.Date(tune_groups[[2]])
  # as.Date(test_groups[[2]])

  # create inner and outer resamplings
  custom_inner$instantiate(task, train_sets, tune_sets)
  inner_sets = lapply(seq_along(train_groups), function(i) {
    c(train_sets[[i]], tune_sets[[i]])
  })
  custom_outer$instantiate(task, inner_sets, test_sets)
  return(list(custom_inner = custom_inner, custom_outer = custom_outer))
}
custom_cvs = list()
custom_cvs[[1]] = nested_cv_split(task_ret_week, 24, 3, 1, 1, 1)


# LEARNER ----------------------------------------------------------------
# ranger
graph = po("subsample") %>>%
  po("pca", rank. = 5) %>>%
  po("learner", learner = lrn("regr.ranger"))
graph_rf = as_learner(graph)
graph_rf$param_set
search_space_rf = ps(
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  regr.ranger.max.depth = p_int(2, 20),
  regr.ranger.mtry.ratio = p_dbl(0.1, 1)
)

# xgboost
graph = po("subsample") %>>%
  po("pca", rank. = 5) %>>%
  po("learner", learner = lrn("regr.xgboost"))
graph_xgb = as_learner(graph)
graph_xgb$param_set
search_space_xgb = ps(
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  regr.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  regr.xgboost.max_depth = p_int(1, 20)
)


# DESIGNS -----------------------------------------------------------------
designs_l = lapply(custom_cvs, function(cv_) {
  # debug
  # cv_ = custom_cvs[[1]]

  # get cv inner object
  cv_inner = cv_$custom_inner
  cv_outer = cv_$custom_outer
  cat("Number of iterations fo cv inner is ", cv_inner$iters, "\n")

  designs_cv_l = lapply(1:cv_inner$iters, function(i) { # 1:cv_inner$iters
    # debug
    # i = 1

    # choose task_
    print(cv_inner$id)
    if (cv_inner$id == "taskRetWeek") {
      task_ = task_ret_week$clone()
    } else if (cv_inner$id == "taskRetMonth") {
      task_ = task_ret_month$clone()
    } else if (cv_inner$id == "taskRetMonth2") {
      task_ = task_ret_month2$clone()
    } else if (cv_inner$id == "taskRetQuarter") {
      task_ = task_ret_quarter$clone()
    }

    task_inner = task_ret_week$clone()
    task_inner$filter(c(cv_inner$train_set(i), cv_inner$test_set(i)))

    # inner resampling
    custom_ = rsmp("custom")
    custom_$id = paste0("custom_", cv_inner$iters, "_", i)
    custom_$instantiate(task_inner,
                        list(cv_inner$train_set(i)),
                        list(cv_inner$test_set(i)))

    # objects for all autotuners
    measure_ = msr("regr.mse")
    tuner_   = tnr("hyperband", eta = 4)

    # auto tuner rf
    at_rf = auto_tuner(
      tuner = tuner_,
      learner = graph_rf,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_rf,
      terminator = trm("none")
    )

    # auto tuner xgboost
    at_xgb = auto_tuner(
      tuner = tuner_,
      learner = graph_xgb,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_xgb,
      terminator = trm("none")
    )

    # outer resampling
    customo_ = rsmp("custom")
    customo_$id = paste0("custom_", cv_inner$iters, "_", i)
    customo_$instantiate(task_, list(cv_outer$train_set(i)), list(cv_outer$test_set(i)))

    # nested CV for one round
    design = benchmark_grid(
      tasks = task_,
      learners = list(at_rf, at_xgb),
      resamplings = customo_
    )
  })
  designs_cv = do.call(rbind, designs_cv_l)
})
designs = do.call(rbind, designs_l)

# benchmark
bmr = benchmark(designs[1:2], store_models = FALSE, store_backends = FALSE)


# HELP --------------------------------------------------------------------
# DT
# cols_ = c(colnames(DT)[1:31], "yearmonthid")
# DT[, ..cols_]
#
# DT_sample = DT[, ..cols_]
# library(AzureStor)
# blob_key = readLines('./blob_key.txt')
# endpoint = "https://snpmarketdata.blob.core.windows.net/"
# BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
# cont = storage_container(BLOBENDPOINT, "test")
# storage_write_csv(DT_sample, cont, "pead-sample.csv")
