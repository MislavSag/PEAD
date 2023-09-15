library(data.table)
library(gausscov)
library(paradox)
library(mlr3)
library(mlr3pipelines)
library(mlr3viz)
library(mlr3tuning)
library(mlr3misc)
library(future)
library(future.apply)


# SETUP -------------------------------------------------------------------
# create folder in which we will save results
mlr3_save_path = paste0("./H4-jobarray-", Sys.getenv('PBS_ARRAY_ID'))
if (!dir.exists(mlr3_save_path)) {
  dir.create(mlr3_save_path)
}

# utils https://stackoverflow.com/questions/1995933/number-of-months-between-two-dates
monnb <- function(d) {
  lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
  lt$year*12 + lt$mon }
mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }

# snake to camel
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


# PREPARE DATA ------------------------------------------------------------
print("Prepare data")

# read predictors
data_tbl = fread("./pead-predictors-update.csv")
# data_tbl <- fread("D:/features/pead-predictors-20230523202603.csv")

# convert tibble to data.table
DT = as.data.table(data_tbl)

# create group variable
DT[, date_rolling := as.IDate(date_rolling)]
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

# change feature and targets columns names due to lighgbm
cols_features_new = vapply(cols_features, snakeToCamel, FUN.VALUE = character(1L), USE.NAMES = FALSE)
setnames(DT, cols_features, cols_features_new)
cols_features = cols_features_new
targets_new = vapply(targets, snakeToCamel, FUN.VALUE = character(1L), USE.NAMES = FALSE)
setnames(DT, targets, targets_new)
targets = targets_new

# convert columns to numeric. This is important only if we import existing features
chr_to_num_cols <- setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
print(chr_to_num_cols)
DT <- DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]

# remove constant columns in set
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
DT = na.omit(DT, cols = setdiff(targets, colnames(DT)[grep("xtreme", colnames(DT))]))

# change IDate to date, because of error
# Assertion on 'feature types' failed: Must be a subset of
# {'logical','integer','numeric','character','factor','ordered','POSIXct'},
# but has additional elements {'IDate'}.
DT[, date := as.POSIXct(date, tz = "UTC")]
# DT[, .(symbol,date, date_rolling, yearmonthid)]

# sort
# this returns error on HPC. Some problem with memory
# setorder(DT, date)
print("This was the problem")
# DT = DT[order(date)] # DOESNT WORK TOO
DT = DT[order(yearmonthid)]
print("This was the problem. Solved.")



# TASKS -------------------------------------------------------------------
print("Tasks")

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

# task with future month returns as target
target_ = colnames(DT)[grep("^ret.*xcess.*tand.*22", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_month <- as_task_regr(DT[, ..cols_],
                               id = "taskRetMonth",
                               target = target_)

# task with future 2 months returns as target
target_ = colnames(DT)[grep("^ret.*xcess.*tand.*44", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_month2 <- as_task_regr(DT[, ..cols_],
                                id = "taskRetMonth2",
                                target = target_)

# task with future 2 months returns as target
target_ = colnames(DT)[grep("^ret.*xcess.*tand.*66", colnames(DT))]
cols_ = c(id_cols, target_, cols_features)
task_ret_quarter <- as_task_regr(DT[, ..cols_],
                                 id = "taskRetQuarter",
                                 target = target_)

# set roles for symbol, date and yearmonth_id
task_ret_week$col_roles$feature = setdiff(task_ret_week$col_roles$feature,
                                          id_cols)
task_ret_month$col_roles$feature = setdiff(task_ret_month$col_roles$feature,
                                           id_cols)
task_ret_month2$col_roles$feature = setdiff(task_ret_month2$col_roles$feature,
                                            id_cols)
task_ret_quarter$col_roles$feature = setdiff(task_ret_quarter$col_roles$feature,
                                             id_cols)


# CROSS VALIDATIONS -------------------------------------------------------
print("Cross validations")

# create train, tune and test set
nested_cv_split = function(task,
                           train_length = 12,
                           tune_length = 1,
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

# generate cv's
train_sets = seq(12, 12 * 3, 12)
validation_sets = train_sets / 12
custom_cvs = list()
for (i in seq_along(train_sets)) {
  print(i)
  custom_cvs[[i]] = nested_cv_split(task_ret_week,
                                    train_sets[[i]],
                                    validation_sets[[i]],
                                    1)
}

# create expanding window function
nested_cv_split_expanding = function(task,
                                     train_length_start = 6,
                                     tune_length = 1,
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
  get_row_ids = function(mid) unlist(yearmonthid_[yearmonthid %in% mid, 2], use.names = FALSE)

  # create train data
  train_groups = lapply(train_length_start:length(groups_v), function(i) groups_v[1:i])

  # create tune set
  tune_groups <- lapply((train_length_start+1):length(groups_v), function(i) groups_v[i:(i+tune_length-1)])
  index_keep = vapply(tune_groups, function(x) !any(is.na(x)), FUN.VALUE = logical(1L))
  tune_groups = tune_groups[index_keep]

  # equalize train and tune sets
  train_groups = train_groups[1:length(tune_groups)]

  # create test sets
  insample_length = vapply(train_groups, function(x) as.integer(length(x) + tune_length), FUN.VALUE = integer(1))
  test_groups <- lapply((insample_length+1):length(groups_v), function(i) groups_v[i:(i+test_length-1)])
  index_keep = vapply(test_groups, function(x) !any(is.na(x)), FUN.VALUE = logical(1L))
  test_groups = test_groups[index_keep]

  # equalize train, tune and test sets
  train_groups = train_groups[1:length(test_groups)]
  tune_groups = tune_groups[1:length(test_groups)]

  # make sets
  train_sets <- lapply(train_groups, get_row_ids)
  tune_sets <- lapply(tune_groups, get_row_ids)
  test_sets <- lapply(test_groups, get_row_ids)

  # test tune and test
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

# generate cv's for expanding windows
custom_cvs[[length(custom_cvs)+1]] = nested_cv_split_expanding(task_ret_week,
                                                               train_length_start = 6,
                                                               tune_length = 1,
                                                               test_length = 1)

# test if tain , validation and tst set follow logic
lapply(seq_along(custom_cvs), function(i) {
  # extract custyom cv
  custom_cvs_ = custom_cvs[[i]]
  custom_inner = custom_cvs_$custom_inner
  custom_outer = custom_cvs_$custom_outer

  # test set start after train set
  test1 = all(vapply(1:custom_inner$iters, function(i) {
    (tail(custom_inner$train_set(i), 1) + 1) == custom_inner$test_set(i)[1]
  }, FUN.VALUE = logical(1L)))

  # train set in outersample contains ids in innersample 1
  test2  = all(vapply(1:custom_inner$iters, function(i) {
    all(c(custom_inner$train_set(i),
          custom_inner$test_set(i)) == custom_outer$train_set(i))
  }, FUN.VALUE = logical(1L)))
  c(test1, test2)
})


# ADD PIPELINES -----------------------------------------------------------
print("Add pipelines")

# source pipes, filters and other
source("mlr3_winsorization.R")
source("mlr3_uniformization.R")
source("mlr3_gausscov_f1st.R")
source("mlr3_gausscov_f3st.R")
source("mlr3_dropna.R")
source("mlr3_dropnacol.R")
source("mlr3_filter_drop_corr.R")
source("mlr3_winsorizationsimple.R")
source("mlr3_winsorizationsimplegroup.R")
source("PipeOpPCAExplained.R")
# measures
source("Linex.R")
source("AdjLoss2.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", PipeOpUniform)
mlr_pipeops$add("winsorize", PipeOpWinsorize)
mlr_pipeops$add("winsorizesimple", PipeOpWinsorizeSimple)
mlr_pipeops$add("winsorizesimplegroup", PipeOpWinsorizeSimpleGroup)
mlr_pipeops$add("dropna", PipeOpDropNA)
mlr_pipeops$add("dropnacol", PipeOpDropNACol)
mlr_pipeops$add("dropcorr", PipeOpDropCorr)
mlr_pipeops$add("pca_explained", PipeOpPCAExplained)
mlr_filters$add("gausscov_f1st", FilterGausscovF1st)
mlr_filters$add("gausscov_f3st", FilterGausscovF3st)
mlr_measures$add("linex", Linex)
mlr_measures$add("adjloss2", AdjLoss2)


# GRAPH V2 ----------------------------------------------------------------
# TODO: ADD abess
# graph_template_abess =
#   po("subsample") %>>% # uncomment this for hyperparameter tuning
#   po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
#   po("dropna", id = "dropna") %>>%
#   po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
#   po("fixfactors", id = "fixfactors") %>>%
#   # po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
#   po("winsorizesimplegroup", group_var = "yearmonthid", id = "winsorizesimplegroup", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
#   po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
#   po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
#   po("uniformization") %>>%
#   po("dropna", id = "dropna_v2") %>>%
#   po("learner", learner = lrn("regr.abess"))


# graph template
graph_template =
  po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  # po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("winsorizesimplegroup", group_var = "yearmonthid", id = "winsorizesimplegroup", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  po("uniformization") %>>%
  po("dropna", id = "dropna_v2") %>>%
  # filters
  po("branch", options = c("jmi", "relief", "gausscov"), id = "filter_branch") %>>%
  gunion(list(po("filter", filter = flt("jmi"), filter.frac = 0.05),
              po("filter", filter = flt("relief"), filter.frac = 0.05),
              po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0))) %>>%
              # po("nop", id = "nop_filter"))) %>>%
  po("unbranch", id = "filter_unbranch") %>>%
  # modelmatrix
  po("branch", options = c("nop_interaction", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(
    po("nop", id = "nop_interaction"),
    po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0)

# hyperparameters template
search_space_template = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
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
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscov")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix"))
)

# random forest graph
graph_rf = graph_template %>>%
  po("learner", learner = lrn("regr.ranger"))
plot(graph_rf)
graph_rf = as_learner(graph_rf)
as.data.table(graph_rf$param_set)[, .(id, class, lower, upper, levels)]
search_space_rf = search_space_template$clone()
search_space_rf$add(
  ps(regr.ranger.max.depth  = p_int(1, 40),
     regr.ranger.replace    = p_lgl(),
     regr.ranger.mtry.ratio = p_dbl(0.1, 1))
)

# xgboost graph
graph_xgboost = graph_template %>>%
  po("learner", learner = lrn("regr.xgboost"))
plot(graph_xgboost)
graph_xgboost = as_learner(graph_xgboost)
as.data.table(graph_xgboost$param_set)[grep("depth", id), .(id, class, lower, upper, levels)]
search_space_xgboost = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
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
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscov")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  # learner
  regr.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  regr.xgboost.max_depth = p_int(1, 20),
  regr.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  regr.xgboost.nrounds   = p_int(1, 5000)
)

# BART graph
# Error in makeModelMatrixFromDataFrame(x.test, if (!is.null(drop)) drop else TRUE) :
#   when list, drop must have length equal to x
# This happened PipeOp regr.bart's $predict()
graph_bart = graph_template %>>%
  po("learner", learner = lrn("regr.bart"))
graph_bart = as_learner(graph_bart)
as.data.table(graph_bart$param_set)[, .(id, class, lower, upper, levels)]
search_space_bart = search_space_template$clone()
search_space_bart$add(
  ps(regr.bart.k = p_int(lower = 1, upper = 10))
     # regr.bart.nu = p_dbl(lower = 0.1, upper = 10),
     # regr.bart.n_trees = p_int(lower = 10, upper = 100))
)
# chatgpt returns this
# n_trees = p_int(lower = 10, upper = 100),
# n_chains = p_int(lower = 1, upper = 5),
# k = p_int(lower = 1, upper = 10),
# m_try = p_int(lower = 1, upper = 13),
# nu = p_dbl(lower = 0.1, upper = 10),
# alpha = p_dbl(lower = 0.01, upper = 1),
# beta = p_dbl(lower = 0.01, upper = 1),
# burn = p_int(lower = 10, upper = 100),
# iter = p_int(lower = 100, upper = 1000)


# # catboost graph
# ### REMOVED FROM MLR3EXTRALEARNERS FROM VERSION 0.7.0.
# graph_catboost = graph_template %>>%
#   po("learner", learner = lrn("regr.catboost"))
# graph_catboost = as_learner(graph_catboost)
# as.data.table(graph_catboost$param_set)[, .(id, class, lower, upper, levels)]
# search_space_catboost = search_space_template$clone()
# # https://catboost.ai/en/docs/concepts/parameter-tuning#description10
# search_space_catboost$add(
#   ps(regr.catboost.learning_rate   = p_dbl(lower = 0.01, upper = 0.3),
#      regr.catboost.depth           = p_int(lower = 4, upper = 10),
#      regr.catboost.l2_leaf_reg     = p_int(lower = 1, upper = 5),
#      regr.catboost.random_strength = p_int(lower = 0, upper = 3))
# )

# # gamboost graph
# # Error in eval(predvars, data, env) : object 'adxDx14' not found
# # This happened PipeOp regr.gamboost's $train()
# # In addition: There were 50 or more warnings (use warnings() to see the first 50)
# graph_gamboost = graph_template %>>%
#   po("learner", learner = lrn("regr.gamboost"))
# graph_gamboost = as_learner(graph_gamboost)
# as.data.table(graph_gamboost$param_set)[, .(id, class, lower, upper, levels)]
# search_space_gamboost = search_space_template$clone()
# # https://catboost.ai/en/docs/concepts/parameter-tuning#description10
# search_space_gamboost$add(
#   ps(regr.gamboost.mstop       = p_int(lower = 10, upper = 100),
#      regr.gamboost.nu          = p_dbl(lower = 0.01, upper = 0.5),
#      regr.gamboost.baselearner = p_fct(levels = c("bbs", "bols", "btree")))
# )

# # gamboost graph
# # Error in eval(predvars, data, env) : object 'adxDx14' not found
# # This happened PipeOp regr.gamboost's $train()
# # In addition: There were 50 or more warnings (use warnings() to see the first 50)
# graph_gamboost = graph_template %>>%
#   po("learner", learner = lrn("regr.gamboost"))
# graph_gamboost = as_learner(graph_gamboost)
# as.data.table(graph_gamboost$param_set)[, .(id, class, lower, upper, levels)]
# search_space_gamboost = search_space_template$clone()
# # https://catboost.ai/en/docs/concepts/parameter-tuning#description10
# search_space_gamboost$add(
#   ps(regr.gamboost.mstop       = p_int(lower = 10, upper = 100),
#      regr.gamboost.nu          = p_dbl(lower = 0.01, upper = 0.5),
#      regr.gamboost.baselearner = p_fct(levels = c("bbs", "bols", "btree")))
# )

# kknn graph
graph_kknn = graph_template %>>%
  po("learner", learner = lrn("regr.kknn"))
graph_kknn = as_learner(graph_kknn)
as.data.table(graph_kknn$param_set)[, .(id, class, lower, upper, levels)]
search_space_kknn = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
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
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscov")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  # learner
  regr.kknn.k        = p_int(lower = 1, upper = 50, logscale = TRUE),
  regr.kknn.distance = p_dbl(lower = 1, upper = 5),
  regr.kknn.kernel   = p_fct(levels = c("rectangular", "optimal", "epanechnikov", "biweight", "triweight", "cos",  "inv",  "gaussian", "rank"))
)

# nnet graph
graph_nnet = graph_template %>>%
  po("learner", learner = lrn("regr.nnet", MaxNWts = 40000))
graph_nnet = as_learner(graph_nnet)
as.data.table(graph_nnet$param_set)[, .(id, class, lower, upper, levels)]
search_space_nnet = search_space_template$clone()
search_space_nnet$add(
  ps(regr.nnet.size  = p_int(lower = 5, upper = 30),
     regr.nnet.decay = p_dbl(lower = 0.0001, upper = 0.1),
     regr.nnet.maxit = p_int(lower = 50, upper = 500))
)

# ksvm graph
graph_ksvm = graph_template %>>%
  po("learner", learner = lrn("regr.ksvm"), scaled = FALSE)
graph_ksvm = as_learner(graph_ksvm)
as.data.table(graph_ksvm$param_set)[, .(id, class, lower, upper, levels)]
search_space_ksvm = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
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
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscov")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  # learner
  regr.ksvm.kernel  = p_fct(levels = c("rbfdot", "polydot", "vanilladot",
                                       "laplacedot", "besseldot", "anovadot")),
  regr.ksvm.C       = p_dbl(lower = 0.0001, upper = 1000, logscale = TRUE),
  regr.ksvm.degree  = p_int(lower = 1, upper = 5,
                            depends = regr.ksvm.kernel %in% c("polydot", "besseldot", "anovadot")),
  regr.ksvm.epsilon = p_dbl(lower = 0.01, upper = 1)
)

# LAST
# lightgbm graph
# [LightGBM] [Fatal] Do not support special JSON characters in feature name.
graph_template =
  po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  # po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("winsorizesimplegroup", group_var = "yearmonthid", id = "winsorizesimplegroup", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  po("uniformization") %>>%
  po("dropna", id = "dropna_v2") %>>%
  # filters
  po("branch", options = c("jmi", "relief", "gausscov"), id = "filter_branch") %>>%
  gunion(list(po("filter", filter = flt("jmi"), filter.frac = 0.05),
              po("filter", filter = flt("relief"), filter.frac = 0.05),
              po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0))) %>>%
  po("unbranch", id = "filter_unbranch")
search_space_template = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
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
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscov"))
)
graph_lightgbm = graph_template %>>%
  po("learner", learner = lrn("regr.lightgbm"))
graph_lightgbm = as_learner(graph_lightgbm)
as.data.table(graph_lightgbm$param_set)[grep("sample", id), .(id, class, lower, upper, levels)]
search_space_lightgbm = search_space_template$clone()
search_space_lightgbm$add(
  ps(regr.lightgbm.max_depth     = p_int(lower = 2, upper = 10),
     regr.lightgbm.learning_rate = p_dbl(lower = 0.001, upper = 0.3),
     regr.lightgbm.num_leaves    = p_int(lower = 10, upper = 100))
)

# earth graph
graph_earth = graph_template %>>%
  po("learner", learner = lrn("regr.earth"))
graph_earth = as_learner(graph_earth)
as.data.table(graph_earth$param_set)[grep("sample", id), .(id, class, lower, upper, levels)]
search_space_earth = search_space_template$clone()
search_space_earth$add(
  ps(regr.earth.degree  = p_int(lower = 1, upper = 4),
     # regr.earth.penalty = p_int(lower = 1, upper = 5),
     regr.earth.nk      = p_int(lower = 50, upper = 250))
)

# threads
threads = as.integer(Sys.getenv("NCPUS"))
set_threads(graph_rf, n = threads)
set_threads(graph_xgboost, n = threads)
# set_threads(graph_bart, n = threads)
set_threads(graph_ksvm, n = threads)
set_threads(graph_nnet, n = threads)
set_threads(graph_kknn, n = threads)
set_threads(graph_lightgbm, n = threads)
set_threads(graph_earth, n = threads)


# NESTED CV BENCHMARK -----------------------------------------------------
print("Benchmark")

# nested for loop
list.files(mlr3_save_path, full.names = TRUE)
nested_cv_benchmark <- function(i, cv_inner, cv_outer) {

  # debug
  # i = 1
  print(i)

  # inner resampling
  custom_ = rsmp("custom")
  custom_$instantiate(task_ret_week,
                      list(cv_inner$train_set(i)),
                      list(cv_inner$test_set(i)))

  # auto tuner rf
  at_rf = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_rf,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_rf,
    terminator = trm("none")
  )

  # auto tuner xgboost
  at_xgboost = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_xgboost,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_xgboost,
    terminator = trm("none")
  )

  # auto tuner BART
  at_bart = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_bart,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_bart,
    terminator = trm("none")
  )

  # auto tuner ksvm
  at_ksvm = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_ksvm,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_ksvm,
    terminator = trm("none")
  )

  # auto tuner nnet
  at_nnet = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_nnet,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_nnet,
    terminator = trm("none")
  )

  # auto tuner lightgbm
  at_lightgbm = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_lightgbm,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_lightgbm,
    terminator = trm("none")
  )

  # auto tuner earth
  at_earth = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_earth,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_earth,
    terminator = trm("none")
  )

  # auto tuner kknn
  at_kknn = auto_tuner(
    tuner = tnr("hyperband", eta = 5),
    learner = graph_kknn,
    resampling = custom_,
    measure = msr("adjloss2"),
    search_space = search_space_kknn,
    terminator = trm("none")
  )

  # # auto tuner mboost
  # at_gamboost = auto_tuner(
  #   tuner = tnr("hyperband", eta = 5),
  #   learner = graph_gamboost,
  #   resampling = custom_,
  #   measure = msr("adjloss2"),
  #   search_space = search_space_gamboost,
  #   terminator = trm("none")
  # )

  # # auto tuner catboost
  # at_catboost = auto_tuner(
  #   tuner = tnr("hyperband", eta = 5),
  #   learner = graph_catboost,
  #   resampling = custom_,
  #   measure = msr("adjloss2"),
  #   search_space = search_space_catboost,
  #   terminator = trm("none")
  # )

  # outer resampling
  customo_ = rsmp("custom")
  customo_$instantiate(task_ret_week, list(cv_outer$train_set(i)), list(cv_outer$test_set(i)))

  # nested CV for one round
  print("Benchmark!")
  design = benchmark_grid(
    tasks = list(task_ret_week, task_ret_month, task_ret_month2, task_ret_quarter),
    learners = list(at_rf, at_xgboost, at_lightgbm, at_nnet, at_earth, at_kknn), # at_ksvm
    resamplings = customo_
  )
  bmr = benchmark(design, store_models = FALSE, store_backends = FALSE)

  # save locally and to list
  print("Save")
  time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  saveRDS(bmr, file.path(mlr3_save_path, paste0(i, "-", cv_inner$iters, "-", time_, ".rds")))
  return(NULL)
}

i = as.integer(Sys.getenv('PBS_ARRAY_INDEX'))
start_time = Sys.time()
lapply(custom_cvs, function(cv_) {

  # debug
  # i = 1
  # cv_ = custom_cvs[[1]]

  # get cv inner object
  cv_inner = cv_$custom_inner
  cv_outer = cv_$custom_outer
  cat("Number of iterations fo cv inner is ", cv_inner$iters, "\n")

  nested_cv_benchmark(i, cv_inner, cv_outer)
})

end_time = Sys.time()
end_time - start_time
