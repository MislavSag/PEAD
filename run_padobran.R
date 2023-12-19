library(data.table)
library(gausscov)
library(paradox)
library(mlr3)
library(mlr3pipelines)
library(mlr3viz)
library(mlr3tuning)
library(mlr3misc)
library(mlr3extralearners)
library(future)
library(future.apply)
library(batchtools)
library(mlr3batchmark)


# SETUP -------------------------------------------------------------------
# utils https://stackoverflow.com/questions/1995933/number-of-months-between-two-dates
monnb <- function(d) {
  lt <- as.POSIXlt(as.Date(d, origin="1900-01-01"))
  lt$year*12 + lt$mon }
mondf <- function(d1, d2) { monnb(d2) - monnb(d1) }
diff_in_weeks = function(d1, d2) difftime(d2, d1, units = "weeks") # weeks

# weeknb <- function(d) {
#   as.numeric(difftime(as.Date(d), as.Date("1900-01-01"), units = "weeks"))
# }
# weekdf <- function(d1, d2) {
#   weeknb(d2) - weeknb(d1)
# }

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
if (interactive()) {
  data_tbl <- fread("D:/features/pead-predictors-20231031.csv")
} else {
  data_tbl <- fread("pead-predictors-20231106.csv")
}

# convert tibble to data.table
DT = as.data.table(data_tbl)

# create group variable
DT[, date_rolling := as.IDate(date_rolling)]
DT[, yearmonthid := round(date_rolling, digits = "month")]
DT[, weekid := round(date_rolling, digits = "week")]
DT[, .(date, date_rolling, yearmonthid, weekid)]
DT[, yearmonthid := as.integer(yearmonthid)]
DT[, weekid := as.integer(weekid)]
DT[, .(date, date_rolling, yearmonthid, weekid)]

# remove industry and sector vars
DT[, `:=`(industry = NULL, sector = NULL)]

# define predictors
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "bmo_return", "amc_return",
                       "open", "high", "low", "close", "volume", "returns",
                       "yearmonthid", "weekid", "date_rolling"
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
DT = DT[order(yearmonthid, weekid)]
DT[, .(symbol, date, weekid, yearmonthid)]
print("This was the problem. Solved.")


# TASKS -------------------------------------------------------------------
print("Tasks")

# id coluns we always keep
id_cols = c("symbol", "date", "yearmonthid", "weekid")

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

# generate cv's
train_sets = c(24, 48)
gap_sets = c(0:3)
# mat = cbind(train = train_sets)
expanded_list  = lapply(gap_sets, function(v) {
  cbind.data.frame(train_sets, gap = v)
})
cv_param_grid = rbindlist(expanded_list)
cv_param_grid[ ,tune := 3]
custom_cvs = list()
for (i in 1:nrow(cv_param_grid)) {
  print(i)
  param_ = cv_param_grid[i]
  if (param_$gap == 0) {
    custom_cvs[[i]] = nested_cv_split(task_ret_week,
                                      param_$train,
                                      param_$tune,
                                      1,
                                      1,
                                      1)

  } else if (param_$gap == 1) {
    custom_cvs[[i]] = nested_cv_split(task_ret_month,
                                      param_$train,
                                      param_$tune,
                                      1,
                                      param_$gap,
                                      param_$gap)

  } else if (param_$gap == 2) {
    custom_cvs[[i]] = nested_cv_split(task_ret_month2,
                                      param_$train,
                                      param_$tune,
                                      1,
                                      param_$gap,
                                      param_$gap)

  } else if (param_$gap == 3) {
    custom_cvs[[i]] = nested_cv_split(task_ret_quarter,
                                      param_$train,
                                      param_$tune,
                                      1,
                                      param_$gap,
                                      param_$gap)

  }
}

# test
length(custom_cvs) == nrow(cv_param_grid)

# # create expanding window function
# nested_cv_split_expanding = function(task,
#                                      train_length_start = 6,
#                                      tune_length = 3,
#                                      test_length = 1,
#                                      gap_tune = 1,
#                                      gap_test = 1,
#                                      id = task$id) {
#
#   # get year month id data
#   # task = task_ret_week$clone()
#   task_ = task$clone()
#   yearmonthid_ = task_$backend$data(cols = c("yearmonthid", "..row_id"),
#                                     rows = 1:task_$nrow)
#   stopifnot(all(task_$row_ids == yearmonthid_$`..row_id`))
#   groups_v = yearmonthid_[, unlist(unique(yearmonthid))]
#
#   # create cusom CV's for inner and outer sampling
#   custom_inner = rsmp("custom", id = task$id)
#   custom_outer = rsmp("custom", id = task$id)
#
#   # util vars
#   get_row_ids = function(mid) unlist(yearmonthid_[yearmonthid %in% mid, 2], use.names = FALSE)
#
#   # create train data
#   train_groups = lapply(train_length_start:length(groups_v), function(i) groups_v[1:i])
#
#   # create tune set
### REMOVE gap_tune AT THE RIGHT ????
#   tune_groups <- lapply((train_length_start+gap_tune+1):length(groups_v), function(i) groups_v[i:(i+tune_length+gap_tune-1)])
#   index_keep = vapply(tune_groups, function(x) !any(is.na(x)), FUN.VALUE = logical(1L))
#   tune_groups = tune_groups[index_keep]
#
#   # equalize train and tune sets
#   train_groups = train_groups[1:length(tune_groups)]
#
#   # create test sets
#   insample_length = vapply(train_groups, function(x) as.integer(length(x) + gap_tune + tune_length + gap_test), FUN.VALUE = integer(1))
#   test_groups <- lapply(insample_length+gap_test+1, function(i) groups_v[i:(i+test_length-1)])
#   index_keep = vapply(test_groups, function(x) !any(is.na(x)), FUN.VALUE = logical(1L))
#   test_groups = test_groups[index_keep]
#
#   # equalize train, tune and test sets
#   train_groups = train_groups[1:length(test_groups)]
#   tune_groups = tune_groups[1:length(test_groups)]
#
#   # make sets
#   train_sets <- lapply(train_groups, get_row_ids)
#   tune_sets <- lapply(tune_groups, get_row_ids)
#   test_sets <- lapply(test_groups, get_row_ids)
#
#   # test tune and test
#   test_1 = vapply(seq_along(train_groups), function(i) {
#     mondf(
#       tail(as.Date(train_groups[[i]], origin = "1970-01-01"), 1),
#       head(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1)
#     )
#   }, FUN.VALUE = numeric(1L))
#   stopifnot(all(test_1 == 1 + gap_tune))
#   # test_2 = vapply(seq_along(train_groups), function(i) {
#   #   unlist(head(tune_sets[[i]], 1) - tail(train_sets[[i]], 1))
#   # }, FUN.VALUE = numeric(1L))
#   # stopifnot(all(test_2 == 1))
#   test_3 = vapply(seq_along(train_groups), function(i) {
#     mondf(
#       tail(as.Date(tune_groups[[i]], origin = "1970-01-01"), 1),
#       head(as.Date(test_groups[[i]], origin = "1970-01-01"), 1)
#     )
#   }, FUN.VALUE = numeric(1L))
#   stopifnot(all(test_3 == 1 + gap_test))
#   # test_4 = vapply(seq_along(train_groups), function(i) {
#   #   unlist(head(test_sets[[i]], 1) - tail(tune_sets[[i]], 1))
#   # }, FUN.VALUE = numeric(1L))
#   # stopifnot(all(test_2 == 1))
#
#   # create inner and outer resamplings
#   custom_inner$instantiate(task, train_sets, tune_sets)
#   inner_sets = lapply(seq_along(train_groups), function(i) {
#     c(train_sets[[i]], tune_sets[[i]])
#   })
#   custom_outer$instantiate(task, inner_sets, test_sets)
#   return(list(custom_inner = custom_inner, custom_outer = custom_outer))
# }

# # visualize test
# library(ggplot2)
# library(patchwork)
# prepare_cv_plot = function(x, set = "train") {
#   x = lapply(x, function(x) data.table(ID = x))
#   x = rbindlist(x, idcol = "fold")
#   x[, fold := as.factor(fold)]
#   x[, set := as.factor(set)]
#   x[, ID := as.numeric(ID)]
# }
# plot_cv = function(cv, n = 5) {
#   print(cv)
#   cv_test_inner = cv$custom_inner
#   cv_test_outer = cv$custom_outer
#
#   # define task
#   if (cv_test_inner$id == "taskRetQuarter") {
#     task_ = task_ret_quarter$clone()
#   } else if (cv_test_inner$id == "taskRetMonth2") {
#     task_ = task_ret_month2$clone()
#   } else if (cv_test_inner$id == "taskRetMonth") {
#     task_ = task_ret_month$clone()
#   } else if (cv_test_inner$id == "taskRetWeek") {
#     task_ = task_ret_week$clone()
#   }
#
#   # prepare train, tune and test folds
#   train_sets = cv_test_inner$instance$train[1:n]
#   train_sets = prepare_cv_plot(train_sets)
#   tune_sets = cv_test_inner$instance$test[1:n]
#   tune_sets = prepare_cv_plot(tune_sets, set = "tune")
#   test_sets = cv_test_outer$instance$test[1:n]
#   test_sets = prepare_cv_plot(test_sets, set = "test")
#   dt_vis = rbind(train_sets, tune_sets, test_sets)
#   ggplot(dt_vis, aes(x = fold, y = ID, color = set)) +
#     geom_point() +
#     theme_minimal() +
#     coord_flip() +
#     labs(x = "", y = '', title = cv_test_inner$id)
# }
# plots = lapply(custom_cvs[c(1, 4, 7)], plot_cv, n = 12)
# wp = wrap_plots(plots)
# ggsave("plot_cv.png", plot = wp, width = 10, height = 8, dpi = 300)


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
source("PortfolioRet.R")

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
mlr_measures$add("portfolio_ret", PortfolioRet)


# LEARNERS ----------------------------------------------------------------
# graph template
gr = gunion(list(
  po("nop", id = "nop_union_pca"),
  po("pca", center = FALSE, rank. = 50),
  po("ica", n.comp = 10)
)) %>>% po("featureunion")
graph_template =
  po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  # po("winsorizesimplegroup", group_var = "weekid", id = "winsorizesimplegroup", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  # po("uniformization") %>>%
  # scale branch
  po("branch", options = c("uniformization", "scale"), id = "scale_branch") %>>%
  gunion(list(po("uniformization"),
              po("scale")
  )) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  po("dropna", id = "dropna_v2") %>>%
  # add pca columns
  gr %>>%
  # filters
  po("branch", options = c("jmi", "relief", "gausscovf3", "gausscovf1"), id = "filter_branch") %>>%
  gunion(list(po("filter", filter = flt("jmi"), filter.nfeat = 25),
              po("filter", filter = flt("relief"), filter.nfeat = 25),
              po("filter", filter = flt("gausscov_f3st"), m = 1, filter.cutoff = 0),
              po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0)
  )) %>>%
  po("unbranch", id = "filter_unbranch") %>>%
  # modelmatrix
  po("branch", options = c("nop_interaction", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(
    po("nop", id = "nop_interaction"),
    po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0)

# hyperparameters template
graph_template$param_set
search_space_template = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  # preprocessing
  # dropnacol.affect_columns = p_fct(
  #   levels = c("0.01", "0.05", "0.10"),
  #   trafo = function(x, param_set) {
  #     switch(x,
  #            "0.01" = 0.01,
  #            "0.05" = 0.05,
  #            "0.10" = 0.1)
  #   }
  # ),
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
  # winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  # winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  winsorizesimple.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimple.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscovf3", "gausscovf1")),
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
  ps(regr.ranger.max.depth  = p_int(1, 15),
     regr.ranger.replace    = p_lgl(),
     regr.ranger.mtry.ratio = p_dbl(0.1, 1),
     regr.ranger.num.trees  = p_int(10, 2000),
     regr.ranger.splitrule  = p_fct(levels = c("variance", "extratrees")))
)
# regr.ranger.min.node.size   = p_int(1, 20), # Adjust the range as needed
# regr.ranger.sample.fraction = p_dbl(0.1, 1),

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
  # dropnacol.affect_columns = p_fct(
  #   levels = c("0.01", "0.05", "0.10"),
  #   trafo = function(x, param_set) {
  #     switch(x,
  #            "0.01" = 0.01,
  #            "0.05" = 0.05,
  #            "0.10" = 0.1)
  #   }
  # ),
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
  winsorizesimple.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimple.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscovf3", "gausscovf1")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  # learner
  regr.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  regr.xgboost.max_depth = p_int(1, 20),
  regr.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  regr.xgboost.nrounds   = p_int(1, 5000),
  regr.xgboost.subsample = p_dbl(0.1, 1)
)

# gbm graph
graph_gbm = graph_template %>>%
  po("learner", learner = lrn("regr.gbm"))
plot(graph_gbm)
graph_gbm = as_learner(graph_gbm)
as.data.table(graph_gbm$param_set)[, .(id, class, lower, upper, levels)]
search_space_gbm = search_space_template$clone()
search_space_gbm$add(
  ps(regr.gbm.distribution      = p_fct(levels = c("gaussian", "tdist")),
     regr.gbm.shrinkage         = p_dbl(lower = 0.001, upper = 0.1),
     regr.gbm.n.trees           = p_int(lower = 50, upper = 150),
     regr.gbm.interaction.depth = p_int(lower = 1, upper = 3))
  # ....
)

# catboost graph
graph_catboost = graph_template %>>%
  po("learner", learner = lrn("regr.catboost"))
graph_catboost = as_learner(graph_catboost)
as.data.table(graph_catboost$param_set)[, .(id, class, lower, upper, levels)]
search_space_catboost = search_space_template$clone()
# https://catboost.ai/en/docs/concepts/parameter-tuning#description10
search_space_catboost$add(
  ps(regr.catboost.learning_rate   = p_dbl(lower = 0.01, upper = 0.3),
     regr.catboost.depth           = p_int(lower = 4, upper = 10),
     regr.catboost.l2_leaf_reg     = p_int(lower = 1, upper = 5),
     regr.catboost.random_strength = p_int(lower = 0, upper = 3))
)

# cforest graph
graph_cforest = graph_template %>>%
  po("learner", learner = lrn("regr.cforest"))
graph_cforest = as_learner(graph_cforest)
as.data.table(graph_cforest$param_set)[, .(id, class, lower, upper, levels)]
search_space_cforest = search_space_template$clone()
# https://cran.r-project.org/web/packages/partykit/partykit.pdf
search_space_cforest$add(
  ps(regr.cforest.mtryratio    = p_dbl(0.05, 1),
     regr.cforest.ntree        = p_int(lower = 10, upper = 2000),
     regr.cforest.mincriterion = p_dbl(0.1, 1),
     regr.cforest.replace      = p_lgl())
)

# # gamboost graph
# # Error in eval(predvars, data, env) : object 'adxDx14' not found
# # This happened PipeOp regr.gamboost's $train()
# # In addition: There were 50 or more warnings (use warnings() to see the first 50)
# graph_gamboost = graph_template %>>%
#   po("learner", learner = lrn("regr.gamboost"))
# graph_gamboost = as_learner(graph_gamboost)
# as.data.table(graph_gamboost$param_set)[, .(id, class, lower, upper, levels)]
# search_space_gamboost = search_space_template$clone()
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
  dropcorr.cutoff = p_fct(
    levels = c("0.80", "0.90", "0.95", "0.99"),
    trafo = function(x, param_set) {
      switch(
        x,
        "0.80" = 0.80,
        "0.90" = 0.90,
        "0.95" = 0.95,
        "0.99" = 0.99
      )
    }
  ),
  # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
  winsorizesimple.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimple.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscovf3", "gausscovf1")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  # learner
  regr.kknn.k        = p_int(
    lower = 1,
    upper = 50,
    logscale = TRUE
  ),
  regr.kknn.distance = p_dbl(lower = 1, upper = 5),
  regr.kknn.kernel   = p_fct(
    levels = c(
      "rectangular",
      "optimal",
      "epanechnikov",
      "biweight",
      "triweight",
      "cos",
      "inv",
      "gaussian",
      "rank"
    )
  )
)

# nnet graph
graph_nnet = graph_template %>>%
  po("learner", learner = lrn("regr.nnet", MaxNWts = 50000))
graph_nnet = as_learner(graph_nnet)
as.data.table(graph_nnet$param_set)[, .(id, class, lower, upper, levels)]
search_space_nnet = search_space_template$clone()
search_space_nnet$add(
  ps(regr.nnet.size  = p_int(lower = 2, upper = 15),
     regr.nnet.decay = p_dbl(lower = 0.0001, upper = 0.1),
     regr.nnet.maxit = p_int(lower = 50, upper = 500))
)

# glmnet graph
graph_glmnet = graph_template %>>%
  po("learner", learner = lrn("regr.glmnet"))
graph_glmnet = as_learner(graph_glmnet)
as.data.table(graph_glmnet$param_set)[, .(id, class, lower, upper, levels)]
search_space_glmnet = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization

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
  winsorizesimple.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimple.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscovf3", "gausscovf1")),
  # interaction
  interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  # learner
  regr.glmnet.s     = p_int(lower = 5, upper = 30),
  regr.glmnet.alpha = p_dbl(lower = 1e-4, upper = 1, logscale = TRUE)
)


### THIS LEARNER UNSTABLE ####
# ksvm graph
# graph_ksvm = graph_template %>>%
#   po("learner", learner = lrn("regr.ksvm"), scaled = FALSE)
# graph_ksvm = as_learner(graph_ksvm)
# as.data.table(graph_ksvm$param_set)[, .(id, class, lower, upper, levels)]
# search_space_ksvm = ps(
#   # subsample for hyperband
#   subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
#   # preprocessing
#   dropcorr.cutoff = p_fct(
#     levels = c("0.80", "0.90", "0.95", "0.99"),
#     trafo = function(x, param_set) {
#       switch(x,
#              "0.80" = 0.80,
#              "0.90" = 0.90,
#              "0.95" = 0.95,
#              "0.99" = 0.99)
#     }
#   ),
#   # dropcorr.cutoff = p_fct(levels = c(0.8, 0.9, 0.95, 0.99)),
#   winsorizesimplegroup.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
#   winsorizesimplegroup.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
#   # filters
#   filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscov")),
#   # interaction
#   interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
#   # learner
#   regr.ksvm.kernel  = p_fct(levels = c("rbfdot", "polydot", "vanilladot",
#                                        "laplacedot", "besseldot", "anovadot")),
#   regr.ksvm.C       = p_dbl(lower = 0.0001, upper = 1000, logscale = TRUE),
#   regr.ksvm.degree  = p_int(lower = 1, upper = 5,
#                             depends = regr.ksvm.kernel %in% c("polydot", "besseldot", "anovadot")),
#   regr.ksvm.epsilon = p_dbl(lower = 0.01, upper = 1)
# )
### THIS LEARNER UNSTABLE ####

# LAST
# lightgbm graph
# [LightGBM] [Fatal] Do not support special JSON characters in feature name.
graph_template =
  po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  po("winsorizesimple", id = "winsorizesimple", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  # po("winsorizesimplegroup", group_var = "weekid", id = "winsorizesimplegroup", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  # scale branch
  po("branch", options = c("uniformization", "scale"), id = "scale_branch") %>>%
  gunion(list(po("uniformization"),
              po("scale")
  )) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  po("dropna", id = "dropna_v2") %>>%
  # add pca columns
  gr %>>%
  # filters
  po("branch", options = c("jmi", "relief", "gausscovf3", "gausscovf1"), id = "filter_branch") %>>%
  gunion(list(po("filter", filter = flt("jmi"), filter.frac = 0.02),
              po("filter", filter = flt("relief"), filter.frac = 0.02),
              po("filter", filter = flt("gausscov_f3st"), m = 1, filter.cutoff = 0),
              po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0)
  )) %>>%
  po("unbranch", id = "filter_unbranch")
search_space_template = ps(
  # subsample for hyperband
  subsample.frac = p_dbl(0.3, 1, tags = "budget"), # unccoment this if we want to use hyperband optimization
  # preprocessing
  # dropnacol.affect_columns = p_fct(
  #   levels = c("0.01", "0.05", "0.10"),
  #   trafo = function(x, param_set) {
  #     switch(x,
  #            "0.01" = 0.01,
  #            "0.05" = 0.05,
  #            "0.10" = 0.1)
  #   }
  # ),
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
  winsorizesimple.probs_high = p_fct(levels = c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)),
  winsorizesimple.probs_low = p_fct(levels = c(0.001, 0.01, 0.02, 0.03, 0.1, 0.2)),
  # scaling
  scale_branch.selection = p_fct(levels = c("uniformization", "scale")),
  # filters
  filter_branch.selection = p_fct(levels = c("jmi", "relief", "gausscovf3", "gausscovf1"))
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

# rsm graph
graph_rsm = graph_template %>>%
  po("learner", learner = lrn("regr.rsm"))
plot(graph_rsm)
graph_rsm = as_learner(graph_rsm)
as.data.table(graph_rsm$param_set)[, .(id, class, lower, upper, levels)]
search_space_rsm = search_space_template$clone()
search_space_rsm$add(
  ps(regr.rsm.modelfun = p_fct(levels = c("FO", "TWI", "SO")))
)
# Error in fo[, i] * fo[, j] : non-numeric argument to binary operator
# This happened PipeOp regr.rsm's $train()
# Calls: lapply ... resolve.list -> signalConditionsASAP -> signalConditions
# In addition: Warning message:
# In bbandsDn5:volumeDownUpRatio :
#   numerical expression has 4076 elements: only the first used
# This happened PipeOp regr.rsm's $train()

# BART graph
graph_bart = graph_template %>>%
  po("learner", learner = lrn("regr.bart"))
graph_bart = as_learner(graph_bart)
as.data.table(graph_bart$param_set)[, .(id, class, lower, upper, levels)]
search_space_bart = search_space_template$clone()
search_space_bart$add(
  ps(regr.bart.k      = p_int(lower = 1, upper = 10),
     regr.bart.numcut = p_int(lower = 10, upper = 200),
     regr.bart.ntree  = p_int(lower = 50, upper = 500))
)
# chatgpt returns this
# n_chains = p_int(lower = 1, upper = 5),
# m_try = p_int(lower = 1, upper = 13),
# nu = p_dbl(lower = 0.1, upper = 10),
# alpha = p_dbl(lower = 0.01, upper = 1),
# beta = p_dbl(lower = 0.01, upper = 1),
# burn = p_int(lower = 10, upper = 100),
# iter = p_int(lower = 100, upper = 1000)

# threads
threads = 4
set_threads(graph_rf, n = threads)
set_threads(graph_xgboost, n = threads)
# set_threads(graph_bart, n = threads)
# set_threads(graph_ksvm, n = threads) # unstable
set_threads(graph_nnet, n = threads)
set_threads(graph_kknn, n = threads)
set_threads(graph_lightgbm, n = threads)
set_threads(graph_earth, n = threads)
set_threads(graph_gbm, n = threads)
set_threads(graph_rsm, n = threads)
set_threads(graph_catboost, n = threads)
set_threads(graph_glmnet, n = threads)
set_threads(graph_cforest, n = threads)


# DESIGNS -----------------------------------------------------------------
designs_l = lapply(custom_cvs, function(cv_) {
  # debug
  # cv_ = custom_cvs[[1]]

  # get cv inner object
  cv_inner = cv_$custom_inner
  cv_outer = cv_$custom_outer
  cat("Number of iterations fo cv inner is ", cv_inner$iters, "\n")

  # debug
  if (interactive()) {
    to_ = 2
  } else {
    to_ = cv_inner$iters
  }

  designs_cv_l = lapply(1:to_, function(i) { # 1:cv_inner$iters
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

    # with new mlr3 version I have to clone
    task_inner = task_$clone()
    task_inner$filter(c(cv_inner$train_set(i), cv_inner$test_set(i)))

    # inner resampling
    custom_ = rsmp("custom")
    custom_$id = paste0("custom_", cv_inner$iters, "_", i)
    custom_$instantiate(task_inner,
                        list(cv_inner$train_set(i)),
                        list(cv_inner$test_set(i)))

    # objects for all autotuners
    measure_ = msr("portfolio_ret")
    tuner_   = tnr("hyperband", eta = 5)
    # tuner_   = tnr("mbo")
    # term_evals = 20

    # auto tuner rf
    at_rf = auto_tuner(
      tuner = tuner_,
      learner = graph_rf,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_rf,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner xgboost
    at_xgboost = auto_tuner(
      tuner = tuner_,
      learner = graph_xgboost,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_xgboost,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner BART
    at_bart = auto_tuner(
      tuner = tuner_,
      learner = graph_bart,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_bart,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner nnet
    at_nnet = auto_tuner(
      tuner = tuner_,
      learner = graph_nnet,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_nnet,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner lightgbm
    at_lightgbm = auto_tuner(
      tuner = tuner_,
      learner = graph_lightgbm,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_lightgbm,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner earth
    at_earth = auto_tuner(
      tuner = tuner_,
      learner = graph_earth,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_earth,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner kknn
    at_kknn = auto_tuner(
      tuner = tuner_,
      learner = graph_kknn,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_kknn,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner gbm
    at_gbm = auto_tuner(
      tuner = tuner_,
      learner = graph_gbm,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_gbm,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner rsm
    at_rsm = auto_tuner(
      tuner = tuner_,
      learner = graph_rsm,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_rsm,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner rsm
    at_bart = auto_tuner(
      tuner = tuner_,
      learner = graph_bart,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_bart,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner catboost
    at_catboost = auto_tuner(
      tuner = tuner_,
      learner = graph_catboost,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_catboost,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner glmnet
    at_glmnet = auto_tuner(
      tuner = tuner_,
      learner = graph_glmnet,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_glmnet,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # auto tuner glmnet
    at_cforest = auto_tuner(
      tuner = tuner_,
      learner = graph_cforest,
      resampling = custom_,
      measure = measure_,
      search_space = search_space_cforest,
      terminator = trm("none")
      # term_evals = term_evals
    )

    # outer resampling
    customo_ = rsmp("custom")
    customo_$id = paste0("custom_", cv_inner$iters, "_", i)
    customo_$instantiate(task_, list(cv_outer$train_set(i)), list(cv_outer$test_set(i)))

    # nested CV for one round
    design = benchmark_grid(
      tasks = task_,
      learners = list(at_rf, at_xgboost, at_lightgbm, at_nnet, at_earth,
                      at_kknn, at_gbm, at_rsm, at_bart, at_catboost,
                      at_glmnet, at_cforest),
      resamplings = customo_
    )
  })
  designs_cv = do.call(rbind, designs_cv_l)
})
designs = do.call(rbind, designs_l)

# exp dir
if (interactive()) {
  dirname_ = "experiments_test"
  if (dir.exists(dirname_)) system(paste0("rm -r ", dirname_))
} else {
  dirname_ = "experimentsmonth"
}

# create registry
print("Create registry")
packages = c("data.table", "gausscov", "paradox", "mlr3", "mlr3pipelines",
             "mlr3tuning", "mlr3misc", "future", "future.apply",
             "mlr3extralearners", "stats")
reg = makeExperimentRegistry(file.dir = dirname_, seed = 1, packages = packages)

# populate registry with problems and algorithms to form the jobs
print("Batchmark")
batchmark(designs, reg = reg)

# save registry
print("Save registry")
saveRegistry(reg = reg)

# create sh file
sh_file = sprintf("
#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=10GB
#PBS -J 1-%d
#PBS -o experimentsmonth/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0
", nrow(designs))
sh_file_name = "run_month.sh"
file.create(sh_file_name)
writeLines(sh_file, sh_file_name)
