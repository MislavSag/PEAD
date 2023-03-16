library(data.table)
library(gausscov)
library(paradox)
library(mlr3)
library(mlr3pipelines)
library(mlr3viz)
library(mlr3tuning)
library(mlr3mbo)
library(mlr3misc)
library(AzureStor)




# SETUP -------------------------------------------------------------------
# azure creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
mlr3_save_path = "D:/mlfin/cvresults-pead"



# PREPARE DATA ------------------------------------------------------------
# read predictors
DT <- fread("D:/features/pead-predictors.csv")

# save to azure
cont <- storage_container(BLOBENDPOINT, "test")
cols_ = colnames(DT)[1:50]
sample_ <- DT[, ..cols_]
storage_write_csv(as.data.frame(sample_), cont, file = "mlr3test.csv", col_names = TRUE)

# create group variable
DT[, monthid := paste0(data.table::year(as.Date(date, origin = "1970-01-01")),
                       data.table::month(as.Date(date, origin = "1970-01-01")))]
DT[, monthid := as.integer(monthid)]
setorder(DT, monthid)

# define predictors
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "bmo_return", "amc_return",
                       "open", "high", "low", "close", "volume", "returns",
                       "monthid"
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
int_numbers = DT[, ..cols_features][, lapply(.SD, function(x) all(as.integer(x)==x) & x > 0.99)]
int_cols = na.omit(colnames(DT[, ..cols_features])[as.matrix(int_numbers)[1,]])
factor_cols = DT[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
factor_cols = as.matrix(factor_cols)[1, ]
factor_cols = factor_cols[factor_cols <= 100]
DT = DT[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]

# remove observations with missing target
# if we want to keep as much data as possible an use only one predicitn horizont
# we can skeep this step
DT = na.omit(DT, cols = setdiff(targets, colnames(DT)[grep("extreme", colnames(DT))]))

# sort
setorder(DT, date)



# TASKS -------------------------------------------------------------------
# task with aroundzero bins and weekly target
target_ = colnames(DT)[grep("around.*5", colnames(DT))]
cols_ = c(target_, "monthid", cols_features)
task_aroundzero_week <- as_task_classif(DT[, ..cols_],
                                        id = "aroundzero_week",
                                        target = target_)

# task with aroundzero binsa and month target
target_ = targets[grep("around.*22", targets)]
cols_ = c(target_, "monthid", cols_features)
task_aroundzero_month <- as_task_classif(DT[, ..cols_],
                                         id = "aroundzero_month",
                                         target = target_)

# task with aroundzero binsa and quarter target
target_ = targets[grep("around.*66", targets)]
cols_ = c(target_, "monthid", cols_features)
task_aroundzero_quarter <- as_task_classif(DT[, ..cols_],
                                           id = "aroundzero_quarter",
                                           target = target_)

### REGRESSION
# task with future week returns as target
target_ = colnames(DT)[grep("^ret_excess_stand_5", colnames(DT))]
cols_ = c(target_, "monthid", cols_features)
task_ret_week <- as_task_regr(DT[, ..cols_],
                              id = "task_ret_week",
                              target = target_)

# task with future month returns as target
target_ = colnames(DT)[grep("^ret_excess_stand_22", colnames(DT))]
cols_ = c(target_, "monthid", cols_features)
task_ret_month <- as_task_regr(DT[, ..cols_],
                               id = "task_ret_month",
                               target = target_)

# task with future 2 months returns as target
target_ = colnames(DT)[grep("^ret_excess_stand_44", colnames(DT))]
cols_ = c(target_, "monthid", cols_features)
task_ret_month2 <- as_task_regr(DT[, ..cols_],
                                id = "task_ret_month2",
                                target = target_)

# task with future 2 months returns as target
target_ = colnames(DT)[grep("^ret_excess_stand_66", colnames(DT))]
cols_ = c(target_, "monthid", cols_features)
task_ret_quarter <- as_task_regr(DT[, ..cols_],
                                 id = "task_ret_quarter",
                                 target = target_)


# create group and holdout set
# create_validation_set <- function(task, validation_month_start = 20226) {
#   # add group role
#   task$set_col_roles("monthid", "group")
#   groups = task$groups
#
#   # add validation set
#   val_ind <- min(which(groups$group == validation_month_start)):nrow(groups)
#   task$set_row_roles(rows = val_ind, role = "holdout")
#   task$set_col_roles("monthid", "feature")
# }
# create_validation_set(task_aroundzero_month)

# inner custom rolling window resampling
inner_split  <- function(task, train_length = 36, test_length = 2) {
  custom = rsmp("custom")
  task_ <- task$clone()
  groups = cbind(id = 1:task_$nrow, task_$data(cols = "monthid"))
  groups_v = groups[, unique(monthid)]
  rm(task_)
  train_groups <- lapply(1:(length(groups_v)-train_length-test_length), function(x) groups_v[x:(x+train_length)])
  test_groups <- lapply(1:(length(groups_v)-train_length-test_length), function(x) groups_v[(x+train_length+1):(x+train_length+test_length)])
  train_sets <- lapply(train_groups, function(mid) groups[monthid %in% mid, id])
  test_sets <- lapply(test_groups, function(mid) groups[monthid %in% mid, id])
  custom$instantiate(task, train_sets, test_sets)
  return(custom)
}
customi = inner_split(task_aroundzero_month)

# outer custom rolling window resampling
outer_split <- function(task, train_length = 36, test_length = 2, test_length_out = 1) {
  customo = rsmp("custom")
  task_ <- task$clone()
  groups = cbind(id = 1:task_$nrow, task_$data(cols = "monthid"))
  groups_v = groups[, unique(monthid)]
  rm(task_)
  insample_length = train_length + test_length
  train_groups_out <- lapply(1:(length(groups_v)-train_length-test_length), function(x) groups_v[x:(x+insample_length)])
  test_groups_out <- lapply(1:(length(groups_v)-train_length-test_length),
                             function(x) groups_v[(x+insample_length):(x+insample_length+test_length_out-1)])
  train_sets_out <- lapply(train_groups_out, function(mid) groups[monthid %in% mid, id])
  test_sets_out <- lapply(test_groups_out, function(mid) groups[monthid %in% mid, id])
  customo$instantiate(task, train_sets_out, test_sets_out)
}
customo = outer_split(task_aroundzero_month)

# custom checks
(tail(customi$train_set(1), 1) + 1) == customi$test_set(1)[1] # test set start after train set 1
(tail(customi$train_set(2), 1) + 1) == customi$test_set(2)[1] # test set start after train set 2
all(c(customi$train_set(1), customi$test_set(1)) == customo$train_set(1)) # train set in outersample contains ids in innersample 1
all(c(customi$train_set(2), customi$test_set(2)) == customo$train_set(2)) # train set in outersample contains ids in innersample 1



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
# learners
# learners_l = list(
#   ranger = lrn("classif.ranger", predict_type = "prob", id = "ranger"),
#   log_reg = lrn("classif.log_reg", predict_type = "prob", id = "log_reg"),
#   kknn = lrn("classif.kknn", predict_type = "prob", id = "kknn"),
#   # cv_glmnet = lrn("classif.cv_glmnet", predict_type = "prob", id = "cv_glmnet"),
#   xgboost = lrn("classif.xgboost", predict_type = "prob", id = "xgboost")
# )
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
  dropcorr.cutoff = p_fct(levels = c("0.90", "0.99"), trafo = function(x, param_set) {
    switch(x,
           "0.90" = 0.90,
           "0.99" = 0.99
    )
  }),
  interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix")),
  # winsorizesimple.probs_high = p_fct(levels = c("0.99", "0.98", "0.97", "0.90")),
  winsorizesimple.probs_high = p_fct(levels = c(0.99, 0.98, 0.97, 0.90)),
  # winsorizesimple.probs_low = p_dbl(lower = 0, upper = 1, ),
  # winsorizesimple.probs_low = p_fct(levels = c("0.01", "0.02", "0.03", "0.1")),
  winsorizesimple.probs_low = p_fct(levels = c(0.01, 0.02, 0.03, 0.1)),
  # ranger
  ranger.ranger.max.depth = p_fct(levels = c(2L, 10L)),
  ranger.ranger.splitrule = p_fct(levels = c("variance", "extratrees")),
  ranger.ranger.mtry.ratio = p_dbl(0.5, 1),
  # kknn
  kknn.kknn.k = p_int(1, 10)
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
for (i in 17:tail(customi$iters, 1)) { # seq_len(customi$iters)

  # debug
  # i = 1
  print(i)

  # inner resampling
  custom_ = rsmp("custom")
  custom_$instantiate(task_aroundzero_month, list(customi$train_set(i)), list(customi$test_set(i)))

  # auto tuner
  at = auto_tuner(
    method = "mbo", # tnr("random_search", batch_size = 3),
    learner = graph_learner,
    resampling = custom_,
    measure = msr("linex"),
    search_space = search_space,
    term_evals = 5
  )

  # outer resampling
  customo_ = rsmp("custom")
  customo_$instantiate(task_aroundzero_month, list(customo$train_set(i)), list(customo$test_set(i)))

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

# inspect single benchmark
bmr_ = readRDS("D:/mlfin/cvresults-pead/1-20230306111336.rds")
bmr_$aggregate(msrs(c("regr.mse", "linex", "regr.mae")))
predicitons = as.data.table(as.data.table(bmr_)[, "prediction"][1][[1]][[1]])
predicitons[, `:=`(
  truth_sign = as.factor(sign(truth)),
  response_sign = as.factor(sign(response))
)]
mlr3measures::acc(predicitons$truth_sign, predicitons$response_sign)
predicitons_sample = predicitons[response > 0.5]
mlr3measures::acc(predicitons_sample$truth_sign, predicitons_sample$response_sign)
predicitons_sample[, .(
  benchmark = mean(predicitons$truth),
  strategy  = mean(truth)
)]

# important predictors
resample_res = as.data.table(bmr_$resample_result(1))
resample_res$learner[[1]]$state$model$learner$state$model$gausscov_f1st


bmr$learners$learner[[1]]
bmr$learners$learner[[1]]$archive
test = bmr$resample_result(1)
test = as.data.table(test)
test$learner[[1]]$archive
test$learner[[1]]$tuning_instance$result


# # V1 -----------------------------------------------------------------------
# # tuning instance
# instance = ti(
#   task = task_aroundzero_month,
#   learner = graph_learner,
#   resampling = custom,
#   measures = msr("classif.acc"),
#   terminator = trm("none"),
#   search_space = search_space
# )
#
# # tuner
# tuner = tnr("grid_search", batch_size = 4)
# tuner$optimize(instance)
#
# # check results
# instance$result
# instance$archive
# as.data.table(instance$archive)[, resample_result]
# # first parameter
# x = as.data.table(instance$archive)[, resample_result][[1]]
# as.data.table(instance$archive)[, resample_result][[1]]$score()
# x$learner$param_set$values
# task_ = task_aroundzero_month$clone()
# x$learner$predict(task_$filter(rows = customo$test_set(1)))
# # second parameter
# x = as.data.table(instance$archive)[, resample_result][[2]]
# as.data.table(instance$archive)[, resample_result][[2]]$score()
# x$learner$param_set$values
# x$learner$predict()
#
# # evaluate models on test set
# instance$archive
# as.data.table(instance$archive)[, resample_result][[1]]$score()
# graph_learner_clone = graph_learner$clone()
# graph_learner_clone$param_set$values = instance$result_learner_param_vals
# task_train = task_aroundzero_month$clone()
# task_train$filter(rows = c(custom$train_set(1), custom$test_set(1)))
# graph_learner_clone$train(task_train)
# task_test = task_aroundzero_month$clone()
# task_test$filter(rows = customo$test_set(1))
# predictions = graph_learner_clone$predict(task_test)
# predictions$confusion
# predictions$score(msr("classif.acc"))
#
#
