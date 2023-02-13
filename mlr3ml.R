library(data.table)
library(gausscov)
library(paradox)
library(mlr3)
library(mlr3pipelines)



# PREPARE DATA ------------------------------------------------------------
# read predictors
DT <- fread("D:/features/pead-predictors.csv")

# create group variable
DT[, monthid := paste0(data.table::year(as.Date(date, origin = "1970-01-01")),
                       data.table::month(as.Date(date, origin = "1970-01-01")))]
DT[, monthid := as.integer(monthid)]
setorder(DT, monthid)

# define predictors
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "ret_excess_stand_5", "ret_excess_stand_22", "ret_excess_stand_44", "ret_excess_stand_66",
                       colnames(DT)[grep("aroundzero", colnames(DT))],
                       colnames(DT)[grep("extreme", colnames(DT))],
                       colnames(DT)[grep("bin_simple", colnames(DT))],
                       colnames(DT)[grep("bin_decile", colnames(DT))],
                       "bmo_return", "amc_return",
                       "open", "high", "low", "close", "volume", "returns",
                       "monthid"
                       # 'predictors' we don't need
                       # colnames(DT)[grep("nperiods_\\d+", colnames(DT))],
                       # colnames(DT)[grep("seasonal_period_\\d+", colnames(DT))]
                       )
cols_features <- setdiff(colnames(DT), c(cols_non_features))

# convert columns to numeric. This is important only if we import existing features
chr_to_num_cols <- setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
print(chr_to_num_cols)
DT <- DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
# int_to_num_cols <- colnames(DT[, .SD, .SDcols = is.integer])
# DT <- DT[, (int_to_num_cols) := lapply(.SD, as.numeric), .SDcols = int_to_num_cols]

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
factor_cols = factor_cols[factor_cols <= 20]
DT = DT[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]



# TASKS -------------------------------------------------------------------
# task with aroundzero bins
target_ = colnames(DT)[grep("around.*22", colnames(DT))]
cols_ = c(target_, "monthid", cols_features)
tsk_aroundzero_month <- as_task_classif(DT[, ..cols_],
                                        id = "aroundzero_month",
                                        target = target_)

# remove NA from target variable
any(is.na(tsk_aroundzero_month$data()))
target_na_ids = which(is.na(tsk_aroundzero_month$truth()))
target_na_ids = setdiff(tsk_aroundzero_month$row_ids, target_na_ids)
tsk_aroundzero_month$filter(target_na_ids)
any(is.na(tsk_aroundzero_month$data()))
dim(tsk_aroundzero_month$data())

# create group and holdout set
create_validation_set <- function(task, validation_month_start = 20226) {
  # add group role
  task$set_col_roles("monthid", "group")
  groups = task$groups

  # add validation set
  val_ind <- min(which(groups$group == validation_month_start)):nrow(groups)
  task$set_row_roles(rows = val_ind, role = "holdout")
  task$set_col_roles("monthid", "feature")
}
create_validation_set(tsk_aroundzero_month)

# create custom rolling window cross validation set
custom = rsmp("custom")
task_ <- tsk_aroundzero_month$clone()
task_$set_col_roles("monthid", "group")
groups = task_$groups
rm(task_)
groups_v <- groups[, unique(group)]
train_length <- 24
test_length <- 1
train_groups <- lapply(0:(length(groups_v)-(train_length+1)), function(x) x + (1:train_length))
test_groups <- lapply(train_groups, function(x) tail(x, 1) + test_length)
train_sets <- lapply(train_groups, function(x) groups[group %in% groups_v[x], row_id])
test_sets <- lapply(test_groups, function(x) groups[group %in% groups_v[x], row_id])
custom$instantiate(tsk_aroundzero_month, train_sets[1], test_sets[1])

# inspect task
# tsk_aroundzero_month$backend$primary_key
# tsk_aroundzero_month$col_info
# tsk_aroundzero_month$col_roles
# tsk_aroundzero_month$labels
# tsk_aroundzero_month$label
# tsk_aroundzero_month$row_names
# tsk_aroundzero_month$


# GRAPH -------------------------------------------------------------------
# source pipes, filters and other
source("mlr3_winsorization.R")
source("mlr3_uniformization.R")
source("mlr3_gausscov_f1st.R")
source("mlr3_dropna.R")
source("mlr3_dropnacol.R")
source("mlr3_filter_drop_corr.R")
source("mlr3_gausscov_f1st.R")

# add my pipes to mlr dictionary
mlr_pipeops$add("uniformization", PipeOpUniform)
mlr_pipeops$add("winsorize", PipeOpWinsorize)
mlr_pipeops$add("dropna", PipeOpDropNA)
mlr_pipeops$add("dropnacol", PipeOpDropNACol)
mlr_pipeops$add("dropcorr", PipeOpDropCorr)
mlr_filters$add("gausscov_f1st", FilterGausscovF1st)

# create graph
# graph = po("select", id = "select_corr") %>>%
#   po("winsorize", id = "winsorize", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE)
graph = po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0.01)  %>>%
  po("winsorize", id = "winsorize", probs_low = 0.01, probs_high = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0.01)  %>>%
  po("dropcorr", id = "dropcorr", cutoff = 0.99) %>>%
  po("uniformization") %>>%
  po("pca", id = "pca") %>>%
  po("dropna", id = "dropna_v2") %>>%
  po("filter", filter = flt("gausscov_f1st"), filter.cutoff = 0) %>>%
  po("learner", learner = lrn("classif.ranger"))

100


base:         ret = eps_diff + momentum + bs
alternativni: ret = eps_diff + momentum + bs + sentimenti

5, 5.1 , 5.2
13, 14, 15


search_space = ps(
  # preprocesing
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 6, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 6, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson")
)

# train/test graph rolling CV
graph$train(tsk_aroundzero_month)
graph$predict(tsk_aroundzero_month)

graph_learner = as_learner(graph)
task_ = tsk_aroundzero_month$clone()
task_$select(task_$feature_names[1:900]) # 946
# task_$feature_names[947]
# table(task_$data()[, "feasts_shift_var_index_66"])
# table(task_$data(rows = custom$train_set(1))[, "feasts_shift_var_index_66"])
# table(task_$data(rows = custom$test_set(1))[, "feasts_shift_var_index_66"])
# for test
# task_$filter(custom$test_set(1))
# task_$feature_names[947]
# table(task_$data()[, "feasts_shift_var_index_66"])
rr = resample(task_, graph_learner, custom, store_models = FALSE)
rr$aggregate(msr("classif.acc"))
rr$warnings
rr$resampling
rr$prediction()
rr$resampling

# holdout prediction
# rr_decile = resample(task_decile, graph_learner, custom, store_models = TRUE)









graph = po("removeconstants", ratio = 0.01) %>>%
  # modelmatrix
  po("branch", options = c("nop_filter", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(po("nop", id = "nop_filter"), po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0.01) %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  learners%>>%
  po("classifavg", innum = length(learners_l))
plot(graph)



task = mlr3::tsk("mtcars")
task$data()
filter = flt("find_correlation")
filter$calculate(task)
as.data.table(filter)









# FEATURE SELECTION (TEST) ------------------------------------------------
# select features
test_ <- na.omit(unique(c(predictors_f3st_1)))
# task_extreme$select(test_)
task_aroundzero$select(test_)
task_simple$select(test_)
task_decile$select(test_)
task_reg$select(test_)

# rpart tree classificatoin function
tree_visualization <- function(task_, maxdepth = 4, cp = 0.002) {
  learner = lrn("classif.rpart", maxdepth = maxdepth,
                predict_type = "prob", cp = cp)
  learner$train(task_)
  predictins = learner$predict(task_)
  print(predictins$score(c(msr("classif.acc"), msr("classif.recall"), msr("classif.precision"), msr("classif.fbeta"))))
  print(learner$importance())
  rpart_model <- learner$model
  rpart.plot(rpart_model)
}
tree_visualization(task_simple$clone(), cp = 0.001)
tree_visualization(task_simple$clone(), cp = 0.0001)
tree_visualization(task_simple$clone(), cp = 0.00001)

# rpart tree regression
learner = lrn("regr.rpart", maxdepth = 4, cp = 0.01)
task_ <- task_reg$clone()
learner$train(task_reg)
predictins = learner$predict(task_reg)
predictins$score(msr("regr.mae"))
learner$importance()
rpart_model <- learner$model
rpart.plot(rpart_model)



# CLASSIFICATION AUTOML ---------------------------------------------------
# learners
learners_l = list(
  ranger = lrn("classif.ranger", predict_type = "prob", id = "ranger"),
  # log_reg = lrn("classif.log_reg", predict_type = "prob", id = "log_reg"),
  # kknn = lrn("classif.kknn", predict_type = "prob", id = "kknn"),
  # cv_glmnet = lrn("classif.cv_glmnet", predict_type = "prob", id = "cv_glmnet"),
  xgboost = lrn("classif.xgboost", predict_type = "prob", id = "xgboost")
)
# create graph from list of learners
choices = c("ranger", "xgboost")
learners = po("branch", choices, id = "branch_learners") %>>%
  gunion(learners_l) %>>%
  po("unbranch", choices, id = "unbranch_learners")

# create complete grapg
graph = po("removeconstants", ratio = 0.01) %>>%
  # modelmatrix
  po("branch", options = c("nop_filter", "modelmatrix"), id = "interaction_branch") %>>%
  gunion(list(po("nop", id = "nop_filter"), po("modelmatrix", formula = ~ . ^ 2))) %>>%
  po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0.01) %>>%
  # scaling
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  learners%>>%
  po("classifavg", innum = length(learners_l))
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:70, .(id, class, lower, upper)]
search_space = ps(
  # preprocesing
  interaction_branch.selection = p_fct(levels = c("nop_filter", "modelmatrix")),
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 6, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 6, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson"),
  # models
  ranger.ranger.mtry.ratio = p_dbl(0.2, 1),
  ranger.ranger.max.depth = p_int(2, 4),
  # kknn.kknn.k = p_int(5, 20),
  xgboost.xgboost.nrounds = p_int(100, 5000),
  xgboost.xgboost.eta = p_dbl(1e-4, 1),
  xgboost.xgboost.max_depth = p_int(1, 8),
  xgboost.xgboost.colsample_bytree = p_dbl(0.1, 1),
  xgboost.xgboost.colsample_bylevel = p_dbl(0.1, 1),
  xgboost.xgboost.lambda = p_dbl(0.1, 1),
  xgboost.xgboost.gamma = p_dbl(1e-4, 1000),
  xgboost.xgboost.alpha = p_dbl(1e-4, 1000),
  xgboost.xgboost.subsample = p_dbl(0.1, 1)
)
# plan("multisession", workers = 4L)

rr = resample(task_aroundzero, graph_learner, custom, store_models = TRUE)
rr$aggregate(msr("classif.acc"))
rr$warnings
rr$resampling
rr$prediction()

# holdout prediction
rr$

  rr_decile = resample(task_decile, graph_learner, custom, store_models = TRUE)


at_classif = auto_tuner(
  method = "random_search",
  learner = graph_learner,
  resampling = custom,
  measure = msr("classif.acc"),
  search_space = search_space
  # term_evals = 10
)
at_classif
# at_classif$train(task_aroundzero)

# inspect results
at_classif$tuning_result
at_classif$learner
archive <- as.data.table(at_classif$archive)
length(at_classif$state)
ggplot(archive[, mean(classif.fbeta), by = "ranger.ranger.max.depth"], aes(x = ranger.ranger.max.depth, y = V1)) + geom_line()
ggplot(archive[, mean(classif.fbeta), by = "prep_branch.selection"], aes(x = prep_branch.selection, y = V1)) + geom_bar(stat = "identity")
ggplot(archive[, mean(classif.fbeta), by = "interaction_branch.selection"], aes(x = interaction_branch.selection, y = V1)) + geom_bar(stat = "identity")
preds = at_classif$predict(task_extreme)
preds$confusion
preds$score(list(msr("classif.acc")))
preds$score(list(msr("classif.fbeta"), msr("classif.acc")))

# holdout extreme
preds_holdout <- at_classif$predict(task_extreme_holdout)
preds_holdout$confusion
autoplot(preds_holdout, type = "roc")
preds_holdout$score(msrs(c("classif.acc")))
preds_holdout$score(msrs(c("classif.acc", "classif.recall", "classif.precision", "classif.fbeta")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)
prediciotns_extreme_holdout <- prediciotns_extreme_holdout[`prob.1` > 0.6]
nrow(prediciotns_extreme_holdout)
mlr3measures::acc(prediciotns_extreme_holdout$truth,
                  prediciotns_extreme_holdout$response)
prediciotns_extreme_holdout[, truth := as.factor(ifelse(truth == 0, 1, -1))]
prediciotns_extreme_holdout$truth <- droplevels(prediciotns_extreme_holdout$truth)
prediciotns_extreme_holdout$response <- droplevels(prediciotns_extreme_holdout$response)
# levels(prediciotns_extreme_holdout$response) <- c("-1", "1")
# mlr3measures::acc(prediciotns_extreme_holdout$truth,
#                   prediciotns_extreme_holdout$response)

# try extreme on bin simple
X_model_sim <- copy(X_holdout)
levels(X_model_sim$bin_simple_ret_excess_stand_5) <- c("-1", "1")
X_model_sim <- X_model_sim[, .SD, .SDcols = !c("symbol","date", labels[!grepl("simple", labels)])]
setnames(X_model_sim, "bin_simple_ret_excess_stand_5", "bin_extreme_ret_excess_stand_5")
X_model_sim$bin_extreme_ret_excess_stand_5
# summary(X_model_sim$eps_diff)
# X_model_sim <- X_model_sim[eps_diff > .1 | eps_diff < -.1] # sample here !
# dim(X_model_sim)
task_simple_on_extreme <- as_task_classif(na.omit(X_model_sim), id = "simple_on_extreme",
                                          target = labels[grep("extreme", labels)])
task_simple_on_extreme$select(test_)
preds_holdout <- at_classif$predict(task_simple_on_extreme)
as.data.table(task_simple_on_extreme)
preds_holdout$confusion
autoplot(preds_holdout, type = "roc")
preds_holdout$score(msrs(c("classif.acc", "classif.recall", "classif.precision", "classif.fbeta")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)
prediciotns_extreme_holdout <- prediciotns_extreme_holdout[prob.1 > 0.55]
nrow(prediciotns_extreme_holdout)
mlr3measures::acc(prediciotns_extreme_holdout$truth, prediciotns_extreme_holdout$response)


task_simple_extreme_holdout

# which variable correlate with extreme?
cols_ <- c(colnames(X_model)[3:which(colnames(X_model) == "DCOILWTICO_ret_week")], "ret_excess_stand_5")
test_ <- X_model[, ..cols_]
dim(test_)
test_[, 700:703]
test_[, 1:3]
# test_[, bin_extreme_ret_excess_stand_5 := as.integer(as.character(bin_extreme_ret_excess_stand_5))]
# test_ <- test_[!is.na(bin_extreme_ret_excess_stand_5)]
corr_bin <- cor(test_[, 1:702], test_$ret_excess_stand_5)
class(corr_bin)
head(corr_bin)
head(corr_bin[order(corr_bin[, 1], decreasing = TRUE), , drop = FALSE])

# predictions for qc
cols_qc <- c("symbol", "date")
predictoins_qc <- cbind(X_holdout[, ..cols_qc], as.data.table(preds_holdout))
predictoins_qc[, grep("row_ids|truth", colnames(predictoins_qc)) := NULL]
predictoins_qc <- unique(predictoins_qc)
setorder(predictoins_qc, "date")

# save to dropbox for live trading (create table for backtest)
cols <- c("date", "symbol", colnames(predictoins_qc)[4:ncol(predictoins_qc)])
pead_qc <- predictoins_qc[, ..cols]
pead_qc[, date := as.character(date)]
print(unique(pead_qc$symbol))
pead_qc <- pead_qc[, .(symbol = paste0(unlist(symbol), collapse = ", "),
                       prob1 = paste0(unlist(prob.1), collapse = ",")), by = date]
bl_endp_key <- storage_endpoint(Sys.getenv("BLOB-ENDPOINT"), key=Sys.getenv("BLOB-KEY"))
cont <- storage_container(bl_endp_key, "qc-backtest")
storage_write_csv2(pead_qc, cont, file = "hft.csv", col_names = FALSE)



# TRAIN FINAL MODEL -------------------------------------------------------
# train final model
hft_mlr3_model <- at_classif$learner$train(task_extreme)

# holdout extreme
preds_holdout <- hft_mlr3_model$predict(task_aroundzero_holdout)
preds_holdout$confusion
autoplot(preds_holdout, type = "roc")
preds_holdout$score(msrs(c("classif.acc", "classif.recall", "classif.precision", "classif.fbeta")))
prediciotns_extreme_holdout <- as.data.table(preds_holdout)
prediciotns_extreme_holdout <- prediciotns_extreme_holdout[`prob.1` > 0.60]
nrow(prediciotns_extreme_holdout)
mlr3measures::acc(prediciotns_extreme_holdout$truth,
                  prediciotns_extreme_holdout$response)
mlr3measures::acc(prediciotns_extreme_holdout$truth,
                  prediciotns_extreme_holdout$response)


# time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
# saveRDS(hft_mlr3_model,
#         paste0("D:/mlfin/mlr3_models/hft_mlr3_model-", time_, ".rds"))
# hft_mlr3_model
#
#
# hftmlr_model = readRDS(file = "D:/mlfin/mlr3_models/hft_mlr3_model-20220830164033.rds")
# saveRDS(hftmlr_model,
#         paste0("D:/mlfin/mlr3_models/hftmlr_model.rds"))



library(mlr3)
library(mlr3pipelines)
task = tsk("iris")
pop = po("scalemaxabs")
pop$train(list(task))[[1]]$data()

library(mlr3)
library(mlr3pipelines)
task = tsk("iris")
dt = task$data()
dt[, month := c(rep(1, 50), rep(2, 50), rep(3, 50))]
task = as_task_classif(dt, target = "Species", id = "iris")

task$data()[, lapply(.SD, function(x) as.vector(scale(x))), .SDcols = names(DT)[2:5], by = month]

pop = po("scalemaxabs")
pop$train(list(task))[[1]]$data()
