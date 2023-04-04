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
library(AzureStor)




# SETUP -------------------------------------------------------------------
# azure creds
blob_key = "0M4WRlV0/1b6b3ZpFKJvevg4xbC/gaNBcdtVZW+zOZcRi0ZLfOm1v/j2FZ4v+o8lycJLu1wVE6HT+ASt0DdAPQ=="
endpoint = "https://snpmarketdata.blob.core.windows.net/"
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)



# PREPARE DATA ------------------------------------------------------------
# read predictors
DT = fread("https://snpmarketdata.blob.core.windows.net/jphd/pead-predictors.csv")

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
print("Tasks")
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
customi = inner_split(task_ret_week)

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
customo = outer_split(task_ret_week)

# custom checks
(tail(customi$train_set(1), 1) + 1) == customi$test_set(1)[1] # test set start after train set 1
(tail(customi$train_set(2), 1) + 1) == customi$test_set(2)[1] # test set start after train set 2
all(c(customi$train_set(1), customi$test_set(1)) == customo$train_set(1)) # train set in outersample contains ids in innersample 1
all(c(customi$train_set(2), customi$test_set(2)) == customo$train_set(2)) # train set in outersample contains ids in innersample 1



# ADD PIPELINES -----------------------------------------------------------
print("Add pipelines")
PipeOpDropNA = R6::R6Class(
  "PipeOpDropNA",
  inherit = mlr3pipelines::PipeOpTaskPreproc,
  public = list(
    initialize = function(id = "drop.na") {
      super$initialize(id)
    }
  ),

  private = list(
    .train_task = function(task) {
      self$state = list()
      featuredata = task$data(cols = task$feature_names)
      exclude = apply(is.na(featuredata), 1, any)
      task$filter(task$row_ids[!exclude])
    },

    .predict_task = function(task) {
      # nothing to be done
      task
    }
  )
)

PipeOpDropNACol = R6::R6Class(
  "PipeOpDropNACol",
  inherit = mlr3pipelines::PipeOpTaskPreprocSimple,
  public = list(
    initialize = function(id = "drop.nacol", param_vals = list()) {
      ps = ParamSet$new(list(
        ParamDbl$new("cutoff", lower = 0, upper = 1, default = 0.05, tags = c("dropnacol_tag"))
      ))
      ps$values = list(cutoff = 0.2)
      super$initialize(id, param_set = ps, param_vals = param_vals)
    }
  ),

  private = list(
    .get_state = function(task) {
      pv = self$param_set$get_values(tags = "dropnacol_tag")
      features_names = task$feature_names
      data = task$data(cols = features_names)
      keep = sapply(data, function(column) (sum(is.na(column))) / length(column) < pv$cutoff)
      list(cnames = colnames(data)[keep])
    },

    .transform = function(task) {
      task$select(self$state$cnames)
    }
  )
)
PipeOpDropCorr = R6::R6Class(
  "PipeOpDropCorr",
  inherit = mlr3pipelines::PipeOpTaskPreprocSimple,
  public = list(
    initialize = function(id = "drop.const", param_vals = list()) {
      ps = ParamSet$new(list(
        ParamFct$new("use", c("everything", "all.obs", "complete.obs", "na.or.complete", "pairwise.complete.obs"), default = "everything"),
        ParamFct$new("method", c("pearson", "kendall", "spearman"), default = "pearson"),
        ParamDbl$new("cutoff", lower = 0, upper = 1, default = 0.99)
      ))
      ps$values = list(use = "everything", method = "pearson", cutoff = 0.99)
      super$initialize(id = id, param_set = ps, param_vals = param_vals, feature_types = c("numeric"))
    }
  ),

  private = list(
    .get_state = function(task) {
      # debug
      # pv = list(
      #   use = "everything",
      #   method = "pearson",
      #   cutoff = 0.9
      # )

      fn = task$feature_types[type == self$feature_types, id]
      data = task$data(cols = fn)
      pv = self$param_set$values

      cm = mlr3misc::invoke(stats::cor, x = data, use = pv$use, method = pv$method)
      cm[upper.tri(cm)] <- 0
      diag(cm) <- 0
      cm <- abs(cm)
      remove_cols <- colnames(data)[apply(cm, 2, function(x) any(x > pv$cutoff))]
      keep_cols <- setdiff(fn, remove_cols)
      list(cnames = keep_cols)
    },

    .transform = function(task) {
      task$select(self$state$cnames)
    }
  )
)
FilterGausscovF1st = R6::R6Class(
  "FilterGausscovF1st",
  inherit = mlr3filters::Filter,

  public = list(

    #' @description Create a GaussCov object.
    initialize = function() {
      param_set = ps(
        p0   = p_dbl(lower = 0, upper = 1, default = 0.01),
        kmn  = p_int(lower = 0, default = 0),
        kmx  = p_int(lower = 0, default = 0),
        mx   = p_int(lower = 1, default = 21),
        kex  = p_int(lower = 0, default = 0),
        sub  = p_lgl(default = TRUE),
        inr  = p_lgl(default = TRUE),
        xinr = p_lgl(default = FALSE),
        qq   = p_int(lower = 0, default = 0)
      )

      super$initialize(
        id = "gausscov_f1st",
        task_types = c("classif", "regr"),
        param_set = param_set,
        feature_types = c("integer", "numeric"),
        packages = "gausscov",
        label = "Gauss Covariance f1st",
        man = "mlr3filters::mlr_filters_gausscov_f1st"
      )
    }
  ),

  private = list(
    .calculate = function(task, nfeat) {
      # debug
      # pv = list(
      #   p0   = 0.01,
      #   kmn  = 0,
      #   kmx  = 0,
      #   mx   = 21,
      #   kex  = 0,
      #   sub  = TRUE,
      #   inr  = TRUE,
      #   xinr = FALSE,
      #   qq   = 0
      # )

      # empty vector with variable names as vector names
      scores = rep(-1, length(task$feature_names))
      scores = mlr3misc::set_names(scores, task$feature_names)

      # calculate gausscov pvalues
      pv = self$param_set$values
      x = as.matrix(task$data(cols = task$feature_names))
      if (task$task_type == "classif") {
        y = as.matrix(as.integer(task$truth()))
      } else {
        y = as.matrix(task$truth())
      }
      res = mlr3misc::invoke(gausscov::f1st, y = y, x = x, .args = pv)
      res_1 = res[[1]]
      res_1 = res_1[res_1[, 1] != 0, , drop = FALSE]
      scores[res_1[, 1]] = abs(res_1[, 4])
      sort(scores, decreasing = TRUE)
    }
  )
)
PipeOpUniform = R6::R6Class(
  "PipeOpUniform",
  inherit = mlr3pipelines::PipeOpTaskPreproc,
  public = list(
    groups = NULL,
    initialize = function(id = "uniformization", param_vals = list()) {
      super$initialize(id, param_vals = param_vals, feature_types = c("numeric", "integer"))
    }
  ),

  private = list(

    .select_cols = function(task) {
      self$groups = task$groups
      task$feature_names
    },

    .train_dt = function(dt, levels, target) {
      # state variables
      if (!(is.null(self$groups))) {
        row_ids  = self$groups[group == self$groups[nrow(self$groups), group], row_id]
        ecdf_ = mlr3misc::map(dt[row_ids], ecdf)
      } else {
        ecdf_ = mlr3misc::map(dt, ecdf)
      }
      self$state = list(
        ecdf_ = ecdf_
      )

      # dt object train
      if (!(is.null(self$groups))) {
        dt = dt[, lapply(.SD, function(x) as.vector(ecdf(x)(x))), by = self$groups[, group]]
        dt = dt[, -1]
      } else {
        dt = dt[, lapply(.SD, function(x) ecdf(x)(x))]
      }
      dt
    },

    .predict_dt = function(dt, levels) {
      dt[, Map(function(a, b) b(a), .SD, self$state$ecdf_)]
    }
  )
)
PipeOpWinsorizeSimple = R6::R6Class(
  "PipeOpWinsorizeSimple",
  inherit = mlr3pipelines::PipeOpTaskPreprocSimple,
  public = list(
    groups = NULL,
    initialize = function(id = "winsorization", param_vals = list()) {
      ps = ParamSet$new(list(
        ParamDbl$new("probs_low", lower = 0, upper = 1, default = 0.05, tags = c("winsorize_tag")),
        ParamDbl$new("probs_high", lower = 0, upper = 1, default = 0.95, tags = c("winsorize_tag")),
        ParamLgl$new("na.rm", default = TRUE, tags = c("winsorize_tag")),
        ParamInt$new("qtype", lower = 1L, upper = 9L, default = 7L, tags = c("winsorize_tag"))
      ))
      ps$values = list(qtype = 7L)
      super$initialize(id, param_set = ps, param_vals = param_vals, feature_types = c("numeric"))
    }
  ),

  private = list(

    .get_state_dt = function(dt, levels, target) {
      # debug
      # task = copy(tsk_aroundzero_month)
      # dt = tsk_aroundzero_month$data()
      # cols = tsk_aroundzero_month$feature_types[type %in% c("numeric", "integer"), id]
      # dt = dt[, ..cols]
      # pv = list(
      #   probs_low = 0.01, probs_high = 0.99, na.rm = TRUE, qtype = 7
      # )
      # self = list()

      # params
      pv = self$param_set$get_values(tags = "winsorize_tag")

      # state variables
      q = dt[, lapply(.SD,
                      quantile,
                      probs = c(pv$probs_low, pv$probs_high),
                      na.rm = pv$na.rm,
                      type = pv$qtype)]
      list(
        minvals = q[1],
        maxvals = q[2]
      )
    },

    .transform_dt  = function(dt, levels) {
      dt = dt[, Map(function(a, b) ifelse(a < b, b, a), .SD, self$state$minvals)]
      dt = dt[, Map(function(a, b) ifelse(a > b, b, a), .SD, self$state$maxvals)]
      dt
    }
  )
)
PipeOpWinsorize = R6::R6Class(
  "PipeOpWinsorize",
  inherit = mlr3pipelines::PipeOpTaskPreproc,
  public = list(
    groups = NULL,
    initialize = function(id = "winsorization", param_vals = list()) {
      ps = ParamSet$new(list(
        ParamDbl$new("probs_low", lower = 0, upper = 1, default = 0.05, tags = c("winsorize_tag")),
        ParamDbl$new("probs_high", lower = 0, upper = 1, default = 0.95, tags = c("winsorize_tag")),
        ParamLgl$new("na.rm", default = TRUE, tags = c("winsorize_tag")),
        ParamInt$new("qtype", lower = 1L, upper = 9L, default = 7L, tags = c("winsorize_tag"))
      ))
      ps$values = list(qtype = 7L)
      super$initialize(id, param_set = ps, param_vals = param_vals, feature_types = c("numeric", "integer"))
    }
  ),

  private = list(

    # .select_cols = function(task) {
    #   self$groups = task$groups
    #   cols = task$feature_types[type %in% self$feature_types, id]
    #   cols[cols %in% task$feature_names]
    # },

    .train_dt = function(dt, levels, target) {
      # debug
      # task = copy(tsk_aroundzero_month)
      # dt = tsk_aroundzero_month$data()
      # cols = tsk_aroundzero_month$feature_types[type %in% c("numeric", "integer"), id]
      # dt = dt[, ..cols]
      # pv = list(
      #   probs_low = 0.01, probs_high = 0.99, na.rm = TRUE, qtype = 7
      # )
      # self = list()

      # params
      pv = self$param_set$get_values(tags = "winsorize_tag")

      # state variables
      # if (!(is.null(self$groups))) {
      #   row_ids  = self$groups[group == self$groups[nrow(self$groups), group], row_id]
      #   q = dt[row_ids, lapply(.SD,
      #                          quantile,
      #                          probs = c(pv$probs_low, pv$probs_high),
      #                          na.rm = pv$na.rm,
      #                          type = pv$qtype)]
      # } else {
      q = dt[, lapply(.SD,
                      quantile,
                      probs = c(pv$probs_low, pv$probs_high),
                      na.rm = pv$na.rm,
                      type = pv$qtype)]
      # }
      self$state = list(
        minvals = q[1],
        maxvals = q[2]
      )

      # dt object train
      # if (!(is.null(self$groups))) {
      #   dt = dt[, lapply(.SD, function(x){
      #     q = quantile(x,
      #                  probs = c(pv$probs_low, pv$probs_high),
      #                  na.rm = pv$na.rm,
      #                  type = pv$qtype)
      #     minval = q[1L]
      #     maxval = q[2L]
      #     x[x < minval] <- minval
      #     x[x > maxval] <- maxval
      #     x
      #   }), by = self$groups[, group]]
      #   dt = dt[, -1]
      # } else {
      dt = dt[, Map(function(a, b) ifelse(a < b, b, a), .SD, self$state$minvals)]
      dt = dt[, Map(function(a, b) ifelse(a > b, b, a), .SD, self$state$maxvals)]
      # }
      dt
    },

    .predict_dt  = function(dt, levels) {
      dt = dt[, Map(function(a, b) ifelse(a < b, b, a), .SD, self$state$minvals)]
      dt = dt[, Map(function(a, b) ifelse(a > b, b, a), .SD, self$state$maxvals)]
      dt
    }
  )
)
Linex = R6::R6Class(
  "Linex",
  inherit = mlr3::MeasureRegr,
  public = list(
    initialize = function() {
      super$initialize(
        # custom id for the measure
        id = "linex",

        # additional packages required to calculate this measure
        packages = character(),

        # properties, see below
        properties = character(),

        # required predict type of the learner
        predict_type = "response",

        # feasible range of values
        range = c(0, Inf),

        # minimize during tuning?
        minimize = TRUE
      )
    }
  ),

  private = list(
    # custom scoring function operating on the prediction object
    .score = function(prediction, ...) {
      linex = function(truth, response, a1 = 1, a2 = -1) {
        mlr3measures:::assert_regr(truth, response = response)
        if (a2 == 0) stop("Argument a2 can't be 0.")
        if (a1 <= 0) stop("Argument a1 must be greater than 0.")
        e = truth - response
        mean(abs(a1 * (exp(-a2*e) - a2*e - 1)))
      }

      linex(prediction$truth, prediction$response)
      # linex(c(0, 0.5, 1), c(0.5, 0.5, 0.5))
      # root_mse = function(truth, response) {
      #   mse = mean((truth - response)^2)
      #   sqrt(mse)
      # }
      #
      # root_mse(prediction$truth, prediction$response)
      # root_mse(c(0, 0.5, 1), c(0.5, 0.5, 0.5))
    }
  )
)

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



# NESTED CV BENCHMARK -----------------------------------------------------
print("Nested banchmark")
# nested for loop
# future::plan("multisession", workers = 4L)
for (i in 28:tail(customi$iters, 1)) { # seq_len(customi$iters)

  # debug
  # i = 1
  print(i)

  # inner resampling
  print("Inner resampling")
  custom_ = rsmp("custom")
  custom_$instantiate(task_ret_week, list(customi$train_set(i)), list(customi$test_set(i)))

  # auto tuner
  print("Auto tuner")
  at = auto_tuner(
    tuner = tnr("mbo"), # tnr("random_search", batch_size = 3),
    learner = graph_learner,
    resampling = custom_,
    measure = msr("linex"),
    search_space = search_space,
    term_evals = 5
  )

  # outer resampling
  print("Outer resampling")
  customo_ = rsmp("custom")
  customo_$instantiate(task_ret_week, list(customo$train_set(i)), list(customo$test_set(i)))

  # nested CV for one round
  print("Design")
  design = benchmark_grid(
    tasks = list(task_ret_week, task_ret_month), # task_ret_month, task_ret_month2
    learners = at,
    resamplings = customo_
  )
  bmr = benchmark(design, store_models = TRUE)

  # save to azure blob
  print("Save")
  cont = storage_container(BLOBENDPOINT, "peadcv")
  time_ = format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  storage_save_rds(bmr, cont, paste0(i, "-", time_, ".rds"))
}
