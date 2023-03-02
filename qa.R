library(mlr3)
library(mlr3tuning)
library(mlr3pipelines)
library(mlr3learners)


# task
task  = tsk("iris")
task_ = task$clone()
data_ = task_$data()
data_ = cbind(data_, monthid = c(rep(1, 30), rep(2, 30), rep(3, 30), rep(4, 30), rep(5, 30)))
task = as_task_classif(data_, target = "Species")

# inner custom rolling window resampling
custom_inner <- function(task, train_length = 2, test_length = 1) {
  custom = rsmp("custom")
  task_ <- task$clone()
  task_$set_col_roles("monthid", "group")
  groups = task_$groups
  rm(task_)
  groups_v <- groups[, unique(group)]
  train_groups <- lapply(0:(length(groups_v)-(train_length+1)), function(x) x + (1:train_length))
  test_groups <- lapply(train_groups, function(x) tail(x, 1) + test_length)
  train_sets <- lapply(train_groups, function(x) groups[group %in% groups_v[x], row_id])
  test_sets <- lapply(test_groups, function(x) groups[group %in% groups_v[x], row_id])
  custom$instantiate(task, train_sets, test_sets)
  return(custom)
}
customi = custom_inner(task)
customi$train_set(1)
customi$test_set(1)
customi$train_set(2)
customi$test_set(2)
customi$train_set(3)
customi$test_set(3)

# outer custom rolling window resampling
custom_outer <- function(task, train_length = 2, test_length = 1, test_length_out = 1) {
  customo = rsmp("custom")
  task_ <- task$clone()
  task_$set_col_roles("monthid", "group")
  groups = task_$groups
  rm(task_)
  groups_v <- groups[, unique(group)]
  train_length_out <- train_length + test_length
  test_length_out <- 1
  train_groups_out <- lapply(0:(length(groups_v)-(train_length_out+1)), function(x) x + (1:train_length_out))
  test_groups_out <- lapply(train_groups_out, function(x) tail(x, 1) + test_length_out)
  train_sets_out <- lapply(train_groups_out, function(x) groups[group %in% groups_v[x], row_id])
  test_sets_out <- lapply(test_groups_out, function(x) groups[group %in% groups_v[x], row_id])
  customo$instantiate(task, train_sets_out, test_sets_out)
}
customo = custom_outer(task)
customo$train_set(1)
customo$test_set(1)
customo$train_set(2)
customo$test_set(2)
tryCatch(customo$train_set(3), error = function(e) NULL)  # don't have data for test set
tryCatch(customo$test_set(3), error = function(e) NULL)   # don't have data for test set

# costruct graph
graph = po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("branch", options = c("nop_prep", "yeojohnson", "pca", "ica"), id = "prep_branch") %>>%
  gunion(list(po("nop", id = "nop_prep"), po("yeojohnson"), po("pca", scale. = TRUE), po("ica"))) %>>%
  po("unbranch", id = "prep_unbranch") %>>%
  po("learner", learner = lrn("classif.rpart"))
plot(graph)
graph_learner = as_learner(graph)
as.data.table(graph_learner$param_set)[1:70, .(id, class, lower, upper)]
search_space = ps(
  prep_branch.selection = p_fct(levels = c("nop_prep", "yeojohnson", "pca", "ica")),
  pca.rank. = p_int(2, 3, depends = prep_branch.selection == "pca"),
  ica.n.comp = p_int(2, 3, depends = prep_branch.selection == "ica"),
  yeojohnson.standardize = p_lgl(depends = prep_branch.selection == "yeojohnson")
)


bmrs = list()
for (i in seq_len(customi$iters)) { # seq_len(custom$iters)

  # debug
  # i = 1
  print(i)

  # stop if last fold (we don't have outofsample data)
  if (i == customi$iters) {
    break
  }

  # inner resampling
  custom_ = rsmp("custom")
  custom_$instantiate(task, list(customi$train_set(i)), list(customi$test_set(i)))

  # auto tuner
  at = auto_tuner(
    method = tnr("grid_search", batch_size = 2),
    learner = graph_learner,
    resampling = custom_,
    measure = msr("classif.acc"),
    search_space = search_space,
    term_evals = 2
  )

  # outer resampling
  customo_ = rsmp("custom")
  customo_$instantiate(task, list(customo$train_set(i)), list(customo$test_set(i)))

  # nested CV for one round
  design = benchmark_grid(
    tasks = list(task),
    learners = at,
    resamplings = customo_
  )
  bmr = benchmark(design, store_models = TRUE)
  bmrs[[i]] = bmr
}
