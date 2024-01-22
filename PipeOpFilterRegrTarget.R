PipeOpFilterRegrTarget = R6::R6Class(
  "PipeOpFilterRegrTarget",
  inherit = mlr3pipelines::PipeOpTaskPreproc,
  public = list(
    groups = NULL,
    initialize = function(id = "filter_target", param_vals = list()) {
      ps = ParamSet$new(params = list(
        ParamDbl$new("q", default = 0.1, lower = 0, upper = 1, tags = c("train", "filter_target"))
      ))
      ps$values = list(q = 0.1)
      super$initialize(id, param_set = ps, param_vals = param_vals, feature_types = c("numeric", "integer"))
    }
  ),

  private = list(
    .train_task = function(task) {
      # task = task_$clone()
      params = self$param_set$get_values(tags = "filter_target")
      # Filter logic: Keep only observations where target > 2
      self$state = list()
      data = task$data()
      target_q = quantile(data[[task$target_names]], probs = c(params$q, 1 - params$q))
      # target_q = quantile(data[, get(task$target_names)], probs = c(0.1, 1 - 0.1))
      include = data[[task$target_names]] >= target_q[2] |
        data[[task$target_names]] <= target_q[1]
      filtered_row_ids =  task$row_ids[include]
      task$filter(filtered_row_ids)
      task
    },

    .predict_task = function(task) {
      # No filtering for prediction
      task
    }
  )
)

# # try it out
# task_ = tsk("mtcars")
# pipeop_filter_test = PipeOpFilterRegrTarget$new()
# pipeop_filter_test$param_set$values$q = 0.1
# print(task_$nrow)
# filtered_task = pipeop_filter_test$train(list(task_$clone()))
# print(filtered_task$output$nrow)
