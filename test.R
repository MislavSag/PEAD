library(batchtools)
library(mlr3batchmark)

# load registry
reg = loadRegistry("experiments")

# get jobs
job_table = getJobTable(reg = reg)
print(job_table)



# # featureless baseline
# lrn_baseline = lrn("classif.featureless", id = "featureless")
#
# # logistic regression pipeline
# lrn_lr = lrn("classif.log_reg")
# lrn_lr = as_learner(ppl("robustify", learner = lrn_lr) %>>% lrn_lr)
# lrn_lr$id = "logreg"
# lrn_lr$fallback = lrn_baseline
# lrn_lr$encapsulate = c(train = "try", predict = "try")
#
# # random forest pipeline
# lrn_rf = lrn("classif.ranger")
# lrn_rf = as_learner(ppl("robustify", learner = lrn_rf) %>>% lrn_rf)
# lrn_rf$id = "ranger"
# lrn_rf$fallback = lrn_baseline
# lrn_rf$encapsulate = c(train = "try", predict = "try")
#
# learners = list(lrn_lr, lrn_rf, lrn_baseline)
#
# library(mlr3oml)
#
# otask_collection = ocl(id = 99)
#
# binary_cc18 = list_oml_tasks(
#   limit = 6,
#   task_id = otask_collection$task_ids,
#   number_classes = 2
# )
#
# # load tasks as a list
# otasks = lapply(binary_cc18$task_id, otsk)
#
# # convert to mlr3 tasks and resamplings
# tasks = as_tasks(otasks)
# resamplings = as_resamplings(otasks)
#
# large_design = benchmark_grid(tasks, learners, resamplings,
#                               paired = TRUE)
#
# library(batchtools)
#
# # create registry
# reg = makeExperimentRegistry(
#   file.dir = "./experiments",
#   seed = 1,
#   packages = "mlr3verse"
# )
# library(mlr3batchmark)
# batchmark(large_design, reg = reg)
#
# job_table = getJobTable(reg = reg)
