library(data.table)
library(mlr3)


synology_path = "C:/Users/Mislav/SynologyDrive"
dta_path = file.path(synology_path, "H2")

# benchmark files ----
files_ = list.files(dta_path, full.names = TRUE)
files_info = file.info(files_)
files_info = files_info[order(files_info$ctime), ]

# read banchmark results
bmr = readRDS(files_[1])
bmr_dt = as.data.table(bmr)

# aggreaget results
aggregate = bmr$aggregate(msrs(c("regr.mse", "regr.mae", "regr.rmse")))

# instances across folds
rresults = bmr$resample_result(1)
instance = rresults$learners[[1]]$state$model$tuning_instance

# number of components
bmr_dt$learner[[1]]$state$model$learner$state
bmr_dt$learner[[1]]$state$model$learner$state$model$pca_explained
bmr_dt$learner[[3]]$state$model$learner$state$model$pca_explained

bmr_dt$learner[[1]]$state$model$learner$state$model$pca_explained
x = bmr$resample_result(1)
x$learners[[1]]$state$model$learner$state$model$pca_explained

x$learners[[1]]$state$model$learner$model$pca_explained

#
bmr_dt$learner[[1]]$state$model$learner$state$model$ranger.ranger$model$prediction.error
bmr_dt$learner[[1]]$state$model$learner$state$model$xgboost.xgboost

