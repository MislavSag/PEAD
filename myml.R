library(data.table)
library(DescTools)
library(gausscov)



# read predictors
DT <- fread("D:/features/pead-predictors.csv")

# create group variable
DT[, monthid := paste0(data.table::year(as.Date(date, origin = "1970-01-01")),
                       data.table::month(as.Date(date, origin = "1970-01-01")))]

# choose label
print(paste0("Choose among this features: ",
             colnames(DT)[grep("^ret_excess_stand", colnames(DT))]))
LABEL = "ret_excess_stand_22"

# define features
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "ret_excess_stand_5", "ret_excess_stand_22", "ret_excess_stand_44", "ret_excess_stand_66",
                       colnames(DT)[grep("aroundzero", colnames(DT))],
                       colnames(DT)[grep("extreme", colnames(DT))],
                       colnames(DT)[grep("bin_simple", colnames(DT))],
                       colnames(DT)[grep("bin_decile", colnames(DT))],
                       "bmo_return", "amc_return",
                       "open", "high", "low", "close", "volume", "returns",
                       "monthid")
cols_features <- setdiff(colnames(DT), c(cols_non_features))

# convert columns to numeric. This is important only if we import existing features
chr_to_num_cols <- setdiff(colnames(DT[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
DT <- DT[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
int_to_num_cols <- colnames(DT[, .SD, .SDcols = is.integer])
DT <- DT[, (int_to_num_cols) := lapply(.SD, as.numeric), .SDcols = int_to_num_cols]
log_to_num_cols <- colnames(DT[, .SD, .SDcols = is.logical])
DT <- DT[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]


# define feature matrix
cols_keep <- c("symbol", "date", cols_features, LABEL, "monthid")
X <- DT[, ..cols_keep]

# winsorize LABEL
X[, (LABEL) := Winsorize(get(LABEL), probs = c(0.01, 0.99), na.rm = TRUE)]
X <- na.omit(X)
setorder(X, date)

# Instantiate Resampling
groups <- X[, unique(monthid)]
train_length <- 24
test_length <- 1
train_groups <- lapply(0:(length(groups)-(train_length+1)), function(x) x + (1:train_length))
test_groups <- lapply(train_groups, function(x) tail(x, 1) + test_length)
train_sets <- lapply(train_groups, function(x) groups[x])
test_sets <- lapply(test_groups, function(x) groups[x])

# ML loop
ml_results <- list()
for (i in seq_along(test_groups)) { # seq_along(test_groups)

  # debug i = 78
  print(i)

  # month ids
  train_ids = train_sets[[i]]
  test_ids = test_sets[[i]]

  # train / test split
  train_set <- X[monthid %in% train_ids]
  test_set <- X[monthid %in% test_ids]

  # define feature columns
  feature_cols <- colnames(train_set)[colnames(train_set) %in% cols_features]

  # macro vars
  macro_cols <- colnames(train_set)[which(colnames(train_set) == "tbl"):which(colnames(train_set) == "t10y2y")]

  # quantiles for winsorizing test test
  summary(test_set$q1_close_divergence_528)
  wins_cols <- feature_cols # setdiff(feature_cols, macro_cols)
  train_set[, q_ := paste0(data.table::year(as.Date(date, origin = as.Date("1970-01-01"))),
                           data.table::quarter(as.Date(date, origin = as.Date("1970-01-01"))))]
  q_lower <- train_set[,  lapply(.SD, function(x) quantile(x, probs = c(0.01), na.rm = TRUE)),
                       .SDcols = wins_cols,
                       by = q_]
  q_lower <- q_lower[, lapply(.SD, mean), .SDcols = colnames(q_lower)[-1]]
  q_upper <- train_set[,  lapply(.SD, function(x) quantile(x, probs = c(0.99), na.rm = TRUE)),
                       .SDcols = wins_cols,
                       by = q_]
  q_upper <- q_upper[, lapply(.SD, mean), .SDcols = colnames(q_upper)[-1]]
  test_set[, (wins_cols) := Map(function(a, b) ifelse(a < b, b, a),
                                .SD, q_lower), .SDcols = wins_cols]
  test_set[, (wins_cols) := Map(function(a, b) ifelse(a > b, b, a),
                                .SD, q_upper), .SDcols = wins_cols]

  # debug
  summary(test_set$q1_close_divergence_528)
  q_lower$q1_close_divergence_528
  q_upper$q1_close_divergence_528

  # winsorization train set
  train_set[, (wins_cols) := lapply(.SD, Winsorize, probs = c(0.01, 0.99), na.rm = TRUE),
            .SDcols = wins_cols,
            by = q_]
  train_set[, c("q_")  := NULL]

  # remove constant columns in set and remove same columns in test set
  features_ <- train_set[, ..feature_cols]
  remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
  # print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
  feature_cols <- setdiff(feature_cols, remove_cols)

  # remove highly correlated features
  features_ <- train_set[, ..feature_cols]
  cor_matrix <- cor(features_)
  cor_matrix_rm <- cor_matrix                  # Modify correlation matrix
  cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
  diag(cor_matrix_rm) <- 0
  remove_cols <- colnames(features_)[apply(cor_matrix_rm, 2, function(x) any(x > 0.99))]
  # print(paste0("Removing highly correlated featue (> 0.99): ", remove_cols))
  feature_cols <- setdiff(feature_cols, remove_cols)

  # remove missing values
  train_set <- na.omit(train_set, cols = feature_cols)
  test_set <- na.omit(test_set, cols = feature_cols)

  # uniformisation of features in test set
  test_uniformatization <- rbind(train_set[monthid %in% tail(train_ids, 3)], test_set)
  test_set_first_date = test_set[, head(date, 1)]
  dates_ = test_uniformatization[, unique(date)]
  # print(as.Date(dates_, origin = "1970-01-01"))
  dates_v <- c()
  test_uniform_l <- list()
  for (j in seq_along(dates_)) {
    d = dates_[j]
    dates_v <- c(dates_v, d)
    if (d < test_set_first_date) {
      next
    }
    y <- test_uniformatization[date %in% dates_v]
    y[, (feature_cols) := lapply(.SD, function(x) ecdf(x)(x)), .SDcols = feature_cols]
    test_uniform_l[[j]] = y[date == tail(dates_v, 1)]
  }
  X_test_unif = rbindlist(test_uniform_l)
  X_test_unif[, as.Date(date, origin = "1970-01-01")] # debug

  # uniformisation of features
  # TODO Should we uniform macro predictors
  train_set[, q_ := paste0(data.table::year(as.Date(date, origin = as.Date("1970-01-01"))),
                           data.table::quarter(as.Date(date, origin = as.Date("1970-01-01"))))]
  train_set[, (feature_cols) := lapply(.SD, function(x) ecdf(x)(x)),
            .SDcols = feature_cols,
            by = q_]
  train_set[, c("q_")  := NULL]

  # features selection
  y_train <- as.matrix(train_set[, get(LABEL)])
  X_train <- as.matrix(train_set[, .SD, .SDcols = feature_cols])
  f1st_fi_ <- f1st(y_train, X_train, kmn = 20, sub = TRUE)
  cov_index_f1st_1 <- colnames(as.matrix(X_train))[f1st_fi_[[1]][, 1]]
  # print(cov_index_f1st_1)
  # TODO: THIS IS SLOW, TRY IN SECOND ITERATION
  # f3st_1_ <- f3st(y_train, X_train, kmn = 10, m = 1)
  # f3st_1_index <- unique(as.integer(f3st_1_[[1]][1, ]))[-1]
  # f3st_1_index <- f3st_1_index[f3st_1_index != 0]
  # predictors_f3st_1 <- colnames(X_train)[f3st_1_index]

  # # train linear regression
  # cov_index_f1st_1 <- cov_index_f1st_1[-1]
  # X_train_imp <- X_train[, cov_index_f1st_1]
  # train_data <- cbind.data.frame(y = y_train, X_train_imp)
  # reg <- lm(y ~ ., data = train_data)
  # summary(reg)
  # # plot(reg)
  #
  # # sample data for test set
  # test_set_sample <- X_test_unif[, ..cov_index_f1st_1]
  # predictions <- predict(reg, newdata = test_set_sample)
  # # predictions <- sort(predictions, decreasing = TRUE)
  # cols_test <- c("symbol", "date", LABEL)
  # predictions_test <- cbind(test_set[, ..cols_test], predictions)
  # setorder(predictions_test, -predictions)
  # predictions_test[, .(mean = mean(predictions), median = median(predictions))]
  # predictions_test[, true := as.factor(ifelse(ret_excess_stand_22 > 0, 1, 0))]
  # predictions_test[, predict := as.factor(ifelse(predictions > 0, 1, 0))]

  # save all important objects
  save_obj = list(
    train_months = train_sets[[i]],
    test_monts = test_sets[[i]],
    f1st_1 =  f1st_fi_[[1]],
    f1st_2 =  f1st_fi_[[2]],
    f1st_3 =  f1st_fi_[[3]],
    f1st_predictors = cov_index_f1st_1
    # f3st_1_1 =  f1st_fi_[[1]],
    # f3st_1_2 =  f1st_fi_[[2]],
    # f3st_1_3 =  f1st_fi_[[3]],
    # f1st_predictors = cov_index_f1st_1,
    # train_data = train_data,
    # train_reg = reg,
    # test_data = test_set_sample,
    # predictions_test = predictions_test
  )

  # save results
  ml_results[[i]] <- save_obj
}

# inspect
length(ml_results)
length(unique(lengths(ml_results))) == 1
ml_results[[1]]

# save results
time_ = format.POSIXct(Sys.time(), format = "%y%m%d%H%M%S")
file_name = paste0("D:/mlfin/mypead/prad_ml_myapproach-", time_, ".rds")
saveRDS(ml_results, file_name)



# REG ---------------------------------------------------------------------
# most important predictors througt time
ip_l = lapply(ml_results, function(x) x$f1st_predictors)
ip = data.table(predictors = unlist(ip_l))
ip = ip[, .N, by = predictors]
setorder(ip, N)
tail(ip, 20)

# regression with n important predictors
predictions_best = lapply(seq_along(ml_results), function(i) {
  # debug
  print(i)

  # month ids
  train_ids = train_sets[[i]]
  test_ids = test_sets[[i]]

  # train / test split
  train_set <- X[monthid %in% train_ids]
  test_set <- X[monthid %in% test_ids]

  # keep important features
  feature_cols <- ml_results[[i]]$f1st_predictors

  # macro vars
  macro_cols <- colnames(train_set)[which(colnames(train_set) == "tbl"):which(colnames(train_set) == "t10y2y")]

  # quantiles for winsorizing test test
  wins_cols <- setdiff(feature_cols, macro_cols)
  train_set[, q_ := paste0(data.table::year(as.Date(date, origin = as.Date("1970-01-01"))),
                           data.table::quarter(as.Date(date, origin = as.Date("1970-01-01"))))]
  q_lower <- train_set[,  lapply(.SD, function(x) quantile(x, probs = c(0.01), na.rm = TRUE)),
                       .SDcols = wins_cols,
                       by = q_]
  q_lower <- q_lower[, lapply(.SD, mean), .SDcols = colnames(q_lower)[-1]]
  q_upper <- train_set[,  lapply(.SD, function(x) quantile(x, probs = c(0.99), na.rm = TRUE)),
                       .SDcols = wins_cols,
                       by = q_]
  q_upper <- q_upper[, lapply(.SD, mean), .SDcols = colnames(q_upper)[-1]]
  test_set[, (wins_cols) := Map(function(a, b) ifelse(a < b, b, a),
                                .SD, q_lower), .SDcols = wins_cols]
  test_set[, (wins_cols) := Map(function(a, b) ifelse(a > b, b, a),
                                .SD, q_upper), .SDcols = wins_cols]

  # winsorization train set
  train_set[, (wins_cols) := lapply(.SD, Winsorize, probs = c(0.01, 0.99), na.rm = TRUE),
            .SDcols = wins_cols,
            by = q_]
  train_set[, c("q_")  := NULL]

  # remove constant columns in set and remove same columns in test set
  features_ <- train_set[, ..feature_cols]
  remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
  print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
  feature_cols <- setdiff(feature_cols, remove_cols)

  # remove highly correlated features
  features_ <- train_set[, ..feature_cols]
  cor_matrix <- cor(features_)
  cor_matrix_rm <- cor_matrix                  # Modify correlation matrix
  cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
  diag(cor_matrix_rm) <- 0
  remove_cols <- colnames(features_)[apply(cor_matrix_rm, 2, function(x) any(x > 0.99))]
  print(paste0("Removing highly correlated featue (> 0.99): ", remove_cols))
  feature_cols <- setdiff(feature_cols, remove_cols)

  # remove missing values
  train_set <- na.omit(train_set, cols = feature_cols)
  test_set <- na.omit(test_set, cols = feature_cols)

  # uniformisation of features in test set
  test_uniformatization <- rbind(train_set[monthid %in% tail(train_ids, 3)], test_set)
  test_set_first_date = test_set[, head(date, 1)]
  dates_ = test_uniformatization[, unique(date)]
  # print(as.Date(dates_, origin = "1970-01-01"))
  dates_v <- c()
  test_uniform_l <- list()
  for (j in seq_along(dates_)) {
    d = dates_[j]
    dates_v <- c(dates_v, d)
    if (d < test_set_first_date) {
      next
    }
    y <- test_uniformatization[date %in% dates_v]
    y[, (feature_cols) := lapply(.SD, function(x) ecdf(x)(x)), .SDcols = feature_cols]
    test_uniform_l[[j]] = y[date == tail(dates_v, 1)]
  }
  X_test_unif = rbindlist(test_uniform_l)
  X_test_unif[, as.Date(date, origin = "1970-01-01")] # debug

  # uniformisation of features
  # TODO Should we uniform macro predictors
  train_set[, q_ := paste0(data.table::year(as.Date(date, origin = as.Date("1970-01-01"))),
                           data.table::quarter(as.Date(date, origin = as.Date("1970-01-01"))))]
  train_set[, (feature_cols) := lapply(.SD, function(x) ecdf(x)(x)),
            .SDcols = feature_cols,
            by = q_]
  train_set[, c("q_")  := NULL]

  # target and X
  y_train <- as.matrix(train_set[, get(LABEL)])
  X_train <- as.matrix(train_set[, .SD, .SDcols = feature_cols])

  # train linear regression
  n = 3
  X_train_imp <- X_train[, feature_cols[1:n]]
  train_data <- cbind.data.frame(y = y_train, X_train_imp)
  reg <- lm(y ~ ., data = train_data)
  summary(reg)
  # plot(reg)
  predictions_train = predict(reg)

  # sample data for test set
  cols = feature_cols[1:3]
  test_set_sample <- X_test_unif[, ..cols]
  predictions <- predict(reg, newdata = test_set_sample)
  cols_test <- c("symbol", "date", LABEL)
  predictions_test <- cbind(test_set[, ..cols_test], predictions)
  setorder(predictions_test, -predictions)
  predictions_test[, .(mean = mean(predictions), median = median(predictions))]
  predictions_test[, true := as.factor(ifelse(ret_excess_stand_22 > 0, 1, 0))]
  predictions_test[, predict := as.factor(ifelse(predictions > 0, 1, 0))]

  # save results
  list(
    predictions_train = predictions_train,
    predictions_test = predictions_test
  )
})

# accuracy on whole test sets
accs <- vapply(predictions_best, function(y) {
  train_predicitons_ = y$predictions_train
  test_predicitons_ = y$predictions_test
  truth = test_predicitons_$true
  resp = test_predicitons_$predict
  if (all(resp == 1)) {
    resp <- as.numeric(resp)
    resp <- factor(resp, levels = c("0", "1"))
  } else if (all(resp == 0)) {
    levels(resp) <- c("0", "1")
  }
  acc <- mlr3measures::acc(truth,
                           resp)
  acc
}, numeric(1))
mean(accs)
median(accs)


# prediction for highest return predictions
# y = predictions_best[[1]]
predictions_best_l <- lapply(predictions_best, function(y) {
  train_predicitons_ = y$predictions_train
  test_predicitons_ = y$predictions_test
  q_train <- quantile(train_predicitons_, 0.95)
  predictions_q <- test_predicitons_[predictions > q_train]
  if (nrow(predictions_q) == 0) {
    print(y$test_monts)
    return(NULL)
  }
  if (all(predictions_q$predict == 1) & length(levels(predictions_q$predict)) == 1) {
    predictions_q$predict <- as.numeric(predictions_q$predict)
    predictions_q$predict <- factor(predictions_q$predict, levels = c("0", "1"))
  } else if (all(predictions_q$predict == 0) & length(levels(predictions_q$predict)) == 1) {
    levels(predictions_q$predict) <- c("0", "1")
  }
  acc <- mlr3measures::acc(predictions_q$true,
                           predictions_q$predict)
  cbind.data.frame(obs = nrow(predictions_q), acc = acc)
})
predictions_best_ <- rbindlist(predictions_best_l)
# predictions_best_ <- cbind(vapply(ml_results, function(x) x[[2]], numeric(1)), predictions_best_)
predictions_best_[, .(mean = mean(acc), median = median(acc),
                      mean_weight = weighted.mean(acc, obs))]

#
predictions_best_l <- lapply(predictions_best, function(y) {
  train_predicitons_ = y$predictions_train
  test_predicitons_ = y$predictions_test
  q_train <- quantile(train_predicitons_, 0.90)
  predictions_q <- test_predicitons_[predictions > q_train]
  if (nrow(predictions_q) == 0) {
    print(y$test_monts)
    return(NULL)
  }
  if (all(predictions_q$predict == 1) & length(levels(predictions_q$predict)) == 1) {
    predictions_q$predict <- as.numeric(predictions_q$predict)
    predictions_q$predict <- factor(predictions_q$predict, levels = c("0", "1"))
  } else if (all(predictions_q$predict == 0) & length(levels(predictions_q$predict)) == 1) {
    levels(predictions_q$predict) <- c("0", "1")
  }
  cbind.data.frame(true = predictions_q$true, predict = predictions_q$predict)
})
predictions_best_ <- rbindlist(predictions_best_l)
mlr3measures::acc(predictions_best_$true,
                  predictions_best_$predict)

#



# INTERACTION -------------------------------------------------------------
# regression with n important predictors
predictions_best_int = lapply(seq_along(ml_results), function(i) {
  # debug
  print(i)

  # month ids
  train_ids = train_sets[[i]]
  test_ids = test_sets[[i]]

  # train / test split
  train_set <- X[monthid %in% train_ids]
  test_set <- X[monthid %in% test_ids]

  # keep important features
  feature_cols <- ml_results[[i]]$f1st_predictors

  # macro vars
  macro_cols <- colnames(train_set)[which(colnames(train_set) == "tbl"):which(colnames(train_set) == "t10y2y")]

  # quantiles for winsorizing test test
  wins_cols <- setdiff(feature_cols, macro_cols)
  train_set[, q_ := paste0(data.table::year(as.Date(date, origin = as.Date("1970-01-01"))),
                           data.table::quarter(as.Date(date, origin = as.Date("1970-01-01"))))]
  q_lower <- train_set[,  lapply(.SD, function(x) quantile(x, probs = c(0.01), na.rm = TRUE)),
                       .SDcols = wins_cols,
                       by = q_]
  q_lower <- q_lower[, lapply(.SD, mean), .SDcols = colnames(q_lower)[-1]]
  q_upper <- train_set[,  lapply(.SD, function(x) quantile(x, probs = c(0.99), na.rm = TRUE)),
                       .SDcols = wins_cols,
                       by = q_]
  q_upper <- q_upper[, lapply(.SD, mean), .SDcols = colnames(q_upper)[-1]]
  test_set[, (wins_cols) := Map(function(a, b) ifelse(a < b, b, a),
                                .SD, q_lower), .SDcols = wins_cols]
  test_set[, (wins_cols) := Map(function(a, b) ifelse(a > b, b, a),
                                .SD, q_upper), .SDcols = wins_cols]

  # winsorization train set
  train_set[, (wins_cols) := lapply(.SD, Winsorize, probs = c(0.01, 0.99), na.rm = TRUE),
            .SDcols = wins_cols,
            by = q_]
  train_set[, c("q_")  := NULL]

  # remove constant columns in set and remove same columns in test set
  features_ <- train_set[, ..feature_cols]
  remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
  print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
  feature_cols <- setdiff(feature_cols, remove_cols)

  # remove highly correlated features
  features_ <- train_set[, ..feature_cols]
  cor_matrix <- cor(features_)
  cor_matrix_rm <- cor_matrix                  # Modify correlation matrix
  cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
  diag(cor_matrix_rm) <- 0
  remove_cols <- colnames(features_)[apply(cor_matrix_rm, 2, function(x) any(x > 0.99))]
  print(paste0("Removing highly correlated featue (> 0.99): ", remove_cols))
  feature_cols <- setdiff(feature_cols, remove_cols)

  # remove missing values
  train_set <- na.omit(train_set, cols = feature_cols)
  test_set <- na.omit(test_set, cols = feature_cols)

  # uniformisation of features in test set
  test_uniformatization <- rbind(train_set[monthid %in% tail(train_ids, 3)], test_set)
  test_set_first_date = test_set[, head(date, 1)]
  dates_ = test_uniformatization[, unique(date)]
  # print(as.Date(dates_, origin = "1970-01-01"))
  dates_v <- c()
  test_uniform_l <- list()
  for (j in seq_along(dates_)) {
    d = dates_[j]
    dates_v <- c(dates_v, d)
    if (d < test_set_first_date) {
      next
    }
    y <- test_uniformatization[date %in% dates_v]
    y[, (feature_cols) := lapply(.SD, function(x) ecdf(x)(x)), .SDcols = feature_cols]
    test_uniform_l[[j]] = y[date == tail(dates_v, 1)]
  }
  X_test_unif = rbindlist(test_uniform_l)
  X_test_unif[, as.Date(date, origin = "1970-01-01")] # debug

  # uniformisation of features
  # TODO Should we uniform macro predictors
  train_set[, q_ := paste0(data.table::year(as.Date(date, origin = as.Date("1970-01-01"))),
                           data.table::quarter(as.Date(date, origin = as.Date("1970-01-01"))))]
  train_set[, (feature_cols) := lapply(.SD, function(x) ecdf(x)(x)),
            .SDcols = feature_cols,
            by = q_]
  train_set[, c("q_")  := NULL]

  # target and X
  y_train <- as.matrix(train_set[, get(LABEL)])
  X_train <- as.matrix(train_set[, .SD, .SDcols = feature_cols])

  # train linear regression
  X_train_imp <- X_train[, feature_cols]
  train_data <- cbind.data.frame(y = y_train, X_train_imp)
  train_data <- model.matrix(y ~ (.)^2 - 1, data = train_data)
  reg <- lm(y ~ ., data = cbind.data.frame(y = y_train, train_data))

  # Get the coefficient matrix
  coefs <- summary(reg)$coefficients
  vars <- rownames(coefs)[which(coefs[, 4] < 0.05)]
  vars = setdiff(vars, "(Intercept)")
  reg <- lm(y ~ ., data = cbind.data.frame(y = y_train, train_data[, gsub("`", "", vars)]))
  summary(reg)
  predictions_train = predict(reg)

  # sample data for test set
  test_set_sample <- X_test_unif[, ..feature_cols]
  test_set_sample <- model.matrix( ~ (.)^2 - 1, data = test_set_sample)
  test_set_sample <- test_set_sample[, gsub("`", "", vars), drop = FALSE]
  predictions <- predict(reg, newdata = as.data.frame(test_set_sample))
  cols_test <- c("symbol", "date", LABEL)
  predictions_test <- cbind(test_set[, ..cols_test], predictions)
  setorder(predictions_test, -predictions)
  predictions_test[, .(mean = mean(predictions), median = median(predictions))]
  predictions_test[, true := as.factor(ifelse(ret_excess_stand_22 > 0, 1, 0))]
  predictions_test[, predict := as.factor(ifelse(predictions > 0, 1, 0))]

  # save results
  list(
    predictions_train = predictions_train,
    predictions_test = predictions_test
  )
})

# accuracy on whole test sets
accs <- vapply(predictions_best_int, function(y) {
  train_predicitons_ = y$predictions_train
  test_predicitons_ = y$predictions_test
  truth = test_predicitons_$true
  resp = test_predicitons_$predict
  if (all(resp == 1)) {
    resp <- as.numeric(resp)
    resp <- factor(resp, levels = c("0", "1"))
  } else if (all(resp == 0)) {
    levels(resp) <- c("0", "1")
  }
  acc <- mlr3measures::acc(truth,
                           resp)
  acc
}, numeric(1))
mean(accs)
median(accs)

#
predictions_best_int_l <- lapply(predictions_best_int, function(y) {
  train_predicitons_ = y$predictions_train
  test_predicitons_ = y$predictions_test
  q_train <- quantile(train_predicitons_, 0.90)
  predictions_q <- test_predicitons_[predictions > q_train]
  if (nrow(predictions_q) == 0) {
    print(y$test_monts)
    return(NULL)
  }
  if (all(predictions_q$predict == 1) & length(levels(predictions_q$predict)) == 1) {
    predictions_q$predict <- as.numeric(predictions_q$predict)
    predictions_q$predict <- factor(predictions_q$predict, levels = c("0", "1"))
  } else if (all(predictions_q$predict == 0) & length(levels(predictions_q$predict)) == 1) {
    levels(predictions_q$predict) <- c("0", "1")
  }
  cbind.data.frame(true = predictions_q$true, predict = predictions_q$predict)
})
predictions_best_int_ <- rbindlist(predictions_best_int_l)
mlr3measures::acc(predictions_best_int_$true,
                  predictions_best_int_$predict)
