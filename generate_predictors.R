options(progress_enabled = FALSE)

library(data.table)
library(checkmate)
library(arrow)
library(httr)
library(fredr)
library(alfred)
library(finfeatures)
library(gausscov)
library(mlr3)
library(mlr3verse)
library(ggplot2)
library(rpart.plot)
library(DescTools)
library(reticulate)
library(findata)
library(AzureStor)
library(fs)
library(duckdb)
# Python environment and python modules
# Instructions: some functions use python modules. Steps to use python include:
# 1. create new conda environment:
#    https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html
#    Choose python 3.8. for example:
#    conda create -n mlfinlabenv python=3.8
# 2. Install following packages inside environments
#    mlfinlab
#    tsfresh
#    TSFEL
# python packages
reticulate::use_python("C:/Users/Mislav/.conda/envs/mlfinlabenv/python.exe", required = TRUE)
# # mlfinlab = reticulate::import("mlfinlab", convert = FALSE)
# pd = reticulate::import("pandas", convert = FALSE)
builtins = import_builtins(convert = FALSE)
main = import_main(convert = FALSE)
tsfel = reticulate::import("tsfel", convert = FALSE)
# # tsfresh = reticulate::import("tsfresh", convert = FALSE)
warnigns = reticulate::import("warnings", convert = FALSE)
warnigns$filterwarnings('ignore')


# SET UP ------------------------------------------------------------------
# check if we have all necessary env variables
assert_choice("BLOB-ENDPOINT", names(Sys.getenv()))
assert_choice("BLOB-KEY", names(Sys.getenv()))
assert_choice("APIKEY-FMPCLOUD", names(Sys.getenv()))
assert_choice("FRED-KEY", names(Sys.getenv()))

# global vars
PATH = "F:/data/equity/us"

# parameters
strategy = "PEAD"  # PEAD (for predicting post announcement drift) or PRE (for predicting pre announcement)
events_data = "intersection" # data source, one of "fmp", "investingcom", "intersection"


# EARING ANNOUNCEMENT DATA ------------------------------------------------
# get events data
events = read_parquet(fs::path(PATH, "fundamentals", "earning_announcements", ext = "parquet"))

# coarse filtering
events <- events[date < Sys.Date()]                 # remove announcements for today
events <- unique(events, by = c("symbol", "date"))  # remove duplicated symbol / date pair
if (strategy == "PEAD") {
  print(paste0("Remove ", sum(is.na(events$eps)), " observations because of missing eps values or ",
               100 * round(sum(is.na(events$eps)) / nrow(events), 4), "% percent of data."))
  events <- na.omit(events, cols = c("eps")) # remove rows with NA for earnings
}

# keep only usa stocks
url <- modify_url("https://financialmodelingprep.com/",
                  path = "api/v3/stock/list",
                  query = list(apikey = Sys.getenv("APIKEY-FMPCLOUD")))
res <- GET(url)
stocks <- rbindlist(content(res), fill = TRUE)
stock_symbols <- stocks[type == "stock", symbol]
print(paste0("Remove ", nrow(events[!(symbol %in% stock_symbols)]),
             " observations because they are not stocks but ETF, fund etc., or  ",
             100 * round( nrow(events[!(symbol %in% stock_symbols)]) / nrow(events), 4),
             "% percent."))
events <- events[symbol %in% stock_symbols] # keep only stocks
us_symbols <- stocks[exchangeShortName %in% c("AMEX", "NASDAQ", "NYSE", "OTC"), symbol]
print(paste0("Remove ", nrow(events[!(symbol %in% us_symbols)]),
             " observations because they are not us stocks, or ",
             100 * round( nrow(events[!(symbol %in% us_symbols)]) / nrow(events), 4),
             "% percent."))
events <- events[symbol %in% us_symbols] # keep only US stocks

# get investing.com data
investingcom_ea = read_parquet(fs::path(PATH,
                                        "fundamentals",
                                        "earning_announcements_investingcom",
                                        ext = "parquet"))
if (strategy == "PEAD") {
  investingcom_ea <- na.omit(investingcom_ea, cols = c("eps", "eps_forecast"))
}
investingcom_ea <- investingcom_ea[, .(symbol, time, eps, eps_forecast, revenue, revenue_forecast, right_time)]
investingcom_ea[, date_investingcom := as.Date(time)]
setnames(investingcom_ea, colnames(investingcom_ea)[2:6], paste0(colnames(investingcom_ea)[2:6], "_investingcom"))

# merge DT and investing com earnings surprises
events <- merge(events, investingcom_ea,
                by.x = c("symbol", "date"),
                by.y = c("symbol", "date_investingcom"),
                all.x = TRUE, all.y = FALSE)

# choose events subsample
if (events_data == "intersection") {

  # keep only observations available in both datasets by checking dates
  events <- events[!is.na(date) & !is.na(as.Date(time_investingcom))]

  # replace FMP cloud data with investing.com data if FMP CLOUD data doesn't exists
  # events[, eps := ifelse(is.na(eps), eps_investingcom, eps)]
  # events[, epsEstimated := ifelse(is.na(epsEstimated), eps_forecast_investingcom, epsEstimated)]
  # events[, revenue := ifelse(is.na(revenue), revenue_investingcom, revenue)]
  # events[, revenueEstimated := ifelse(is.na(revenueEstimated), revenue_forecast_investingcom, revenueEstimated)]

  # check if time are the same
  events[!is.na(right_time) & right_time == "marketClosed ", right_time := "amc"]
  events[!is.na(right_time) & right_time == "marketOpen ", right_time := "bmo"]
  events[, same_announce_time := time == right_time]

  # if both fmp cloud and investing.com data exists keep similar
  print(paste0("Number of removed observations because of investing.com / FMP cloud disbalance is :",
               nrow(events[abs(eps - eps_investingcom) > 0.02]), " or ",
               round(nrow(events[abs(eps - eps_investingcom) > 0.02]) / nrow(events), 4) * 100, "% percent."))
  events <- events[abs(eps - eps_investingcom) < 0.02] # keep only observations where earnings are very similar

  # if PRE keep only same time
  if (strategy == "PRE") {
    # if both fmp cloud and investing.com data exists keep similar
    print(paste0("Number of removed observations because time of announcements are not same :",
                 sum(!((events$same_announce_time) == TRUE), na.rm = TRUE), " or ",
                 round(sum(!((events$same_announce_time) == TRUE), na.rm = TRUE) / nrow(events), 4) * 100, "% percent."))
    events <- events[events$same_announce_time == TRUE] # keep only observations where earnings are very similar
  }
}

# remove duplicated events
events <- unique(events, by = c("symbol", "date"))

# check last date
events[, max(date)]
events[date == max(date), symbol]


# MARKET DATA AND FUNDAMENTALS ---------------------------------------------
# import factors
prices_dt = read_parquet(fs::path(PATH,
                                  "predictors_daily",
                                  "factors",
                                  "prices_factors",
                                  ext = "parquet"))

# filter dates and symbols
prices_dt <- prices_dt[date > as.Date("2009-01-01")]
prices_dt <- prices_dt[symbol %in% c(unique(events$symbol), "SPY")]
prices_dt <- unique(prices_dt, by = c("symbol", "date"))
setorder(prices_dt, symbol, date)
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[which(prices_n$N > 700)]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n$symbol]

# SPY data
con <- dbConnect(duckdb::duckdb())
symbol = "SPY"
path_ = "F:/data/equity/daily_fmp_all.csv"
query <- sprintf("
  SELECT *
  FROM '%s'
  WHERE Symbol = '%s'
", path_, symbol)
data_ <- dbGetQuery(con, query)
dbDisconnect(con)
data_ = as.data.table(data_)
data_ = data_[, .(date = date, close = adjClose)]
data_[, returns := close / shift(close) - 1]
spy = na.omit(data_)


# REGRESSION LABELING ----------------------------------------------------------
# calculate returns
setorder(prices_dt, symbol, date)
prices_dt[, ret_5 := shift(close, -5L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]   # PEAD
prices_dt[, ret_22 := shift(close, -21L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"] # PEAD
prices_dt[, ret_44 := shift(close, -43L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"] # PEAD
prices_dt[, ret_66 := shift(close, -65L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"] # PEAD
prices_dt[, amc_return := shift(open, -1L, "shift") / close - 1, by = "symbol"] # PRE
prices_dt[, bmo_return := open / shift(close) - 1, by = "symbol"] # PRE

# calculate rolling sd
prices_dt[, sd_5 := roll::roll_sd(close / shift(close, 1L) - 1, 5), by = "symbol"]
prices_dt[, sd_22 := roll::roll_sd(close / shift(close, 1L) - 1, 22), by = "symbol"]
prices_dt[, sd_44 := roll::roll_sd(close / shift(close, 1L) - 1, 44), by = "symbol"]
prices_dt[, sd_66 := roll::roll_sd(close / shift(close, 1L) - 1, 66), by = "symbol"]

# calculate spy returns
spy[, ret_5_spy := shift(close, -5L, "shift") / shift(close, -1L, "shift") - 1]
spy[, ret_22_spy := shift(close, -21L, "shift") / shift(close, -1L, "shift") - 1]
spy[, ret_44_spy := shift(close, -43L, "shift") / shift(close, -1L, "shift") - 1]
spy[, ret_66_spy := shift(close, -65L, "shift") / shift(close, -1L, "shift") - 1]

# calculate excess returns
prices_dt <- merge(prices_dt,
                   spy[, .(date, ret_5_spy, ret_22_spy, ret_44_spy, ret_66_spy)],
                   by = "date", all.x = TRUE, all.y = FALSE)
prices_dt[, ret_5_excess := ret_5 - ret_5_spy]
prices_dt[, ret_22_excess := ret_22 - ret_22_spy]
prices_dt[, ret_44_excess := ret_44 - ret_44_spy]
prices_dt[, ret_66_excess := ret_66 - ret_66_spy]
prices_dt[, `:=`(ret_5_spy = NULL, ret_22_spy = NULL, ret_44_spy = NULL, ret_66_spy = NULL)]
setorder(prices_dt, symbol, date)

# calculate standardized returns
prices_dt[, ret_excess_stand_5 := ret_5_excess / shift(sd_5, -4L), by = "symbol"]
prices_dt[, ret_excess_stand_22 := ret_22_excess / shift(sd_22, -21L), by = "symbol"]
prices_dt[, ret_excess_stand_44 := ret_44_excess / shift(sd_44, -43L), by = "symbol"]
prices_dt[, ret_excess_stand_66 := ret_66_excess / shift(sd_66, -65L), by = "symbol"]

# remove unnecesary columns
prices_dt[, `:=`(ret_5 = NULL, ret_22 = NULL, ret_44 = NULL, ret_66 = NULL,
                 sd_5 = NULL, sd_22 = NULL, sd_44 = NULL, sd_66 = NULL,
                 ret_5_excess = NULL, ret_22_excess = NULL, ret_44_excess = NULL, ret_66_excess = NULL)]

# remove NA values
# This was uncomment in first version
# prices_dt <- na.omit(prices_dt, cols = c("symbol", "date", "ret_excess_stand_5",
#                                          "ret_excess_stand_22",  "ret_excess_stand_44",
#                                          "ret_excess_stand_66"))


# MERGE MARKET DATA, EVENTS AND CLASSIF LABELS ---------------------------------
# merge clf_data and labels
cols_ = colnames(prices_dt)
cols_keep_prices = c(
  "symbol", "date", "ret_excess_stand_5", "ret_excess_stand_22",
  "ret_excess_stand_44", "ret_excess_stand_66", "amc_return", "bmo_return",
  cols_[which(cols_ == "e"):(which(cols_ == "amc_return")-1)]
)
dataset <- merge(events, prices_dt[, ..cols_keep_prices],
                 by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)

# extreme labeling
possible_target_vars <- c("ret_excess_stand_5", "ret_excess_stand_22",
                          "ret_excess_stand_44", "ret_excess_stand_66")
bin_extreme_col_names <- paste0("bin_extreme_", possible_target_vars)
dataset[, (bin_extreme_col_names) := lapply(.SD, function(x) {
  y <- cut(x,
           quantile(x, probs = c(0, 0.2, 0.8, 1), na.rm = TRUE),
           labels = c(-1, NA, 1),
           include.lowest = TRUE)
  as.factor(droplevels(y))
}), .SDcols = possible_target_vars]

# around zero labeling
labeling_around_zero <- function(x) {
  x_abs <- abs(x)
  bin <- cut(x_abs, quantile(x_abs, probs = c(0, 0.3333), na.rm = TRUE), labels = 0L, include.lowest = TRUE)
  max_0 <- max(x[bin == 0], na.rm = TRUE)
  min_0 <- min(x[bin == 0], na.rm = TRUE)
  levels(bin) <- c(levels(bin), 1L, -1L)
  bin[x > max_0] <- as.character(1L)
  bin[x < min_0] <- as.factor(-1)
  return(bin)
}
bin_aroundzero_col_names <- paste0("bin_aroundzero_", possible_target_vars)
dataset[, (bin_aroundzero_col_names) := lapply(.SD, labeling_around_zero), .SDcols = possible_target_vars]

# simple labeling (ret > 0 -> 1, vice versa)
possible_target_vars <- c("ret_excess_stand_5", "ret_excess_stand_22", "ret_excess_stand_44", "ret_excess_stand_66")
bin_extreme_col_names <- paste0("bin_simple_", possible_target_vars)
dataset[, (bin_extreme_col_names) := lapply(.SD, function(x) {
  as.factor(ifelse(x > 0, 1, 0))
}), .SDcols = possible_target_vars]

# decile labeling
bin_decile_col_names <- paste0("bin_decile_", possible_target_vars)
dataset[, (bin_decile_col_names) := lapply(.SD, function(x) {
  y <- cut(x,
           quantile(x, probs = c(0, seq(0.1, 0.9, 0.1), 1), na.rm = TRUE),
           labels = 1:10,
           include.lowest = TRUE)
  as.factor(droplevels(y))
}), .SDcols = possible_target_vars]

# sort dataset
setorderv(dataset, c("symbol", "date"))


# FEATURES ----------------------------------------------------------------
# Ohlcv feaures
OhlcvInstance = Ohlcv$new(prices_dt[, .(symbol, date, open, high, low, close, volume)],
                          date_col = "date")
if (strategy == "PEAD") {
  lag_ <- -1L # negative lag means look forward
} else {
  lag_ <- 1L
  # ako je red u events amc. label je open_t+1 / close_t; lag je -1L
  # ako je red u events bmo. label je open_t / close_t-1; lag je -2L
}

# util function that returns most recently saved predictor object
get_latest = function(predictors = "RollingExuberFeatures") {
  f = file.info(list.files("D:/features", full.names = TRUE, pattern = predictors))
  if (length(f) == 0) {
    print(paste0("There is no file with ", predictors))
    return(NULL)
  }
  latest = tail(f[order(f$ctime), ], 1)
  row.names(latest)
}

# import existing data
f_fread = function(x) tryCatch(fread(get_latest(x)), error = function(e) NULL)
# OhlcvFeaturesSetSample = fread(get_latest("OhlcvFeaturesSetSample"))
RollingBidAskFeatures = f_fread("RollingBidAskFeatures")
RollingBackCusumFeatures = f_fread("RollingBackCusumFeatures")
RollingExuberFeatures = f_fread("RollingExuberFeatures")
RollingForecatsFeatures = f_fread("RollingForecatsFeatures")
RollingGpdFeatures = f_fread("RollingGpdFeatures")
RollingTheftCatch22Features = f_fread("RollingTheftCatch22Features")
RollingTheftTsfelFeatures = f_fread("RollingTheftTsfelFeatures")
RollingTsfeaturesFeatures = f_fread("RollingTsfeaturesFeatures")
RollingWaveletArimaFeatures = f_fread("RollingWaveletArimaFeatures")
RollingFracdiffFeatures = f_fread("RollingFracdiffFeatures")

# util function for identifying missing dates and create at_ object
get_at_ = function(predictors) {
  # debug
  # predictors = copy(RollingFracdiffFeatures)

  # test if previous data exists
  if (is.null(predictors)) {
    print("No data. ")
    new_dataset = dataset[, .(symbol, date = as.IDate(date))]
  } else {
    # get only new data
    new_dataset = fsetdiff(dataset[, .(symbol, date = as.IDate(date))],
                           predictors[, .(symbol, date)])
  }

  # new_dataset = new_dataset[date > as.Date("2021-01-01")]
  new_data <- merge(OhlcvInstance$X,
                    dataset[new_dataset[, .(symbol, date)]],
                    by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
  at_ <- which(!is.na(new_data$eps))
  at_
}

# BidAsk features
print("Calculate BidAsk features.")
at_ = get_at_(RollingBidAskFeatures)
if (length(at_) > 0) {
  RollingBidAskInstance <- RollingBidAsk$new(windows = c(5, 22, 22 * 6),
                                             workers = 4L,
                                             at = at_,
                                             lag = lag_,
                                             methods = c("EDGE", "Roll", "OHLC", "OHL.CHL"))
  RollingBidAskFeatures_new = RollingBidAskInstance$get_rolling_features(OhlcvInstance)
  gc()

  # merge and save
  RollingBidAskFeatures_new[, date := as.IDate(date)]
  RollingBidAskFeatures_new_merged = rbind(RollingBidAskFeatures, RollingBidAskFeatures_new)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingBidAskFeatures_new_merged, paste0("D:/features/PEAD-RollingBidAskFeatures-", time_, ".csv"))
}

# BackCUSUM features
print("Calculate BackCUSUM features.")
at_ = get_at_(RollingBackCusumFeatures)
if (length(at_) > 0) {
  RollingBackcusumInit = RollingBackcusum$new(windows = c(22 * 3, 22 * 6), workers = 4L,
                                              at = at_, lag = lag_,
                                              alternative = c("greater", "two.sided"),
                                              return_power = c(1, 2))
  RollingBackCusumFeatures_new = RollingBackcusumInit$get_rolling_features(OhlcvInstance)
  gc()

  # merge and save
  RollingBackCusumFeatures_new[, date := as.IDate(date)]
  RollingBackCusumFeatures_new_merged = rbind(RollingBackCusumFeatures, RollingBackCusumFeatures_new)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingBackCusumFeatures_new_merged, paste0("D:/features/PEAD-RollingBackCusumFeatures-", time_, ".csv"))
}

# Exuber features
print("Calculate Exuber features.")
at_ = get_at_(RollingExuberFeatures)
if (length(at_) > 0) {
  RollingExuberInit = RollingExuber$new(windows = c(100, 300, 600),
                                        workers = 6L,
                                        at = at_,
                                        lag = lag_,
                                        exuber_lag = 1L)
  RollingExuberFeaturesNew = RollingExuberInit$get_rolling_features(OhlcvInstance, TRUE)
  gc()

  # merge and save
  RollingExuberFeaturesNew[, date := as.IDate(date)]
  RollingExuberFeatures_new_merged = rbind(RollingExuberFeatures, RollingExuberFeaturesNew)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingExuberFeatures_new_merged, paste0("D:/features/PEAD-RollingExuberFeatures-", time_, ".csv"))
}

# Forecast Features
print("Calculate AutoArima features.")
at_ = get_at_(RollingForecatsFeatures)
if (length(at_) > 0) {
  RollingForecatsInstance = RollingForecats$new(windows = c(252 * 2), workers = 4L,
                                                lag = lag_, at = at_,
                                                forecast_type = c("autoarima", "nnetar", "ets"),
                                                h = 22)
  RollingForecatsFeaturesNew = RollingForecatsInstance$get_rolling_features(OhlcvInstance)

  # merge and save
  RollingForecatsFeaturesNew[, date := as.IDate(date)]
  RollingForecatsFeaturesNewMerged = rbind(RollingForecatsFeatures, RollingForecatsFeaturesNew)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingForecatsFeaturesNewMerged, paste0("D:/features/PEAD-RollingForecatsFeatures-", time_, ".csv"))
}

# # GAS
# print("Calculate GAS features.")
# RollingGasInit = RollingGas$new(windows = c(100, 252),
#                                 workers = 6L,
#                                 at = at_,
#                                 lag = lag_,
#                                 gas_dist = "sstd",
#                                 gas_scaling = "Identity",
#                                 prediction_horizont = 10)
# RollingGasFeatures = RollingGasInit$get_rolling_features(OhlcvInstance)
# # ERROR:
# #   Error in merge.data.table(x, y, by = c("symbol", "date"), all.x = TRUE,  :
# #                               Elements listed in `by` must be valid column names in x and y
# #                             In addition: Warning message:
# #                               In merge.data.table(x, y, by = c("symbol", "date"), all.x = TRUE,  :
# #
# #                                                     Error in merge.data.table(x, y, by = c("symbol", "date"), all.x = TRUE, :
# #                                                                                 Elements listed in `by` must be valid column names in x and y
#
# # save
# time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
# fwrite(RollingGasFeatures, paste0("D:/features/PEAD-RollingGasFeatures-", time_, ".csv"))

# Gpd features
print("Calculate Gpd features.")
at_ = get_at_(RollingGpdFeatures)
if (length(at_) > 0) {
  RollingGpdInit = RollingGpd$new(windows = c(22 * 3, 22 * 6), workers = 6L,
                                  at = at_, lag = lag_,
                                  threshold = c(0.03, 0.05, 0.07))
  RollingGpdFeaturesNew = RollingGpdInit$get_rolling_features(OhlcvInstance)

  # merge and save
  RollingGpdFeaturesNew[, date := as.IDate(date)]
  # cols = colnames(RollingGpdFeatures)
  # RollingGpdFeaturesNew = RollingGpdFeaturesNew[, ..cols]
  RollingGpdFeaturesNewMerged = rbind(RollingGpdFeatures, RollingGpdFeaturesNew, fill = TRUE)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingGpdFeaturesNewMerged, paste0("D:/features/PEAD-RollingGpdFeatures-", time_, ".csv"))
}

# theft catch22 features
print("Calculate Catch22 and feasts features.")
at_ = get_at_(RollingTheftCatch22Features)
if (length(at_) > 0) {
  RollingTheftInit = RollingTheft$new(windows = c(5, 22, 22 * 3, 22 * 12),
                                      workers = 6L, at = at_, lag = lag_,
                                      features_set = c("catch22", "feasts"))
  RollingTheftCatch22FeaturesNew = RollingTheftInit$get_rolling_features(OhlcvInstance)
  gc()

  # save
  RollingTheftCatch22FeaturesNew[, date := as.IDate(date)]
  RollingTheftCatch22Features[, c("feasts____22_5", "feasts____25_22") := NULL]
  # cols = colnames(RollingTheftCatch22Features)
  # RollingTheftCatch22FeaturesNew = RollingTheftCatch22FeaturesNew[, ..cols]
  RollingTheftCatch22FeaturesNewMerged = rbind(RollingTheftCatch22Features, RollingTheftCatch22FeaturesNew, fill = TRUE)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingTheftCatch22FeaturesNewMerged, paste0("D:/features/PEAD-RollingTheftCatch22Features-", time_, ".csv"))
}

# tsfeatures features
print("Calculate tsfeatures features.")
at_ = get_at_(RollingTsfeaturesFeatures)
if (length(at_) > 0) {
  RollingTsfeaturesInit = RollingTsfeatures$new(windows = c(22 * 3, 22 * 6),
                                                workers = 6L, at = at_,
                                                lag = lag_, scale = TRUE)
  RollingTsfeaturesFeaturesNew = RollingTsfeaturesInit$get_rolling_features(OhlcvInstance)
  gc()

  # save
  RollingTsfeaturesFeaturesNew[, date := as.IDate(date)]
  RollingTsfeaturesFeaturesNewMerged = rbind(RollingTsfeaturesFeatures, RollingTsfeaturesFeaturesNew)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingTsfeaturesFeaturesNewMerged, paste0("D:/features/PEAD-RollingTsfeaturesFeatures-", time_, ".csv"))
}

# theft tsfel features, Must be alone, because number of workers have to be 1L
print("Calculate tsfel features.")
at_ = get_at_(RollingTheftTsfelFeatures)
if (length(at_) > 0) {
  RollingTheftInit = RollingTheft$new(windows = c(22 * 3, 22 * 12), workers = 1L,
                                      at = at_, lag = lag_,  features_set = "TSFEL")
  RollingTheftTsfelFeaturesNew = suppressMessages(RollingTheftInit$get_rolling_features(OhlcvInstance))

  # save
  RollingTheftTsfelFeaturesNew[, date := as.IDate(date)]
  RollingTheftTsfelFeaturesNewMerged = rbind(RollingTheftTsfelFeatures, RollingTheftTsfelFeaturesNew, fill = TRUE)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingTheftTsfelFeaturesNewMerged, paste0("D:/features/PEAD-RollingTheftTsfelFeatures-", time_, ".csv"))
}

# # quarks
# at_ = get_at_(RollingQuarksFeatures)
# RollingQuarksInit = RollingQuarks$new(windows = 22 * 6, workers = 6L, at = at_,
#                                       lag = lag_, model = c("EWMA", "GARCH"),
#                                       method = c("plain", "age"))
# RollingQuarksFeaturesNew = RollingQuarksInit$get_rolling_features(OhlcvInstance)
# gc()
#
# # save
# RollingQuarksFeaturesNew[, date := as.IDate(date)]
# RollingQuarksFeaturesNewMerged = rbind(RollingQuarksFeatures, RollingQuarksFeaturesNew)
# time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
# fwrite(RollingQuarksFeaturesNewMerged, paste0("D:/features/PEAD-RollingQuarksFeatures-", time_, ".csv"))

# TVGARCH
# SLOW !!!
# Error:
# Error in checkForRemoteErrors(val) :
#   one node produced an error: system is computationally singular: reciprocal condition number = 1.63061e-16
# RollingTvgarchInit = RollingTvgarch$new(windows = 22 * 6,
#                                         workers = 6L,
#                                         at = at_,
#                                         lag = lag_,
#                                         na_pad = TRUE,
#                                         simplify = FALSE)
# RollingTvgarchFeatures = RollingTvgarchInit$get_rolling_features(OhlcvInstance)

# Wavelet arima
print("Wavelet predictors")
at_ = get_at_(RollingWaveletArimaFeatures)
if (is.null(RollingWaveletArimaFeatures)) {
  RollingWaveletArimaInstance = RollingWaveletArima$new(windows = 252, workers = 6L,
                                                        lag = lag_, at = at_, filter = "haar")
  RollingWaveletArimaFeaturesNew = RollingWaveletArimaInstance$get_rolling_features(OhlcvInstance)
  gc()
  RollingWaveletArimaFeaturesNew[, date := as.IDate(date)]
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingWaveletArimaFeaturesNew,
         paste0("D:/features/PEAD-RollingWaveletArimaFeatures-", time_, ".csv"))
} else if (length(at_) > 0) {
  RollingWaveletArimaInstance = RollingWaveletArima$new(windows = 252, workers = 6L,
                                                        lag = lag_, at = at_, filter = "haar")
  RollingWaveletArimaFeaturesNew = RollingWaveletArimaInstance$get_rolling_features(OhlcvInstance)
  gc()

  # save
  RollingWaveletArimaFeaturesNew[, date := as.IDate(date)]
  RollingWaveletArimaFeaturesNewMerged = rbind(RollingWaveletArimaFeatures, RollingWaveletArimaFeaturesNew)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingWaveletArimaFeaturesNewMerged, paste0("D:/features/PEAD-RollingWaveletArimaFeatures-", time_, ".csv"))
}

# Fracdiff
print("Fradiff predictors")
at_ = get_at_(RollingFracdiffFeatures)
if (is.null(RollingWaveletArimaFeatures)) {
  RollingFracdiffInstance = RollingFracdiff$new(windows = 252, workers = 6L,
                                                lag = lag_, at = at_,
                                                nar = c(1, 2), nma = c(1, 2),
                                                bandw_exp = c(0.1, 0.5, 0.9))
  RollingFracdiffFeaturesNew = RollingFracdiffInstance$get_rolling_features(OhlcvInstance)
  gc()
  RollingFracdiffFeaturesNew[, date := as.IDate(date)]
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingFracdiffFeaturesNew, paste0("D:/features/PEAD-RollingFracdiffFeatures-", time_, ".csv"))
} else if (length(at_) > 0) {
  RollingFracdiffInstance = RollingFracdiff$new(windows = 252, workers = 6L,
                                                lag = lag_, at = at_,
                                                nar = c(1, 2), nma = c(1, 2),
                                                bandw_exp = c(0.1, 0.5, 0.9))
  RollingFracdiffFeaturesNew = RollingFracdiffInstance$get_rolling_features(OhlcvInstance)
  gc()

  # save
  RollingFracdiffFeaturesNew[, date := as.IDate(date)]
  RollingFracdiffFeaturesNewMerged = rbind(RollingFracdiffFeatures, RollingFracdiffFeaturesNew)
  time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
  fwrite(RollingFracdiffFeaturesNewMerged, paste0("D:/features/PEAD-RollingFracdiffFeatures-", time_, ".csv"))
}

# prepare arguments for features
prices_events <- merge(prices_dt, dataset[, .(symbol, date, eps)],
                       by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
at_ <- which(!is.na(prices_events$eps))

# Features from OHLLCV
print("Calculate Ohlcv features.")
OhlcvFeaturesInit = OhlcvFeatures$new(at = NULL,
                                      windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12, 22 * 12 * 2),
                                      quantile_divergence_window =  c(22, 22*3, 22*6, 22*12, 22*12*2))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(OhlcvInstance)
OhlcvFeaturesSetSample <- OhlcvFeaturesSet[at_ - lag_]
setorderv(OhlcvFeaturesSetSample, c("symbol", "date"))
# DEBUG
events[date == max(date)]
events[date == max(date), symbol]
OhlcvFeaturesSet[symbol %in% events[date == max(date), symbol]]
head(dataset[, .(symbol, date)])
head(OhlcvFeaturesSetSample[symbol == "A", .(symbol, date)])
tail(dataset[, .(symbol, date)], 10)
OhlcvFeaturesSetSample[symbol == "ZYXI", .(symbol, date)]

# free memory
rm(OhlcvFeaturesSet)
gc()

# THINK THIS IS NOT NECESSARY
# # save Ohlcv data
# time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
# fwrite(OhlcvFeaturesSetSample, paste0("D:/features/PEAD-OhlcvFeaturesSetSample-", time_, ".csv"))

# util function that returns most recently saved predictor object
get_latest = function(predictors = "RollingExuberFeatures") {
  f = file.info(list.files("D:/features", full.names = TRUE, pattern = predictors))
  latest = tail(f[order(f$ctime), ], 1)
  row.names(latest)
}

# import all saved predictors
# OhlcvFeaturesSetSample = fread(get_latest("OhlcvFeaturesSetSample"))
RollingBidAskFeatures = fread(get_latest("RollingBidAskFeatures"))
RollingBackCusumFeatures = fread(get_latest("RollingBackCusumFeatures"))
RollingExuberFeatures = fread(get_latest("RollingExuberFeatures"))
RollingForecatsFeatures = fread(get_latest("RollingForecatsFeatures"))
RollingGpdFeatures = fread(get_latest("RollingGpdFeatures"))
RollingTheftCatch22Features = fread(get_latest("RollingTheftCatch22Features"))
RollingTheftTsfelFeatures = fread(get_latest("RollingTheftTsfelFeatures"))
RollingTsfeaturesFeatures = fread(get_latest("RollingTsfeaturesFeatures"))
# RollingQuarksFeatures = fread(get_latest("RollingQuarksFeatures"))
RollingWaveletArimaFeatures = fread(get_latest("RollingWaveletArimaFeatures")) # TODO: add i next itertion
# RollingFracdiffFeatures = fread(get_latest("RollingFracdiffFeatures")) # TODO: add i next itertion

# merge all features test
rolling_predictors <- Reduce(
  function(x, y) merge( x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
  list(
    RollingBidAskFeatures,
    RollingBackCusumFeatures,
    RollingExuberFeatures,
    RollingForecatsFeatures,
    # RollingGasFeatures,
    RollingGpdFeatures,
    RollingTheftCatch22Features,
    RollingTheftTsfelFeatures,
    RollingTsfeaturesFeatures,
    # RollingQuarksFeatures,
    RollingWaveletArimaFeatures
    # RollingFracdiffFeatures     # TODO Add this in next iteration
  )
)

# check
s = "AAPL"
tail(events[symbol == s])
tail(OhlcvFeaturesSetSample[symbol == s, 1:5])
tail(rolling_predictors[symbol == s, 1:5])

### Importmant notes:
# 1. OhlcvFeaturesSetSample has date columns one trading day after the event date.
# 2. Rolling predictors have date column that is the same as event date, but
# the predictor is calculated for one day lead
# 3. So, we have to merge OhlcvFeaturesSetSample with roling from behind.

# merge
rolling_predictors[, date_rolling := date]
OhlcvFeaturesSetSample[, date_ohlcv := date]
features <- rolling_predictors[OhlcvFeaturesSetSample, on = c("symbol", "date"), roll = Inf]

# check last date
features[, max(date)]

# check for duplicates
features[duplicated(features[, .(symbol, date)]), .(symbol, date)]
features[duplicated(features[, .(symbol, date_ohlcv)]), .(symbol, date_ohlcv)]
features[duplicated(features[, .(symbol, date_rolling)]), .(symbol, date_rolling)]
features[duplicated(features[, .(symbol, date_rolling)]) | duplicated(features[, .(symbol, date_rolling)], fromLast = TRUE),
         .(symbol, date, date_ohlcv, date_rolling)]
features = features[!duplicated(features[, .(symbol, date_rolling)])]

# merge features and events
any(duplicated(dataset[, .(symbol, date)]))
any(duplicated(features[, .(symbol, date_rolling)]))
features <- merge(features, dataset,
                  by.x = c("symbol", "date_rolling"), by.y = c("symbol", "date"),
                  all.x = TRUE, all.y = FALSE)
# features <- features[dataset, on = c("symbol", "date"), roll = -Inf]
features[, .(symbol, date, date_rolling, date_ohlcv)]
features[duplicated(features[, .(symbol, date)]), .(symbol, date)]
features[duplicated(features[, .(symbol, date_ohlcv)]), .(symbol, date_ohlcv)]
features[duplicated(features[, .(symbol, date_rolling)]), .(symbol, date_rolling)]

# remove missing ohlcv
features <- features[!is.na(date_ohlcv)]

# predictors from events data
features[, `:=`(
  nincr = frollsum(eps > epsEstimated, 4, na.rm = TRUE),
  nincr_half = frollsum(eps > epsEstimated, 2, na.rm = TRUE),
  nincr_2y = frollsum(eps > epsEstimated, 8, na.rm = TRUE),
  nincr_3y = frollsum(eps > epsEstimated, 12, na.rm = TRUE),
  eps_diff = (eps - epsEstimated + 0.00001) / (epsEstimated + 0.00001)
)]

# import fundamnetal fators
fundamentals = read_parquet(fs::path(PATH,
                                     "predictors-daily",
                                     "factors",
                                     "fundamental_factors",
                                     ext = "parquet"))

# clean fundamentals
fundamentals = fundamentals[date > as.Date("2009-01-01")]
fundamentals[, acceptedDateTime := as.POSIXct(acceptedDate, tz = "America/New_York")]
fundamentals[, acceptedDate := as.Date(acceptedDateTime)]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
fundamentals <- unique(fundamentals, by = c("symbol", "acceptedDate"))

# merge features and fundamental data
features[, date_day_after_event := date_ohlcv]
features = fundamentals[features, on = c("symbol", "acceptedDate" = "date_ohlcv"), roll = Inf]
features[, .(symbol, acceptedDate, acceptedDateTime, date_day_after_event, date)]

# remove unnecesary columns
features[, `:=`(period = NULL, link = NULL, finalLink = NULL,
                reportedCurrency = NULL, cik = NULL, calendarYear = NULL)]
features[symbol == "AAPL", .(symbol, fundamental_date, acceptedDate,
                             acceptedDateFundamentals, date_day_after_event, date)]

# convert char features to numeric features
char_cols <- features[, colnames(.SD), .SDcols = is.character]
char_cols <- setdiff(char_cols, c("symbol", "time", "right_time", "industry", "sector"))
features[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]



############# ADD TRANSCRIPTS #################
# import transcripts sentiments datadata
# config <- tiledb_config()
# config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
# config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
# config["vfs.s3.region"] <- "us-east-1"
# context_with_config <- tiledb_ctx(config)
# arr <- tiledb_array("s3://equity-transcripts-sentiments",
#                     as.data.frame = TRUE,
#                     query_layout = "UNORDERED",
# )
# system.time(transcript_sentiments <- arr[])
# tiledb_array_close(arr)
# sentiments_dt <- as.data.table(transcript_sentiments)
# setnames(sentiments_dt, "date", "time_transcript")
# attr(sentiments_dt$time, "tz") <- "UTC"
# sentiments_dt[, date := as.Date(time)]
# sentiments_dt[, time := NULL]
# cols_sentiment = colnames(sentiments_dt)[grep("FLS", colnames(sentiments_dt))]

# merge with features
# features[, date_day_after_event_ := date_day_after_event]
# features <- sentiments_dt[features, on = c("symbol", "date" = "date_day_after_event_"), roll = Inf]
# features[, .(symbol, date, date_day_after_event, time_transcript, Not_FLS_positive)]
# features[1:50, .(symbol, date, date_day_after_event, time_transcript, Not_FLS_positive)]

# remove observations where transcripts are more than 2 days away
# features <- features[date - as.IDate(as.Date(time_transcript)) >= 3,
#                      (cols_sentiment) := NA]
# features[, ..cols_sentiment]
############# ADD TRANSCRIPTS ###############

# import macro factors
macros = read_parquet(fs::path(PATH,
                               "predictors-daily",
                               "factors",
                               "macro_factors",
                               ext = "parquet"))

# macro data
features[, date_day_after_event_ := date_day_after_event]
macros[, date_macro := date]
features <- macros[features, on = c("date" = "date_day_after_event_"), roll = Inf]
features[, .(symbol, date, date_day_after_event, date_macro, vix)]

# final checks for predictors
any(duplicated(features[, .(symbol, date_day_after_event)]))
features[duplicated(features[, .(symbol, date_day_after_event)]), .(symbol, date_day_after_event)]
features[duplicated(features[, .(symbol, date)]), .(symbol, date)]


# FEATURES SPACE ----------------------------------------------------------
# features space from features raw
cols_remove <- c("trading_date_after_event", "time", "datetime_investingcom",
                 "eps_investingcom", "eps_forecast_investingcom", "revenue_investingcom",
                 "revenue_forecast_investingcom", "time_dummy",
                 "trading_date_after_event", "fundamental_date", "cik", "link", "finalLink",
                 "fillingDate", "calendarYear", "eps.y", "revenue.y", "period.x", "period.y",
                 "acceptedDateTime", "acceptedDateFundamentals", "reportedCurrency",
                 "fundamental_acceptedDate", "period", "right_time",
                 "updatedFromDate", "fiscalDateEnding", "time_investingcom",
                 "same_announce_time", "eps", "epsEstimated", "revenue", "revenueEstimated",
                 "same_announce_time", "time_transcript", "i.time",
                 # remove dates we don't need
                 setdiff(colnames(features)[grep("date", colnames(features), ignore.case = TRUE)], c("date", "date_rolling")),
                 # remove columns with i - possible duplicates
                 colnames(features)[grep("i\\.|\\.y", colnames(features))]
                 )
cols_non_features <- c("symbol", "date", "date_rolling", "time", "right_time",
                       "ret_excess_stand_5", "ret_excess_stand_22", "ret_excess_stand_44", "ret_excess_stand_66",
                       colnames(features)[grep("aroundzero", colnames(features))],
                       colnames(features)[grep("extreme", colnames(features))],
                       colnames(features)[grep("bin_simple", colnames(features))],
                       colnames(features)[grep("bin_decile", colnames(features))],
                       "bmo_return", "amc_return")
                       # "open", "high", "low", "close", "volume", "returns")
cols_features <- setdiff(colnames(features), c(cols_remove, cols_non_features))
head(cols_features, 10)
tail(cols_features, 500)
cols <- c(cols_non_features, cols_features)
features <- features[, .SD, .SDcols = cols]

# checks
features[, max(date)]
features[, .(symbol, date, date_rolling)]



# CLEAN DATA --------------------------------------------------------------
# convert columns to numeric. This is important only if we import existing features
clf_data <- copy(features)
chr_to_num_cols <- setdiff(colnames(clf_data[, .SD, .SDcols = is.character]),
                           c("symbol", "time", "right_time", "industry", "sector"))
clf_data <- clf_data[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
# int_to_num_cols <- colnames(clf_data[, .SD, .SDcols = is.integer])
# clf_data <- clf_data[, (int_to_num_cols) := lapply(.SD, as.numeric), .SDcols = int_to_num_cols]
log_to_num_cols <- colnames(clf_data[, .SD, .SDcols = is.logical])
clf_data <- clf_data[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]

# remove duplicates
any(duplicated(clf_data[, .(symbol, date)]))
clf_data <- unique(clf_data, by = c("symbol", "date"))

# remove columns with many NA
keep_cols <- names(which(colMeans(!is.na(clf_data)) > 0.5))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(clf_data), c(keep_cols, "right_time"))))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(clf_data))) > 0.98))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(clf_data), keep_cols)))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove inf values
n_0 <- nrow(clf_data)
clf_data <- clf_data[is.finite(rowSums(clf_data[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
n_1 <- nrow(clf_data)
print(paste0("Removing ", n_0 - n_1, " rows because of Inf values"))

# final checks
clf_data[, .(symbol, date, date_rolling)]
features[, .(symbol, date, date_rolling)]
features[, max(date)]
clf_data[, max(date)]

# save features
last_pead_date = strftime(clf_data[, max(date)], "%Y%m%d")
file_name = paste0("pead-predictors-", last_pead_date, ".csv")
file_name_local = fs::path("D:/features", file_name)
fwrite(clf_data, file_name_local)

# save to Azure blob
endpoint = "https://snpmarketdata.blob.core.windows.net/"
blob_key = readLines('./blob_key.txt')
BLOBENDPOINT = storage_endpoint(endpoint, key=blob_key)
cont = storage_container(BLOBENDPOINT, "jphd")
storage_write_csv(clf_data, cont, file_name)



# Ah, I see. You're looking to engineer new features by transforming and combining the existing ones to potentially capture more complex relationships in the data. Here are some techniques to increase the feature space:
#
# Differencing:
#
# For time series data, the difference between consecutive observations can be used as a feature.
# For non-time series data, you can compute the difference between two or more related features.
# Squares, Cubes, and Higher-order Polynomials:
#
#  ,… can capture non-linear relationships.
# Interaction Features:
#
# As mentioned earlier, you can create new features that are the product of two or more existing features.
# Ratios:
#
# Compute the ratio of two features, which can be particularly useful if the relationship between the two features is multiplicative or if one feature can be a scale factor of another.
# Rolling Statistics:
#
# For time series data, you can compute rolling statistics like rolling mean, rolling standard deviation, etc., over a window of time.
# Lag Features:
#
# For time series data, values from previous time steps (lags) can be used as features.
# Cumulative Sum:
#
# The running total of a feature can sometimes provide additional insights, especially in time series data.
# Moving Averages:
#
# Simple moving averages or exponentially weighted moving averages can be used, especially for time series data.
# Statistical Transformations:
#
# Features like z-scores, percentiles, or ranks of a feature can be added.
# Frequency Encoding:
#
# For categorical features, you can create a new feature representing the frequency (or probability) of each category.
# Embeddings:
#
# For high cardinality categorical features or text data, embeddings (like word2vec or entity embeddings) can be used to represent the data in a dense form.
# Time Since:
#
# For event data or time series, the time since the last event or a particular condition was met.
# Aggregated Features:
#
# Group by one or more features and compute aggregated statistics (mean, sum, variance, etc.) on another feature.
# Cross-features:
#
# Combine two categorical features into a single feature, which can then be one-hot encoded or target encoded.
# Decomposition Techniques:
#
# Techniques like PCA (Principal Component Analysis), t-SNE, or UMAP can be used to create new features, especially when dealing with high-dimensional data.
# Wavelet Transform:
#
# For time series data, wavelet transforms can be used to decompose the series into different frequency components.
# Fourier Transform:
#
# Decompose time series data into its constituent frequencies.
# When engineering new features, it's essential to keep in mind:
#
#   Overfitting: Adding too many features, especially in relation to the number of observations, can lead to overfitting.
# Collinearity: Some of these transformations can introduce multicollinearity, which can be problematic for certain algorithms.
# Computational Complexity: More features mean more data to process, which can increase the computational requirements.
# Interpretability: While some models might benefit from a larger feature space, the model's interpretability might decrease.
# Always validate the effectiveness of the new features using techniques like cross-validation. Not all engineered features will necessarily improve model performance, so it's essential to be selective and iterative in your approach.
