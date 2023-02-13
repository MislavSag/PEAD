options(progress_enabled = FALSE)

library(data.table)
library(checkmate)
library(tiledb)
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
reticulate::use_python("C:/ProgramData/Anaconda3/envs/mlfinlabenv/python.exe", required = TRUE)
mlfinlab = reticulate::import("mlfinlab", convert = FALSE)
pd = reticulate::import("pandas", convert = FALSE)
builtins = import_builtins(convert = FALSE)
main = import_main(convert = FALSE)
tsfel = reticulate::import("tsfel", convert = FALSE)



# SET UP ------------------------------------------------------------------
# check if we have all necessary env variables
assert_choice("AWS-ACCESS-KEY", names(Sys.getenv()))
assert_choice("AWS-SECRET-KEY", names(Sys.getenv()))
assert_choice("AWS-REGION", names(Sys.getenv()))
assert_choice("BLOB-ENDPOINT", names(Sys.getenv()))
assert_choice("BLOB-KEY", names(Sys.getenv()))
assert_choice("APIKEY-FMPCLOUD", names(Sys.getenv()))
assert_choice("FRED-KEY", names(Sys.getenv()))

# set credentials
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)
fredr_set_key(Sys.getenv("FRED-KEY"))

# parameters
strategy = "PEAD"  # PEAD (for predicting post announcement drift) or PRE (for predicting pre announcement)
start_holdout_date = as.Date("2021-06-01") # observations after this date belongs to holdout set
events_data <- "intersection" # data source, one of "fmp", "investingcom", "intersection"



# EARING ANNOUNCEMENT DATA ------------------------------------------------
# get events data from FMP
arr <- tiledb_array("s3://equity-usa-earningsevents", as.data.frame = TRUE)
events <- arr[]
events <- as.data.table(events)
setorder(events)

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
arr <- tiledb_array("s3://equity-usa-earningsevents-investingcom",
                    as.data.frame = TRUE)
investingcom_ea <- arr[]
investingcom_ea <- as.data.table(investingcom_ea)
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



# MARKET DATA AND FUNDAMENTALS ---------------------------------------------
# import market data and fundamentals
factors = Factors$new()
factors_l = factors$get_factors()
price_factors <- factors_l$prices_factos
fundamental_factors <- factors_l$fundamental_factors
macro <- factors_l$macro

# free resources
rm(factors_l)
gc()

# filter dates and symbols
prices_dt <- unique(price_factors, by = c("symbol", "date"))
setorder(prices_dt, symbol, date)
prices_dt <- prices_dt[symbol %in% c(unique(events$symbol), "SPY")]
prices_dt <- prices_dt[date > as.Date("2010-01-01")]
prices_n <- prices_dt[, .N, by = symbol]
prices_n <- prices_n[which(prices_n$N > 700)]  # remove prices with only 700 or less observations
prices_dt <- prices_dt[symbol %in% prices_n$symbol]

# save SPY for later and keep only events symbols
spy <- prices_dt[symbol == "SPY", .(symbol, date, open, high, low, close, volume, returns)]



# REGRESSION LABELING ----------------------------------------------------------
# calculate returns
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
spy[, ret_5_spy := shift(close, -5L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
spy[, ret_22_spy := shift(close, -21L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
spy[, ret_44_spy := shift(close, -43L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]
spy[, ret_66_spy := shift(close, -65L, "shift") / shift(close, -1L, "shift") - 1, by = "symbol"]

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
prices_dt <- na.omit(prices_dt, cols = c("symbol", "date", "ret_excess_stand_5",
                                         "ret_excess_stand_22",  "ret_excess_stand_44",
                                         "ret_excess_stand_66"))



# MERGE MARKET DATA, EVENTS AND CLASSIF LABELS ---------------------------------
# merge clf_data and labels
dataset <- merge(events,
                 prices_dt[, .(symbol, date,
                               ret_excess_stand_5, ret_excess_stand_22,
                               ret_excess_stand_44, ret_excess_stand_66,
                               amc_return, bmo_return)],
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
# prepare arguments for features
prices_events <- merge(prices_dt, dataset[, .(symbol, date, eps)],
                       by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
at_ <- which(!is.na(prices_events$eps))

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

# free memory
rm(prices_events)
gc()

# Features from OHLLCV
print("Calculate Ohlcv features.")
OhlcvFeaturesInit = OhlcvFeatures$new(at = NULL,
                                      windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12, 22 * 12 * 2),
                                      quantile_divergence_window =  c(22, 22*3, 22*6, 22*12, 22*12*2))
OhlcvFeaturesSet = OhlcvFeaturesInit$get_ohlcv_features(OhlcvInstance)
OhlcvFeaturesSetSample <- OhlcvFeaturesSet[at_ - lag_]
setorderv(OhlcvFeaturesSetSample, c("symbol", "date"))
# DEBUG
head(dataset[, .(symbol, date)])
head(OhlcvFeaturesSetSample[symbol == "A", .(symbol, date)])
tail(dataset[, .(symbol, date)], 10)
OhlcvFeaturesSetSample[symbol == "ZYXI", .(symbol, date)]

# free memory
rm(OhlcvFeaturesSet)
gc()

# BidAsk features
print("Calculate BidAsk features.")
RollingBidAskInstance <- RollingBidAsk$new(windows = c(5, 22, 22 * 6),
                                           workers = 8L,
                                           at = at_,
                                           lag = lag_,
                                           methods = c("EDGE", "Roll", "OHLC", "OHL.CHL"))
RollingBidAskFeatures = RollingBidAskInstance$get_rolling_features(OhlcvInstance)
gc()
# DEBUG
head(dataset[, .(symbol, date)])
head(RollingBidAskFeatures[symbol == "AA", .(symbol, date)])

# BackCUSUM features
print("Calculate BackCUSUM features.")
RollingBackcusumInit = RollingBackcusum$new(windows = c(22 * 3, 22 * 6), workers = 8L,
                                            at = at_, lag = lag_,
                                            alternative = c("greater", "two.sided"),
                                            return_power = c(1, 2))
RollingBackCusumFeatures = RollingBackcusumInit$get_rolling_features(OhlcvInstance)

# Exuber features
print("Calculate Exuber features.")
RollingExuberInit = RollingExuber$new(windows = c(100, 300, 600),
                                      workers = 8L,
                                      at = at_,
                                      lag = lag_,
                                      exuber_lag = 1L)
RollingExuberFeatures = RollingExuberInit$get_rolling_features(OhlcvInstance, TRUE)
head(dataset[, .(symbol, date)])
head(RollingExuberFeatures[symbol == "A", .(symbol, date)])
gc()

# Forecast Features
print("Calculate AutoArima features.")
RollingForecatsInstance = RollingForecats$new(windows = c(252 * 2), workers = 8L,
                                              lag = lag_, at = at_,
                                              forecast_type = c("autoarima", "nnetar", "ets"),
                                              h = 22)
RollingForecatsFeatures = RollingForecatsInstance$get_rolling_features(OhlcvInstance)

# GAS
print("Calculate GAS features.")
RollingGasInit = RollingGas$new(windows = c(100, 252),
                                workers = 8L,
                                at = at_,
                                lag = lag_,
                                gas_dist = "sstd",
                                gas_scaling = "Identity",
                                prediction_horizont = 10)
RollingGasFeatures = RollingGasInit$get_rolling_features(OhlcvInstance)
# ERROR:
#   Error in merge.data.table(x, y, by = c("symbol", "date"), all.x = TRUE,  :
#                               Elements listed in `by` must be valid column names in x and y
#                             In addition: Warning message:
#                               In merge.data.table(x, y, by = c("symbol", "date"), all.x = TRUE,  :
#
#                                                     Error in merge.data.table(x, y, by = c("symbol", "date"), all.x = TRUE, :
#                                                                                 Elements listed in `by` must be valid column names in x and y

# Gpd features
print("Calculate Gpd features.")
RollingGpdInit = RollingGpd$new(windows = c(22 * 3, 22 * 6), workers = 8L,
                                at = at_, lag = lag_,
                                threshold = c(0.03, 0.05, 0.07))
RollingGpdFeatures = RollingGpdInit$get_rolling_features(OhlcvInstance)

# theft catch22 features
print("Calculate Catch22 features.")
RollingTheftInit = RollingTheft$new(windows = c(5, 22, 22 * 3, 22 * 12),
                                    workers = 8L, at = at_, lag = lag_,
                                    features_set = c("catch22", "feasts"))
RollingTheftCatch22Features = RollingTheftInit$get_rolling_features(OhlcvInstance)
gc()

# theft tsfeatures features
print("Calculate tsfeatures features.")
RollingTsfeaturesInit = RollingTsfeatures$new(windows = c(22 * 3, 22 * 6),
                                              workers = 8L, at = at_,
                                              lag = lag_, scale = TRUE)
RollingTsfeaturesFeatures = RollingTsfeaturesInit$get_rolling_features(OhlcvInstance)
gc()

# theft tsfel features, Must be alone, because number of workers have to be 1L
print("Calculate tsfel features.")
RollingTheftInit = RollingTheft$new(windows = c(22 * 3, 22 * 12), workers = 1L,
                                    at = at_, lag = lag_,  features_set = "tsfel")
RollingTheftTsfelFeatures = suppressMessages(RollingTheftInit$get_rolling_features(OhlcvInstance))
gc()

# quarks
RollingQuarksInit = RollingQuarks$new(windows = 22 * 6, workers = 6L, at = at_,
                                      lag = lag_, model = c("EWMA", "GARCH"),
                                      method = c("plain", "age"))
RollingQuarksFeatures = RollingQuarksInit$get_rolling_features(OhlcvInstance)
gc()

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
RollingWaveletArimaInstance = RollingWaveletArima$new(windows = 252, workers = 6L,
                                                      lag = lag_, at = at_, filter = "haar")
RollingWaveletArimaFeatures = RollingWaveletArimaInstance$get_rolling_features(OhlcvInstance)
gc()

# merge all features test
features_set <- Reduce(function(x, y) merge(x, y, by = c("symbol", "date"),
                                            all.x = TRUE, all.y = FALSE),
                       list(RollingBidAskFeatures,
                            RollingBackCusumFeatures,
                            # RollingExuberFeatures,
                            RollingForecatsFeatures,
                            # RollingGasFeatures,
                            # RollingGpdFeatures,
                            RollingTheftCatch22Features,
                            RollingTheftTsfelFeatures,
                            RollingTsfeaturesFeatures,
                            RollingQuarksFeatures,
                            # RollingTvgarchFeatures
                            RollingWaveletArimaFeatures
                       ))
features <- features_set[OhlcvFeaturesSetSample, on = c("symbol", "date"), roll = Inf]
head(OhlcvFeaturesSetSample[, 1:5])
head(features_set[, 1:5])
head(features[, 1:5])

# save ohlcv features and merged features
# time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
# fwrite(OhlcvFeaturesSet, paste0("D:/features/OhlcvFeatues-PEAD-", time_, ".csv"))
# fwrite(features, paste0("D:/features-PEAD-", time_, ".csv"))
# fwrite(RollingGasFeatures, paste0("D:/features-PEAD-GPD", time_, ".csv"))

# import features
# list.files("D:/features")
features_set <- fread("D:/features/features-PEAD-20230104090450.csv")
features_gpd <- fread("D:/features-PEAD-GPD20230116090322.csv")
features_set <- merge(features_set, features_gpd, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE)
# OhlcvFeaturesSetSample <- fread("D:/features/OhlcvFeatuesPEAD-20230104113949.csv")
# features <- fread("D:/features/features-PEAD-20221216220140.csv")

# merge OHLCV and events
features[, trading_date_after_event := date]
features <- features[dataset, on = c("symbol", "date"), roll = -Inf]
features[, .(symbol, date, trading_date_after_event)]

# remove missing values for trading_date_after_event
features <- features[!is.na(trading_date_after_event)]

# predictors from events data
features[, `:=`(
  nincr = frollsum(eps > epsEstimated, 4, na.rm = TRUE),
  nincr_half = frollsum(eps > epsEstimated, 2, na.rm = TRUE),
  nincr_2y = frollsum(eps > epsEstimated, 8, na.rm = TRUE),
  nincr_3y = frollsum(eps > epsEstimated, 12, na.rm = TRUE),
  eps_diff = (eps - epsEstimated + 0.00001) / (epsEstimated + 0.00001)
)]

# clean fundamentals
fundamentals <- fundamental_factors[date > as.Date("2009-01-01")]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
fundamentals <- unique(fundamentals, by = c("symbol", "acceptedDate"))

# merge features and fundamental data
features = fundamentals[features, on = c("symbol", "acceptedDate" = "date"), roll = Inf]
features[, .(symbol, acceptedDate, acceptedDateTime)]

# remove unnecesary columns
features[, `:=`(period.x = NULL, period.y = NULL, period = NULL, link = NULL,
                finalLink = NULL, reportedCurrency = NULL)]
features[symbol == "AAPL", .(symbol, fundamental_date, acceptedDate, acceptedDateFundamentals)]

# change date name
setnames(features, "acceptedDate", "date")

# convert char features to numeric features
char_cols <- features[, colnames(.SD), .SDcols = is.character]
char_cols <- setdiff(char_cols, c("symbol", "time", "right_time"))
features[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]

# import transcripts sentiments datadata
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- "us-east-1"
context_with_config <- tiledb_ctx(config)
arr <- tiledb_array("s3://equity-transcripts-sentiments",
                    as.data.frame = TRUE,
                    query_layout = "UNORDERED",
)
system.time(transcript_sentiments <- arr[])
tiledb_array_close(arr)
sentiments_dt <- as.data.table(transcript_sentiments)
setnames(sentiments_dt, "date", "time_transcript")
attr(sentiments_dt$time, "tz") <- "UTC"
sentiments_dt[, date := as.Date(time)]
sentiments_dt[, time := NULL]

# merge with features
features <- sentiments_dt[features, on = c("symbol", "date"), roll = -Inf]
features[, .(symbol, date, time_transcript, Not_FLS_positive)]

# remove observations where transcripts are more than 2 days away
features <- features[date - as.Date(time_transcript) >= -1]

# macro data
features <- macro[features, on = "date", roll = Inf]

# add macro data to features
macro_cols <- colnames(macro)[2:ncol(macro)]
features[, (macro_cols) := lapply(.SD, nafill, type = "locf"), .SDcols = macro_cols]

# checks
any(duplicated(features[, .(symbol, date)]))



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
                 "same_announce_time", "time_transcript", "i.time")
cols_non_features <- c("symbol", "date", "time", "right_time",
                       "ret_excess_stand_5", "ret_excess_stand_22", "ret_excess_stand_44", "ret_excess_stand_66",
                       colnames(features)[grep("aroundzero", colnames(features))],
                       colnames(features)[grep("extreme", colnames(features))],
                       colnames(features)[grep("bin_simple", colnames(features))],
                       colnames(features)[grep("bin_decile", colnames(features))],
                       "bmo_return", "amc_return",
                       "open", "high", "low", "close", "volume", "returns")
cols_features <- setdiff(colnames(features), c(cols_remove, cols_non_features))
head(cols_features, 10)
tail(cols_features, 500)
cols <- c(cols_non_features, cols_features)
features <- features[, .SD, .SDcols = cols]



# CLEAN DATA --------------------------------------------------------------
# convert columns to numeric. This is important only if we import existing features
clf_data <- copy(features)
chr_to_num_cols <- setdiff(colnames(clf_data[, .SD, .SDcols = is.character]), c("symbol", "time", "right_time"))
clf_data <- clf_data[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols]
int_to_num_cols <- colnames(clf_data[, .SD, .SDcols = is.integer])
clf_data <- clf_data[, (int_to_num_cols) := lapply(.SD, as.numeric), .SDcols = int_to_num_cols]
log_to_num_cols <- colnames(clf_data[, .SD, .SDcols = is.logical])
clf_data <- clf_data[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]

# remove duplicates
clf_data <- unique(clf_data, by = c("symbol", "date"))

# remove columns with many NA
keep_cols <- names(which(colMeans(!is.na(clf_data)) > 0.95))
print(paste0("Removing columns with many NA values: ", setdiff(colnames(clf_data), c(keep_cols, "right_time"))))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols <- names(which(colMeans(!is.infinite(as.data.frame(clf_data))) > 0.99))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(clf_data), keep_cols)))
clf_data <- clf_data[, .SD, .SDcols = keep_cols]

# remove inf values
n_0 <- nrow(clf_data)
clf_data <- clf_data[is.finite(rowSums(clf_data[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
n_1 <- nrow(clf_data)
print(paste0("Removing ", n_0 - n_1, " rows because of Inf values"))

# define feature columns
feature_cols <- colnames(clf_data)[colnames(clf_data) %in% cols_features]

# remove NA values
n_0 <- nrow(clf_data)
clf_data <- na.omit(clf_data, cols = feature_cols)
n_1 <- nrow(clf_data)
print(paste0("Removing ", n_0 - n_1, " rows because of NA values"))

# remove constant columns
# TODO: Move this to mlr3 pipeline !
features_ <- clf_data[, ..feature_cols]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# save features
fwrite(clf_data, "D:/features/pead-predictors.csv")
config <- tiledb_config()
config["vfs.s3.aws_access_key_id"] <- Sys.getenv("AWS-ACCESS-KEY")
config["vfs.s3.aws_secret_access_key"] <- Sys.getenv("AWS-SECRET-KEY")
config["vfs.s3.region"] <- Sys.getenv("AWS-REGION")
context_with_config <- tiledb_ctx(config)
fromDataFrame(
  obj = clf_data,
  uri = "s3://predictors-pead",
  col_index = c("symbol", "date"),
  sparse = TRUE,
  tile_domain=list(date=cbind(as.Date("1970-01-01"),
                              as.Date("2099-12-31"))),
  allows_dups = FALSE
)



# PREPROCESSING -----------------------------------------------------------
# TODO: add all this mlr4 pipeline
#  winsorization (remove outliers)
clf_data[, q_ := data.table::quarter(as.Date(date, origin = as.Date("1970-01-01")))]
clf_data[, (feature_cols) := lapply(.SD, Winsorize, probs = c(0.01, 0.99), na.rm = TRUE),
         .SDcols = feature_cols,
         by = q_]
clf_data[, c("q_")  := NULL]

# remove constant columns
# TODO: Move this to mlr3 pipeline !
features_ <- clf_data[, ..feature_cols]
remove_cols <- colnames(features_)[apply(features_, 2, var, na.rm=TRUE) == 0]
print(paste0("Removing feature with 0 standard deviation: ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# remove highly correlated features
# TODO move this to mlr3 pipe7line !
features_ <- clf_data[, ..feature_cols]
cor_matrix <- cor(features_)
cor_matrix_rm <- cor_matrix                  # Modify correlation matrix
cor_matrix_rm[upper.tri(cor_matrix_rm)] <- 0
diag(cor_matrix_rm) <- 0
remove_cols <- colnames(features_)[apply(cor_matrix_rm, 2, function(x) any(x > 0.99))]
print(paste0("Removing highly correlated featue (> 0.99): ", remove_cols))
feature_cols <- setdiff(feature_cols, remove_cols)

# remove missing values
n_0 <- nrow(clf_data)
clf_data <- na.omit(clf_data)
n_1 <- nrow(clf_data)
print(paste0("Removing ", n_0 - n_1, " rows because of NA values"))

# uniformisation of features
clf_data[, q_ := data.table::quarter(as.Date(date, origin = as.Date("1970-01-01")))]
clf_data[, (feature_cols) := lapply(.SD, function(x) ecdf(x)(x)),
         .SDcols = feature_cols,
         by = q_]
clf_data[, c("q_")  := NULL]
skimr::skim(clf_data$TSFEL_0_Absolute_energy_264)
skimr::skim(clf_data$autoarima_sd_504_Hi80)
skimr::skim(clf_data$eps_diff)
skimr::skim(clf_data$revenueGrowth)



# FEATURE SLECTION --------------------------------------------------------
# choose label
print(paste0("Choose among this features: ",
             colnames(clf_data)[grep("^ret_excess_stand", colnames(clf_data))]))
LABEL = "ret_excess_stand_22"

# TODO Move this to mlr3 pipeline!
# define feature matrix
cols_keep <- c(feature_cols, LABEL)
X <- clf_data[, ..cols_keep]

# winsorize LABEL
X[, (LABEL) := Winsorize(get(LABEL), probs = c(0.01, 0.99), na.rm = TRUE)]
skimr::skim(X[, get(LABEL)])
label_index <- which(colnames(X) == LABEL)

# gausscov requre matrix
X <- as.matrix(X)
y <- X[, label_index]
X <- X[, -label_index]

# data(abcq)
# dim(abcq)
# abcql<-flag(abcq,240,4,16)
# a <- f1st(abcql[[1]],abcql[[2]])

# f1st
############## ADD 1 !!!!!!!!!???????? ############
f1st_fi_ <- f1st(y, X, kmn = 10)
predictors_f1st <- colnames(X)[f1st_fi_[[1]][, 1]]

# f3st_1
f3st_1_ <- f3st(y, X, kmn = 10, m = 1)
predictors_f3st_1 <- unique(as.integer(f3st_1_[[1]][1, ]))[-1]
predictors_f3st_1 <- predictors_f3st_1[predictors_f3st_1 != 0]
predictors_f3st_1 <- colnames(X)[predictors_f3st_1]

# f3st_1 m=2
f3st_2_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 2)
cov_index_f3st_2 <- unique(as.integer(f3st_2_[[1]][1, ]))[-1]
cov_index_f3st_2 <- cov_index_f3st_2[cov_index_f3st_2 != 0]
cov_index_f3st_2 <- colnames(X[, -ncol(X)])[cov_index_f3st_2]

# # f3st_1 m=3
# f3st_3_ <- f3st(X[, ncol(X)], X[, -ncol(X)], kmn = 20, m = 3)
# cov_index_f3st_3_ <- unique(as.integer(f3st_3_[[1]][1, ]))[-1]
# cov_index_f3st_3 <- cov_index_f3st_3[cov_index_f3st_3 != 0]
# cov_index_f3st_3 <- colnames(X[, -ncol(X)])[cov_index_f3st_3]

# save important vars
ivars <- list(
  predictors_f1st = predictors_f1st,
  predictors_f3st_1 = predictors_f3st_1
  # f3st_2_ = f3st_2_
)
time_ <- format.POSIXct(Sys.time(), format = "%Y%m%d%H%M%S")
saveRDS(ivars, paste0("D:/mlfin/mlr3_models/PEAD-ivars-", time_, ".rds"))

# import important vars
# file.info(list.files("D:/mlfin/mlr3_models", full.names = TRUE, pattern = "PEAD"))
# ivars <- readRDS("D:/mlfin/mlr3_models/PEAD-ivars-20221219093210.rds")
# f1st_fi_ <- colnames(X[, -ncol(X)])[ivars$f1st_fi_[[1]][, 1]]
# cov_index_f3st_1 <- unique(as.integer(ivars$f3st_1_[[1]][1, ]))[-1]
# cov_index_f3st_1 <- cov_index_f3st_1[cov_index_f3st_1 != 0]
# cov_index_f3st_1 <- colnames(X[, -ncol(X)])[cov_index_f3st_1]
# cov_index_f3st_2 <- unique(as.integer(ivars$f3st_2_[[1]][1, ]))[-1]
# cov_index_f3st_2 <- cov_index_f3st_2[cov_index_f3st_2 != 0]
# cov_index_f3st_2 <- colnames(X[, -ncol(X)])[cov_index_f3st_2]

# interesection of all important vars
most_important_vars <- intersect(predictors_f1st, predictors_f3st_1)
important_vars <- unique(c(predictors_f1st, predictors_f3st_1))



# DEFINE TASKS ------------------------------------------------------------
# select only labels and features
labels <- colnames(clf_data)[grep(LABEL, colnames(clf_data))]
X_mlr3 <- clf_data[, .SD, .SDcols = c("symbol", "date", feature_cols, labels)]

# add groupid
X_mlr3[, monthid := paste0(data.table::year(as.Date(date, origin = "1970-01-01")),
                           data.table::month(as.Date(date, origin = "1970-01-01")))]
setorder(X_mlr3, date)

# task with aroundzero bins
task_aroundzero <- as_task_classif(X_mlr3[, .SD, .SDcols = !c("symbol","date", labels[!grepl("aroundzero", labels)])],
                                   id = "aroundzero",
                                   target = labels[grep("aroundzero", labels)])

# task with aroundzero bins
task_decile <- as_task_classif(X_mlr3[, .SD, .SDcols = !c("symbol","date", labels[!grepl("decile", labels)])],
                               id = "decile",
                               target = labels[grep("decile", labels)])

# bin simple
task_simple <- as_task_classif(X_mlr3[, .SD, .SDcols = !c("symbol","date", labels[!grepl("simple", labels)])],
                               id = "simple",
                               target = labels[grep("simple", labels)])

# task for regression
task_reg <- as_task_regr(X_mlr3[, .SD, .SDcols = !c("symbol","date", labels[!grepl("^ret_excess_stand", labels)])],
                         id = "reg", target = labels[grep("^ret_excess_stand", labels)])

# task with extreme bins
label_ <- labels[grep("extreme", labels)]
X_mlr3_ <- X_mlr3[get(label_) %in% c(-1, 1) & !is.na(get(label_))]
X_mlr3_[, (label_) := droplevels(X_mlr3_[, get(label_)])]
task_extreme <- as_task_classif(X_mlr3_[, .SD, .SDcols = !c("symbol","date", labels[!grepl("extreme", labels)])],
                                id = "extreme",
                                target = labels[grep("extreme", labels)],
                                positive = "1")

# create custom cv and validation set
create_validation_set <- function(task) {
  # add group role
  task_ <- task$clone()
  task_$set_col_roles("monthid", "group")
  groups = task_$groups

  # add validation set
  val_ind <- min(which(groups$group == "202111")):nrow(groups)
  task$set_row_roles(rows = val_ind, role = "holdout")
}
create_validation_set(task_aroundzero)
create_validation_set(task_decile)
create_validation_set(task_simple)
create_validation_set(task_reg)

#  Instantiate Resampling
custom = rsmp("custom")
task_ <- task_aroundzero$clone()
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
custom$instantiate(task_aroundzero, train_sets, test_sets)
# custom$train_set(1)
# custom$test_set(1)



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

