require(data.table)
require(dplyr)
require(ggplot2)
require(knitr)
require(stringi)
require(lubridate)
require(reshape2)
require(sqldf)
require(scales)
require(date)
require(doMC)
require(xgboost)
require(stringr)
require(lubridate)
require(zoo)
require(caret)
require(rpart)
require(randomForest)
require(nnet)
require(kknn)
require(glmnet)

setwd("/Users/smuthurangan/Desktop/MSPA/MSPA498/Project-Final/dataset")

## data: train and test ----
xtrain <- fread('air_visit_data.csv')
xtest <- fread('sample_submission.csv')
## reservations: air clean ----
reserve_air <- fread('air_reserve.csv')
## store info: air ----
xstore <- fread('air_store_info.csv',encoding = "UTF-8")
## date info ----
xdate <- fread('date_info.csv')
hpg_reserve <- fread("hpg_reserve.csv")
hpg_stores <- fread("hpg_store_info.csv")
store_mapping <- fread("store_id_relation.csv")
head(store_mapping)
holidays <- fread("date_info.csv")
####################End of load data##########################################################
# try to merge air and hpg reservation but need a primary key to merge it
# First need to understand relation between air and hpg store details from relation data
# The following query gives an estimated logitude and latitude.
# all_store_details <- sqldf ("
#                         select sm.air_store_id,sm.hpg_store_id,round(a.latitude,0) as air_latitude,
#                         round(a.longitude,0) as air_longitude,round(h.latitude,0) as hpg_latitude,round(h.longitude,0) as hpg_longitude,
#                         a.air_area_name,h.hpg_area_name
#                         from air_stores a,hpg_stores h,store_mapping sm
#                         where sm.air_store_id = a.air_store_id 
#                         and sm.hpg_store_id = h.hpg_store_id 
#                         ")
# all_store_details
# all_store_details$difflongitude <- all_store_details$air_longitude - all_store_details$hpg_longitude
# all_store_details$difflatitude <- all_store_details$air_latitude - all_store_details$hpg_latitude
# nrow(all_store_details[(all_store_details$difflongitude != 0 | all_store_details$difflatitude != 0),])
# Based on above analysis, logitude and latitude with round number gives an primary key to merge those data.
####################End of primary key##########################################################
# ----------------------------DATA CLEAN---------------------------------
# --- 1.train and test data clean ---
# align the columns (test has the store id and date concatenated)
xtest$air_store_id <- str_sub(xtest$id, 1,-12)
xtest$visit_date <- str_sub(xtest$id, -10)
xtest$id <- NULL

# format date 
xtrain$visit_date <- as.Date(xtrain$visit_date)
xtest$visit_date <- as.Date(xtest$visit_date)
# air_population$visit_date <- as.Date(air_population$visit_date)

# combine train and test 
xtrain <- rbind(xtrain, xtest)

## --- 2.reservations: air clean ----
# convert to datetime
reserve_air$visit_datetime <- parse_date_time(
  reserve_air$visit_datetime, 
  orders = '%Y-%m-%d H:M:S'
)
reserve_air$reserve_datetime <- parse_date_time(
  reserve_air$reserve_datetime, 
  orders = '%Y-%m-%d H:M:S'
)

# time ahead = visit_datetime - reserve_datetime
reserve_air$time_ahead <- 
  as.double(reserve_air$visit_datetime - reserve_air$reserve_datetime) / 3600

# round to day
reserve_air$visit_date <- as.Date(reserve_air$visit_datetime)
reserve_air$reserve_datetime <- as.Date(reserve_air$visit_datetime)

# aggregate to id x date combo
res_air_agg <-
  reserve_air[,.(
    air_res_visitors = sum(reserve_visitors),
    air_mean_time_ahead = round(mean(time_ahead), 2)
  ) ,
  by = list(air_store_id, visit_date)]


head(hpg_reserve)
hpg_reserve$visit_datetime <- parse_date_time(
  hpg_reserve$visit_datetime, 
  orders = '%Y-%m-%d H:M:S'
)
hpg_reserve$reserve_datetime <- parse_date_time(
  hpg_reserve$reserve_datetime, 
  orders = '%Y-%m-%d H:M:S'
)

# time ahead = visit_datetime - reserve_datetime
hpg_reserve$time_ahead <- 
  as.double(hpg_reserve$visit_datetime - hpg_reserve$reserve_datetime) / 3600

# round to day
hpg_reserve$visit_date <- as.Date(hpg_reserve$visit_datetime)
hpg_reserve$reserve_datetime <- as.Date(hpg_reserve$visit_datetime)

# aggregate to id x date combo
res_hpg_agg <-
  hpg_reserve[,.(
    hpg_res_visitors = sum(reserve_visitors),
    hpg_mean_time_ahead = round(mean(time_ahead), 2)
  ) ,
  by = list(hpg_store_id, visit_date)]

nrow(res_air_agg)
hpg_res_agg <-  sqldf("
                        select * from store_mapping sm,res_hpg_agg a where sm.hpg_store_id = a.hpg_store_id")

hpg_res_agg$hpg_store_id  <- hpg_res_agg$hpg_store_id <- NULL
nrow(hpg_res_agg)
res_air_agg <- rbind(res_air_agg,hpg_res_agg, fill=TRUE)

#res_air_agg[is.na(res_air_agg)] <- 0
head(res_air_agg)
res_air_agg$air_mean_time_ahead <- res_air_agg$air_mean_time_ahead + res_air_agg$hpg_mean_time_ahead
res_air_agg$air_res_visitors <- res_air_agg$air_res_visitors + res_air_agg$hpg_res_visitors
head(res_air_agg)

nrow(res_air_agg)
res_air_agg <-
  reserve_air[,.(
    air_res_visitors = sum(reserve_visitors),
    air_mean_time_ahead = round(mean(time_ahead), 2)
  ) ,
  by = list(air_store_id, visit_date)]
nrow(res_air_agg)


# combine train and test 

# set air_air_genre_name to factor 
xstore$air_genre_name <- factor(xstore$air_genre_name)
levels(xstore$air_genre_name) <- 1:nlevels(xstore$air_genre_name)

# set air_genre_name to integer
xstore$air_genre_name <- as.integer(xstore$air_genre_name)

# set air_genre_name to factor and then to interger
xstore$air_area_name <- factor(xstore$air_area_name)
levels(xstore$air_area_name) <- 1:nlevels(xstore$air_area_name)
xstore$air_area_name <- as.integer(xstore$air_area_name)

## --- 3.date info clean ---

# (1)holidays at weekends are not special, right?
wkend_holidays <- which(
  xdate$day_of_week %in% c("Saturday", "Sunday") & 
    xdate$holiday_flg ==1
)
xdate[wkend_holidays, 'holiday_flg' := 0]

# (2)add decreasing weights from now
xdate[, 'weight' := (.I/.N) ^ 7]
xdate$calendar_date <- as.Date(xdate$calendar_date)

# (3)add air_population data
# air_population <- distinct(air_population)
# xstore <- inner_join(xstore,air_population)

## --- 4.data aggregation ---
xtrain <- merge(xtrain, res_air_agg, all.x = T)
xtrain <- merge(xtrain, xstore, all.x = T, by = 'air_store_id' )
xtrain <- merge(xtrain, xdate, by.x = 'visit_date', by.y = 'calendar_date')

# add min、max、median variable
# {code}

# it decreases the importance of the further past data by applying 
# a weight to them
xtrain[, 'visitors':= log1p(visitors)]
xtrain[,.(visitors = weighted.mean(visitors, weight)), 
       by = c('air_store_id', 'day_of_week', 'holiday_flg')]


# --- 5.set na to 0 ---
xtrain[is.na(xtrain)] <- 0
xtrain <- xtrain[order(air_store_id, visit_date)]

# delete day_of_week、weight
xtrain[,day_of_week := NULL]
xtrain[,weight := NULL]
rm(res_air_agg, xstore, xdate)

## ----------------------------Feature Engineer---------------------------
# 1.holiday in the last 3 days
xtrain[, h3a := rollapply(
  holiday_flg,
  width = 3,
  FUN = function(s)
    sign(sum(s, na.rm = T)),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

# 2.visits totals for 14 days,21days,28days,35days
xtrain[, vis14 := rollapply(
  log1p(visitors),
  width = 39,
  FUN = function(s)
    sum(s, na.rm = T),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

xtrain[, vis21 := rollapply(
  log1p(visitors),
  width = 46,
  FUN = function(s)
    sum(s, na.rm = T),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

xtrain[, vis28 := rollapply(
  log1p(visitors),
  width = 60,
  FUN = function(s)
    sum(s, na.rm = T),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

xtrain[, vis35 := rollapply(
  log1p(visitors),
  width = 74,
  FUN = function(s)
    sum(s, na.rm = T),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

# aggregate vis14,vis21,vis28,vis35
xtrain[, vLag1 := round((vis21 - vis14) / 7, 2)]
xtrain[, vLag2 := round((vis28 - vis14) / 21, 2)]
xtrain[, vLag3 := round((vis35 - vis14) / 35, 2)]
xtrain[, vis14 := NULL, with = TRUE]
xtrain[, vis21 := NULL, with = TRUE]
xtrain[, vis28 := NULL, with = TRUE]
xtrain[, vis35 := NULL, with = TRUE]

# 3.reservations for 7days and so on (like visit data)
xtrain[, res7 := rollapply(
  log1p(air_res_visitors),
  width = 7,
  FUN = function(s)
    sum(s, na.rm = T),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

xtrain[,res14 := rollapply(
  log1p(air_res_visitors),
  width = 14,
  FUN = function(s)
    sum(s, na.rm = T),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

xtrain[, res21 := rollapply(
  log1p(air_res_visitors),
  width = 21,
  FUN = function(s)
    sum(s, na.rm = T),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

xtrain[, res28 := rollapply(
  log1p(air_res_visitors),
  width = 28,
  FUN = function(s)
    sum(s, na.rm = T),
  partial = TRUE,
  fill = 0,
  align = 'right'
),
by = c('air_store_id')]

# set factor variable to dummy variable
# xtrain$cluster <- factor(xtrain$cluster)
# levels(xtrain$cluster) <- 1:nlevels(xtrain$cluster)

# xtrain_dummy <- dummy.data.frame(xtrain[,c("air_genre_name","air_area_name","cluster")])
# xtrain <- data.table(
#   xtrain[,-c("air_genre_name","air_area_name","cluster")],
#   xtrain_dummy
# )

# separate train and test
x_train <- xtrain[visitors > 0]
x_test <- xtrain[visitors == 0]
head(x_train)
# rm(xtrain,xtest)

# ------------------------------BUILD MODEL-------------------------------
## xgboost - validation ----
x0 <- x_train[x_train$visit_date <= '2017-03-09' & x_train$visit_date > '2016-04-01',]
x1 <- x_train[x_train$visit_date > '2017-03-09',]

# y0 -> train vistors,y1 -> validate vistors
y0 <- x0$visitors
y1 <- x1$visitors
######Fit various models

x0$visit_date <- x0$air_store_id <- x0$visitors  <- NULL
x1$visit_date <- x1$air_store_id <- x1$visitors  <- NULL

d0 <- xgb.DMatrix(
  as.matrix(x0),
  label = y0
)

d1 <- xgb.DMatrix(
  as.matrix(x1),
  label = y1
)

# set parameters 
param <- list(
  "objective" = "reg:linear",
  "eta" = 0.1,
  "max_depth" = 10,
  "subsample" = 0.886,
  'min_child_weight' = 5,
  "colsample_bytree" = 0.886,
  "scale_pos_weight" = 10,
  "alpha" = 10,
  "gamma" = 30,
  "lambda" = 50,
  "silent" = 1,
  "nthread" = 10,
  "seed" = 20171205
)

nround = 500

# xgboost cross validation
bst.cv <- xgb.cv(
  params = param,
  data = d0,
  metrics = "rmse",
  nrounds = nround,
  nfold = 4,
  early_stopping_rounds = 5
)

# best nrounds
best_iteration <- bst.cv$best_iteration

bst <- xgboost(
  params = param,
  data = d0,
  metrics = "rmse",
  nrounds = best_iteration
)

# calcluate sd
pred_val <- predict(bst, as.matrix(x1))
print(paste('validation error:', round(sd(pred_val - y1), 4), sep = ' '))

# 0.595

# calcluate rmsle
val_rmsle <- sqrt(
  sum((log(pred_val+1)-log(y1+1))^2)/nrow(x1)
)
print(paste('validation rmsle error:', round(val_rmsle, 4),sep = ' '))
# 0.1795

## ---xgboost - full ----
x0 <- x_train
x1 <- x_test

y0 <- x0$visitors

x0$visit_date <- x0$air_store_id <- x0$visitors <- NULL
x1$visit_date <- x1$air_store_id <- x1$visitors <- NULL

d0 <- xgb.DMatrix(
  as.matrix(x0),
  label = y0
)

bst <- xgboost(
  params = param,
  data = d0,
  metrics = "rmse",
  nrounds = best_iteration
)

# predict 
pred_full <- predict(bst, as.matrix(x1))

# result submission
prx <- data.frame(
  id = paste(x_test$air_store_id, x_test$visit_date , sep = '_'),
  visitors = expm1(pred_full)
)

head(prx)

write.csv(
  prx,
  paste0("xgb_basic_",Sys.Date(),".csv"), 
  row.names = F, 
  quote = F
)