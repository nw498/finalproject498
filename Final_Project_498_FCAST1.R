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
nrow(xtrain)
xtest <- fread('sample_submission.csv')
## reservations: air clean ----
reserve_air <- fread('air_reserve.csv')
head(reserve_air)
## store info: air ----
xstore <- fread('air_store_info.csv',encoding = "UTF-8")
xstore$air_area_name <- stri_trans_general(xstore$air_area_name,"Latin-ASCII")
## date info ----
xdate <- fread('date_info.csv')
xtrain <- hpg_reserve
max(hpg_reserve$visit_datetime)
hpg_stores <- fread("hpg_store_info.csv")
hpg_stores$hpg_area_name <- stri_trans_general(hpg_stores$hpg_area_name,"Latin-ASCII")
store_mapping <- fread("store_id_relation.csv")
head(store_mapping)
holidays <- fread("date_info.csv")
####################End of load data##########################################################
# try to merge air and hpg reservation but need a primary key to merge it
# First need to understand relation between air and hpg store details from relation data
# The following query gives an estimated logitude and latitude.
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Shanghai food'] = 'Asian'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Spain/Mediterranean cuisine'] = 'Other'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Taiwanese/Hong Kong cuisine'] = 'Asian'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Udon/Soba'] = 'Japanese food'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Dim Sum/Dumplings'] = 'Japanese food' #'Asian'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Sweets'] = 'Cafe/Sweets'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Sichuan food'] = 'Asian'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Cantonese food'] = 'Asian'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Amusement bar'] = 'Other'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Thai/Vietnamese food'] = 'Asian'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Western food'] = 'Western food'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Bar/Cocktail'] = 'Bar/Cocktail'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Pasta/Pizza'] = 'Western food' #'Other'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Sushi'] = 'Japanese food'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Cafe'] = 'Cafe/Sweets'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Bistro'] = 'Dining bar'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Steak/Hamburger/Curry'] = 'Other' #???
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'French'] = 'Italian/French'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Korean cuisine'] = 'Yakiniku/Korean food'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Party'] = 'Karaoke/Party'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Okonomiyaki/Monja/Teppanyaki'] = 'Okonomiyaki/Monja/Teppanyaki'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Shabu-shabu/Sukiyaki'] = 'Other' #???
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Creative Japanese food'] = 'Creative cuisine'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Karaoke'] = 'Karaoke/Party'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Japanese cuisine/Kaiseki'] = 'Japanese food'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Japanese food in general'] = 'Japanese food'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Chinese general'] = 'Asian'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Spain Bar/Italian Bar'] = 'Dining bar'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Italian'] = 'Italian/French'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Grilled meat'] = 'Yakiniku/Korean food'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Seafood'] = 'Japanese food' #'Other'???
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Creation'] = 'Creative cuisine'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'International cuisine'] = 'International cuisine'
hpg_stores$hpg_genre_name_coded[hpg_stores$hpg_genre_name == 'Japanese style'] = 'Izakaya'#'Japanese food'???

# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Japanese style'] <- 'Izakaya'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Party'] <- 'Karaoke/Party'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Creation'] <- 'Izakaya'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Japanese food in general'] <- 'Japanese food'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Italian'] <- 'Italian/French'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Seafood'] <- 'Izakaya'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Spain Bar/Italian Bar'] <- 'Dining Bar'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Creative Japanese food'] <- 'Izakaya'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Steak/Hamburger/Curry'] <- 'Western food'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Spain Bar/Italian Bar'] <- 'Dining Bar'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Karaoke'] <- 'Bar/Cocktail'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Japanese cuisine/Kaiseki'] <- 'Japanese food'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Grilled meat'] <- 'Yakiniku/Korean food'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Amusement bar'] <- 'Bar/Cocktail'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'International cuisine'] <- 'Bar/Cocktail'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Cafe'] <- 'Bar/Cocktail'
# hpg_stores$hpg_genre_name[hpg_stores$hpg_genre_name == 'Karaoke'] <- 'Bar/Cocktail'

xstore$air_area_name <- gsub("\\s", ".", xstore$air_area_name)
xstore$air_area_name <- substr(xstore$air_area_name, 1, regexpr("\\.[^\\.]*", xstore$air_area_name))
head(xstore$air_area_name)

head(hpg_stores$hpg_area_name)
hpg_stores$hpg_area_name <- gsub("\\s", ".", hpg_stores$hpg_area_name)
hpg_stores$hpg_area_name <- substr(hpg_stores$hpg_area_name, 1, regexpr("\\.[^\\.]*", hpg_stores$hpg_area_name))
head(hpg_stores$hpg_area_name)

combine_stores_final <-  sqldf ("
                                select x.air_store_id, h.hpg_store_id, x.air_area_name from xstore x, hpg_stores h
                                where x.air_area_name = h.hpg_area_name
                                ")

nrow(combine_stores_final)
head(combine_stores_final, n = 100)

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
nrow(xtrain)

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

# aggregate to id x date combo
res_air_agg <-
  reserve_air[,.(
    air_res_visitors = sum(reserve_visitors),
    air_mean_time_ahead = round(mean(time_ahead), 2)
  ) ,
  by = list(air_store_id, visit_date)]

comp_agg <- sqldf("
                  select csf.air_store_id,rha.hpg_res_visitors,rha.visit_date from combine_stores_final csf, res_hpg_agg rha 
where csf.hpg_store_id = rha.hpg_store_id group by csf.air_store_id,rha.visit_date 
                  ")
nrow(comp_agg)

# hpg_res_agg <-  sqldf("
#                         select * from store_mapping sm,res_hpg_agg a where sm.hpg_store_id = a.hpg_store_id")
# 
# hpg_res_agg$hpg_store_id <- NULL
# nrow(hpg_res_agg)
# res_air_agg <- rbind(res_air_agg,hpg_res_agg, fill=TRUE)

#res_air_agg[is.na(res_air_agg)] <- 0
# head(res_air_agg)
# res_air_agg$air_mean_time_ahead <- res_air_agg$air_mean_time_ahead + res_air_agg$hpg_mean_time_ahead
# head(res_air_agg)
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
# {code}
## --- 4.data aggregation ---
xtrain <- merge(xtrain, res_air_agg, all.x = T,by = c('air_store_id','visit_date') )
xtrain <- merge(xtrain, comp_agg, all.x = T, by = c('air_store_id','visit_date') )
xtrain <- merge(xtrain, xstore, all.x = T, by = 'air_store_id' )
xtrain <- merge(xtrain, xdate, by.x = 'visit_date', by.y = 'calendar_date')

# it decreases the importance of the further past data by applying 
# a weight to them
xtrain[, 'visitors':= log1p(visitors)]
xtrain[,.(visitors = weighted.mean(visitors, weight)), 
       by = c('air_store_id', 'day_of_week', 'holiday_flg')]


# --- 5.set na to 0 ---
nrow(xtrain)
nrow(xtrain)
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
nrow(x_test)
# rm(xtrain,xtest)

# ------------------------------BUILD MODEL-------------------------------
## xgboost - validation ----
x0 <- x_train[x_train$visit_date <= '2017-03-09' & x_train$visit_date > '2016-04-01',]
x1 <- x_train[x_train$visit_date > '2017-03-09',]
nrow(x1)
# y0 -> train vistors,y1 -> validate vistors
y0 <- x0$visitors
y1 <- x1$visitors
######Fit various models

x0$visit_date <- x0$air_store_id <- x0$visitors <- NULL
x1$visit_date <- x1$air_store_id <- x1$visitors <- NULL

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

x0$visit_date <- x0$air_store_id <- x0$visitors   <- NULL
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
str(pred_full)
str(expm1(pred_full))
# result submission
prx <- data.frame(
  id = paste(xtest$air_store_id, xtest$visit_date , sep = '_'),
  visitors = expm1(pred_full)
)

head(prx)

write.csv(
  prx,
  paste0("xgb_basic_",Sys.Date(),".csv"), 
  row.names = F, 
  quote = F
)
####End of XGBoost##########################
#Start of traditional timeseries
require(forecast)
require(doParallel)
# Basic Timeseries model combo - take time ~ 30 mins to run
xts_train <- fread('air_visit_data.csv')
train_ts <- ts(xts_train[, 2:dim(xts_train)[2]], frequency = 7) 
fcst_intv = 39  ### 39 days of forecast horizon
fcst_matrix <- matrix(NA,nrow=4*nrow(train_ts),ncol=fcst_intv)
nrow(train_ts)

### forecasting - register cores for parallel processing
cl <- makeCluster(3)
registerDoParallel(cl)
fcst_matrix <- foreach(i=1:nrow(train_ts),.combine=rbind, .packages=c("forecast")) %dopar% { 
  print('checking')
  fcst_ets <- forecast(ets(train_ts[i,]),h=fcst_intv)$mean
  fcst_nnet <- forecast(nnetar(train_ts[i,]),h=fcst_intv)$mean
  fcst_arima <- forecast(auto.arima(train_ts[i,]),h=fcst_intv)$mean
  fcst_ses <- forecast(HoltWinters(train_ts[i,], beta=FALSE, gamma=FALSE),h=fcst_intv)$mean
  fcst_matrix <- rbind(fcst_ets, fcst_nnet, fcst_arima, fcst_ses)
}

### post-processing the forecast table
fcst_matrix_mix <- aggregate(fcst_matrix,list(rep(1:(nrow(fcst_matrix)/4),each=4)),mean)[-1]
fcst_matrix_mix[fcst_matrix_mix < 0] <- 0
colnames(fcst_matrix_mix) <- as.character(
  seq(from = as.Date("2017-04-23"), to = as.Date("2017-05-31"), by = 'day'))
fcst_df <- as.data.frame(cbind(train_wide[, 1], fcst_matrix_mix)) 
colnames(fcst_df)[1] <- "air_store_id"

### melt the forecast data frame from wide to long format for final submission
fcst_df_long <- melt(
  fcst_df, id = 'air_store_id', variable.name = "fcst_date", value.name = 'visitors')
fcst_df_long$air_store_id <- as.character(fcst_df_long$air_store_id)
fcst_df_long$fcst_date <- as.Date(parse_date_time(fcst_df_long$fcst_date,'%y-%m-%d'))
fcst_df_long$visitors <- as.numeric(fcst_df_long$visitors)

### get & process the sample submission file
sample_sub <- fread("../input/sample_submission.csv")
sample_sub$visitors <- NULL
sample_sub$store_id <- substr(sample_sub$id, 1, 20)
sample_sub$visit_date <- substr(sample_sub$id, 22, 31)
sample_sub$visit_date <- as.Date(parse_date_time(sample_sub$visit_date,'%y-%m-%d'))

### generate the final submission file
submission <- left_join(
  sample_sub, fcst_df_long, c("store_id" = "air_store_id", 'visit_date' = 'fcst_date'))
submission$visitors[is.na(submission$visitors)] <- 0
final_sub <- select(submission, c('id', 'visitors'))
write.csv(final_sub, "sub_ts_mix.csv", row.names = FALSE)
##########End of timeseries##################################
####Start of H2O deep learning take time ~ 45 mins
require(dplyr)
require(DescTools)
require(h2o)
require(stringr)

# separate train and test
h2o.init(nthreads = 5, enable_assertions = FALSE, max_mem_size = "16g")
train <- x_train
test <- x_test
head(train)

# Convert to H2OFrame objects
train_h2o <- as.h2o(train)

#valid_h2o <- as.h2o(valid)
#valid_h2o$visitors <- log1p(valid_h2o$visitors)
test_h2o  <- as.h2o(test)

str(train)
names(train)

# Set names for h2o
y <- "visitors"
x <- setdiff(names(train_h2o), y)

automl_models_h2o <- h2o.automl(
  x = x, 
  y = y, 
  training_frame = train_h2o, 
  #validation_frame = valid_h2o, 
  leaderboard_frame = test_h2o, 
  max_runtime_secs = 3300, 
  stopping_metric = "AUTO")

# Extract leader model
automl_leader <- automl_models_h2o@leader

pred <- as.data.frame(h2o.predict(automl_leader, newdata = test_h2o))
str(pred)
str(expm1(pred))

results <- data.frame(id = paste(xtest$air_store_id, xtest$visit_date , sep = '_') , visitors = expm1(pred))
results <- results %>% rename(visitors = predict)
fwrite(results, file = 'sub_automl_recruit.csv')

h2o.performance(automl_leader, newdata = test_h2o)
as.data.frame(automl_models_h2o@leaderboard)
automl_leader

###########End of H2O############################
#Multi regressor models
#combine multiple regressor and add mean visitors similar to traditional forecast block
require(gbm)
require(kknn)
#GradientBoostingRegressor
x0 <- x_train[x_train$visit_date <= '2017-03-09' & x_train$visit_date > '2016-04-01',]
x1 <- x_train[x_train$visit_date > '2017-03-09',]

# y0 -> train vistors,y1 -> validate vistors
y0 <- x0$visitors
y1 <- x1$visitors
######Fit various models

x0$visit_date <- x0$air_store_id  <- NULL
x1$visit_date <- x1$air_store_id <- NULL
x_train$visit_date <- x_train$air_store_id <- NULL

gbm.boost=gbm(visitors ~ . ,data = x0, distribution = "gaussian",
              n.trees = 1000,
              shrinkage = 0.01, 
              interaction.depth = 4,
              verbose = T)
gbm.boost
summary(gbm.boost)

pred_val <- predict(gbm.boost,x1,n.trees = 1000)
print(paste('validation error:', round(sd(pred_val - y1), 4), sep = ' '))


# calcluate rmsle
val_rmsle <- sqrt(
  sum((log(pred_val+1)-log(y1+1))^2)/nrow(x1)
)
print(paste('validation rmsle error:', round(val_rmsle, 4),sep = ' '))

model <- train.kknn(visitors ~ . ,data = x0, kmax = 4, verbose = T)
model

pred_val <- predict(model,x1,kmax = 4)
print(paste('validation error:', round(sd(pred_val - y1), 4), sep = ' '))


# calcluate rmsle
val_rmsle <- sqrt(
  sum((log(pred_val+1)-log(y1+1))^2)/nrow(x1)
)
print(paste('validation rmsle error:', round(val_rmsle, 4),sep = ' '))

model_full_gbm <- gbm(visitors ~ . ,data = x_train, distribution = "gaussian",
                      n.trees = 1000,
                      shrinkage = 0.01, 
                      interaction.depth = 4,
                      verbose = T)

pred_full_gbm <- predict(model_full_gbm,x_test,n.trees = 1000)

model_full_kknn <- train.kknn(visitors ~ . ,data = x_train, kmax = 4, verbose = T)
pred_full_kknn <- predict(model_full_kknn,x_test,kmax = 4)

# result submission
prx <- data.frame(
  id = paste(x_test$air_store_id, x_test$visit_date , sep = '_'),
  visitors = expm1(0.4 * pred_full + 0.3 * pred_full_gbm + 0.3 * pred_full_kknn)
)

head(prx)

write.csv(
  prx,
  paste0("xgb_basic_",Sys.Date(),".csv"), 
  row.names = F, 
  quote = F
)
## lgbm - validation ----

x0 <- xtrain[visit_date <= '2017-03-09' & visit_date > '2016-04-01']
x1 <- xtrain[visit_date > '2017-03-09']
y0 <- log1p(x0$visitors)
y1 <- log1p(x1$visitors)

mx1 <- as.integer(max(x0$visit_date) -min(x0$visit_date) )
mx2 <- as.integer(x0$visit_date -min(x0$visit_date))


x0$visit_date <- x0$air_store_id <- x0$visitors <- NULL
x1$visit_date <- x1$air_store_id <- x1$visitors <- NULL

cat_features <- c('air_genre_name', 'air_area_name')
d0 <- lgb.Dataset(as.matrix(x0), label = y0, 
                  categorical_feature = cat_features, 
                  free_raw_data = TRUE)
d1 <- lgb.Dataset(as.matrix(x1), label = y1, 
                  categorical_feature = cat_features, 
                  free_raw_data = TRUE)

# x0$wgt <- ((1 + mx2)/(1  + mx1))^5

params <- list(objective = 'regression', metric = 'mse', max_depth = 7,  
               feature_fraction = 0.7,  
               bagging_fraction = 0.8, 
               min_data_in_leaf = 30,
               learning_rate = 0.02, 
               num_threads = 4,
               weight = 'wgt')

ntrx <- 1000
valids <- list(valid = d1)
model <- lgb.train(params = params,  data = d0, valids = valids, nrounds = ntrx,
                   early_stopping_rounds = 10)

pred_val <- predict(model, as.matrix(x1))
print( paste('validation error:', round(sd(pred_val - y1),4), sep = ' ' ))

# 0.5869

ntrx <- model$best_iter

## lgbm - full ----

x0 <- xtrain
x1 <- xtest
y0 <- log1p(x0$visitors)

x0$visit_date <- x0$air_store_id <- x0$visitors <- NULL
x1$visit_date <- x1$air_store_id <- x1$visitors <- NULL

cat_features <- c('air_genre_name', 'air_area_name')
d0 <- lgb.Dataset(as.matrix(x0), label = y0, 
                  categorical_feature = cat_features, 
                  free_raw_data = FALSE)

params <- list(objective = 'regression', metric = 'mse', max_depth = 7,  
               feature_fraction = 0.7,  
               bagging_fraction = 0.8, 
               min_data_in_leaf = 30,
               learning_rate = 0.02, 
               num_threads = 4, 
               weight = 'wgt')

model <- lgb.train(params = params,  data = d0, nrounds = ntrx)

pred_full <- predict(model, as.matrix(x1))

prx <- data.frame(id = paste(xtest$air_store_id, xtest$visit_date , sep = '_')  ,
                  visitors = expm1(pred_full))
write.csv(prx, 'xgb_3011.csv', row.names = F, quote = F)