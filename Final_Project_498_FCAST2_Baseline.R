require(data.table)
require(stringr)
require(lubridate)
require(zoo)
require(lightgbm)
require(caroline)
require(dummies)
require(CatEncoders)
require(knitr)
require(xgboost)
require(scorer)
require(dplyr)
require(h2o)
require(stringr)
require(sqldf)
require(stringi)
require(caret)
require(dplyr)
require(car)
require(resample)
require(flexclust)

setwd("C:\\Kaggle\\fcast")
tra <- fread('air_visit_data.csv')
tes <- fread('sample_submission.csv')
ar <- fread('air_reserve.csv')
ast <- fread('air_store_info.csv',encoding = "UTF-8")
hr <- fread("hpg_reserve.csv")
hol <- fread('date_info.csv')
colnames(hol)[1] <- "visit_date"
hol[, dayof:=wday(as.Date(visit_date))]
hol[, holiday_flg:=ifelse(dayof %in% c(1,7), 1, holiday_flg)]
hol$dayof <- NULL
head(hol)
hs <- fread("hpg_store_info.csv")
id <- fread("store_id_relation.csv")

# align the columns (test has the store id and date concatenated)
tes$air_store_id <- str_sub(tes$id, 1,-12)
tes$visit_date <- str_sub(tes$id, -10)
tes$id <- NULL

hr <- inner_join(hr,id, by="hpg_store_id")
ar <- merge(ar,hr)
nrow(tra)

tra$visit_datetime <- date(tra$visit_date)
tra$dow <- weekdays(tra$visit_datetime)
ar$res_visit_datetime <- date(ar$visit_datetime)
ar$reserve_datetime   <- date(ar$reserve_datetime)
ar$visit_date <- date(ar$res_visit_datetime)
ar$reserve_diff = as.integer(ar$res_visit_datetime-ar$reserve_datetime)
ar$visit_datetime <- NULL
ar$reserve_datetime <- NULL
ar$res_visit_datetime <- NULL
ar$visit_date <- format(ar$visit_date, "%y-%m-%d")

tes$visit_datetime <- date(tes$visit_date)
tes$dow <- weekdays(tes$visit_datetime)

combine_store_weight_air <- sqldf(" select xt.visitors, xs.air_area_name,xs.air_genre_name from ast xs, tra xt where
                              xt.air_store_id = xs.air_store_id group by xt.air_store_id,xs.air_area_name,xs.air_genre_name")
nrow(combine_store_weight_air)
ast$rank <- NA
ast$rank[order(combine_store_weight_air$visitors)] <- nrow(combine_store_weight_air):1
head(ast)

combine_store_weight_hpg <- sqldf(" select xt.reserve_visitors, xs.hpg_area_name,xs.hpg_genre_name from hs xs, hr xt where
                              xt.hpg_store_id = xs.hpg_store_id group by xt.hpg_store_id,xs.hpg_area_name,xs.hpg_genre_name")
head(combine_store_weight_hpg)
hs$hrank <- NA
hs$hrank[order(combine_store_weight_hpg$reserve_visitors)] <- nrow(combine_store_weight_hpg):1

prep_df = left_join(tra,ar, by=c('air_store_id', 'visit_date'))
prep_df = inner_join(prep_df,ast, by=c('air_store_id'))
prep_df = left_join(prep_df,hol, by=c('visit_date'))
nrow(prep_df)

predict_data = left_join(tes,ar, by=c('air_store_id', 'visit_date'))
predict_data = inner_join(predict_data,ast, by=c('air_store_id'))
predict_data = left_join(predict_data,hol, by=c('visit_date'))
nrow(predict_data)

temp <- tra %>%
  group_by(air_store_id, dow) %>% 
  summarise_at(c("visitors"), min, na.rm = TRUE)

colnames(temp)[3] <- "min_visitors"
prep_df <- left_join(prep_df,temp,by=c('air_store_id', 'dow'))
predict_data <- left_join(predict_data, temp, by = c('air_store_id', 'dow'))

temp <- tra %>%
  group_by(air_store_id, dow) %>% 
  summarise_at(c("visitors"), mean, na.rm = TRUE)

colnames(temp)[3] <- "mean_visitors"
prep_df <- left_join(prep_df,temp,by=c('air_store_id', 'dow'))
predict_data <- left_join(predict_data, temp, by = c('air_store_id', 'dow'))

temp <- tra %>%
  group_by(air_store_id, dow) %>% 
  summarise_at(c("visitors"), median, na.rm = TRUE)

colnames(temp)[3] <- "median_visitors"
prep_df <- left_join(prep_df,temp,by=c('air_store_id', 'dow'))
predict_data <- left_join(predict_data, temp, by = c('air_store_id', 'dow'))

temp <- tra %>%
  group_by(air_store_id, dow) %>% 
  summarise_at(c("visitors"), max, na.rm = TRUE)

colnames(temp)[3] <- "max_visitors"
prep_df <- left_join(prep_df,temp,by=c('air_store_id', 'dow'))
predict_data <- left_join(predict_data, temp, by = c('air_store_id', 'dow'))

prep_df$dow <- NULL
predict_data$dow <- NULL
str(prep_df)
str(predict_data)

prep_df$month = format(prep_df$visit_datetime,"%m")
prep_df$day   = format(prep_df$visit_datetime,"%d")
prep_df$visit_datetime <- NULL

predict_data$month = format(predict_data$visit_datetime,"%m")
predict_data$day   = format(predict_data$visit_datetime,"%d")
predict_data$visit_datetime <- NULL

prep_df[is.na(prep_df)] <- as.integer(0)
predict_data[is.na(predict_data)] <- as.integer(0)


add_weather <- function(dataset) { 
        print('Adding weather...')                                                                                                                
        air_nearest <- fread(                                                                                                                
        'air_store_info_with_nearest_active_station.csv') 
        unique_air_store_ids <- unique(dataset$air_store_id) 
        weather_dir <- 'C:\\Kaggle\\fcast\\1-1-16_5-31-17_Weather\\'                                                                            
        weather_keep_columns <- c('precipitation', 'high_temperature')                                                                                                                                   
        dataset_with_weather <- NULL
        dataset_with_weather <- dataset
        dataset_with_weather$precipitation <- NA
        dataset_with_weather$avg_temperature <- NA
        head(dataset_with_weather)
        
        nrownum <- nrow(ast)
        for (i in 1:nrownum) {                                                                                                       
            station = air_nearest[which(air_nearest$air_store_id == unique_air_store_ids[[i]])]$station_id                                                          
            weather_data = fread(paste(weather_dir, station, '.csv',sep="")) 
            weather_data <- as.data.frame(weather_data)
            weather_data$visit_date_str <- strptime(weather_data$calendar_date, "%Y-%m-%d")
            colnames(weather_data)[1] <- "visit_date"
            weather_data$visit_date_str <- NULL
            filterdata <- filter(dataset, dataset$air_store_id == unique_air_store_ids[[i]])
            merged <- left_join(filterdata,weather_data, by=c('visit_date'))
            dataset_with_weather[dataset_with_weather$air_store_id == unique_air_store_ids[[i]],]$precipitation <- merged$precipitation
            dataset_with_weather[dataset_with_weather$air_store_id == unique_air_store_ids[[i]],]$avg_temperature <- merged$avg_temperature
        }
        return(dataset_with_weather)
}


# merge_data_frames <- rbind(prep_df,predict_data)
# dim(merge_data_frames)
# merge_data_frames <- add_weather(merge_data_frames)
# 
# merge_data_frames$longlat <- as.numeric(sum(merge_data_frames$latitude, merge_data_frames$longitude))
# merge_data_frames$latitude <- NULL
# merge_data_frames$longitude <- NULL
# merge_data_frames$hpg_store_id <- NULL

#merge_data_frames <- cbind(merge_data_frames, dummy(merge_data_frames$air_area_name, sep = "-", drop = TRUE))
#merge_data_frames <- cbind(merge_data_frames, dummy(merge_data_frames$air_genre_name, sep = "_", drop = TRUE))
#merge_data_frames <- cbind(merge_data_frames, dummy(merge_data_frames$day_of_week, sep = "_", drop = TRUE))

# prep_df <- merge_data_frames[merge_data_frames$visitors > 0,]
# predict_data <- merge_data_frames[merge_data_frames$visitors == 0,]

str(prep_df)
ncol(prep_df)

str(predict_data)
ncol(predict_data)

'%!in%' <- function(x,y)!('%in%'(x,y))
names(prep_df)
names(predict_data)
colnamesnotintest <- which(!(names(prep_df) %in% names(predict_data)))
head(colnamesnotintest)

prep_df$visitors <- log1p(prep_df$visitors)

lenc <- LabelEncoder.fit(prep_df$air_store_id)
prep_df$air_store_id <- transform(lenc,prep_df$air_store_id)
prep_cols <- colnames( prep_df )

lenc <- LabelEncoder.fit(predict_data$air_store_id)
predict_data$air_store_id <- transform(lenc,predict_data$air_store_id)

lenc <- LabelEncoder.fit(prep_df$air_area_name)
prep_df$air_area_name <- transform(lenc,prep_df$air_area_name)

lenc <- LabelEncoder.fit(predict_data$air_area_name)
predict_data$air_area_name <- transform(lenc,predict_data$air_area_name)

lenc <- LabelEncoder.fit(prep_df$air_genre_name)
prep_df$air_genre_name <- transform(lenc,prep_df$air_genre_name)

lenc <- LabelEncoder.fit(predict_data$air_genre_name)
predict_data$air_genre_name <- transform(lenc,predict_data$air_genre_name)

lenc <- LabelEncoder.fit(prep_df$day_of_week)
prep_df$day_of_week <- transform(lenc,prep_df$day_of_week)

lenc <- LabelEncoder.fit(predict_data$day_of_week)
predict_data$day_of_week <- transform(lenc,predict_data$day_of_week)


prep_df[is.na(prep_df)] <- as.integer(0)
predict_data[is.na(predict_data)] <- as.integer(0)

head(prep_df)

############################################################################################

## xgboost - validation ----
x0 <- prep_df[prep_df$visit_date <= '2017-03-09' & prep_df$visit_date > '2016-04-01',]
x1 <- prep_df[prep_df$visit_date > '2017-03-09',]

# y0 -> train vistors,y1 -> validate vistors
y0 <- x0$visitors
y1 <- x1$visitors
######Fit various models

x0$visit_date  <- x0$visitors <- NULL
x1$visit_date  <- x1$visitors <- NULL

x0[1:ncol(x0)]<- lapply(x0[1:ncol(x0)], as.numeric)
x1[1:ncol(x1)]<- lapply(x1[1:ncol(x1)], as.numeric)
str(x0)

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


x10 <- prep_df
x11 <- predict_data

x10[1:ncol(x10)]<- lapply(x10[1:ncol(x10)], as.numeric)
x11[1:ncol(x11)]<- lapply(x11[1:ncol(x11)], as.numeric)

y10 <- x10$visitors

x10$visit_date  <- x10$visitors <- NULL
x11$visit_date  <- x11$visitors <- NULL

d0 <- xgb.DMatrix(
  as.matrix(x10),
  label = y10
)

bst <- xgboost(
  params = param,
  data = d0,
  metrics = "rmse",
  nrounds = best_iteration
)

# predict 
pred_full <- predict(bst, as.matrix(x11))
str(pred_full)
str(expm1(pred_full))
# result submission
prx <- data.frame(
  id = paste(tes$air_store_id, tes$visit_date , sep = '_'),
  visitors = expm1(pred_full)
)

head(prx)

write.csv(
  prx,
  paste0("xgb_basic_",Sys.Date(),".csv"), 
  row.names = F, 
  quote = F
)

##########################################

####Start of H2O deep learning take time ~ 45 mins

# separate train and test
h2o.init(nthreads = -1, enable_assertions = FALSE, max_mem_size = "16g")
train <- prep_df
test <- predict_data
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

results <- data.frame(id = paste(tes$air_store_id, tes$visit_date , sep = '_') , visitors = expm1(pred))
results <- results %>% rename(visitors = predict)
fwrite(results, file = 'sub_automl_recruit.csv')

h2o.performance(automl_leader, newdata = test_h2o)
as.data.frame(automl_models_h2o@leaderboard)
automl_leader


##################################################
train <- prep_df[prep_df$visit_date <= '2017-03-09' & prep_df$visit_date > '2016-04-01',]
test <- prep_df[prep_df$visit_date > '2017-03-09',]

train$id <- 1:nrow(train)
train[1:ncol(train)]<- lapply(train[1:ncol(train)], as.numeric)

test$id <- 1:nrow(test)
test[1:ncol(test)]<- lapply(test[1:ncol(test)], as.numeric)
head(test)

train$visit_date  <-  train$reserve_diff <- train$longlat   <- NULL
test$visit_date  <-  test$reserve_diff <- test$longlat   <- NULL

str(train)
str(test)

limitedTrain <- train
limitedTrain$visitors <- NULL
limitedTest <- test
limitedTest$visitors <- NULL

preproc <- preProcess(limitedTrain)
normTrain <- predict(preproc, limitedTrain)
normTest <- predict(preproc, limitedTest)
summary(normTest)

set.seed(144)
km <- kmeans(normTrain, centers = 12)
str(km)
km

km.kcca <- as.kcca(km, normTrain)
clusterTrain <- predict(km.kcca)
clusterTest <- predict(km.kcca, newdata = normTest)

# train1 <- train[train$visit_date >= as.Date('2016-01-01','%Y-%m-%d') & train$visit_date <= as.Date('2016-04-28','%Y-%m-%d'),]
# train2 <- train[train$visit_date >= as.Date('2016-04-29','%Y-%m-%d') & train$visit_date <= as.Date('2016-05-31','%Y-%m-%d'),]
# train3 <- train[train$visit_date >= as.Date('2016-06-01','%Y-%m-%d') & train$visit_date <= as.Date('2017-04-22','%Y-%m-%d'),]

train1 <- subset(train, clusterTrain == 1)
train2 <- subset(train, clusterTrain == 2)
train3 <- subset(train, clusterTrain == 3)
train4 <- subset(train, clusterTrain == 4)
train5 <- subset(train, clusterTrain == 5)
train6 <- subset(train, clusterTrain == 6)
train7 <- subset(train, clusterTrain == 7)
train8 <- subset(train, clusterTrain == 8)
train9 <- subset(train, clusterTrain == 9)
train10 <- subset(train, clusterTrain == 10)
train11 <- subset(train, clusterTrain == 11)
train12 <- subset(train, clusterTrain == 12)

test1 <- subset(test, clusterTest == 1)
test2 <- subset(test, clusterTest == 2)
test3 <- subset(test, clusterTest == 3)
test4 <- subset(test, clusterTest == 4)
test5 <- subset(test, clusterTest == 5)
test6 <- subset(test, clusterTest == 6)
test7 <- subset(test, clusterTest == 7)
test8 <- subset(test, clusterTest == 8)
test9 <- subset(test, clusterTest == 9)
test10 <- subset(test, clusterTest == 10)
test11 <- subset(test, clusterTest == 11)
test12 <- subset(test, clusterTest == 12)

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

xgboost_train <- function( train, label) {
  train$visitors <- NULL
  d0 <- xgb.DMatrix(
    as.matrix(train),
    label = label
  )
  
  bst <- xgboost(
    params = param,
    data = d0,
    metrics = "rmse",
    nrounds = 120
  )
  return(bst)
}

model1 <- xgboost_train(train1, train1$visitors)
model2 <- xgboost_train(train2, train2$visitors)
model3 <- xgboost_train(train3, train3$visitors)
model4 <- xgboost_train(train4, train4$visitors)
model5 <- xgboost_train(train5, train5$visitors)
model6 <- xgboost_train(train6, train6$visitors)
model7 <- xgboost_train(train7, train7$visitors)
model8 <- xgboost_train(train8, train8$visitors)
model9 <- xgboost_train(train9, train9$visitors)
model10 <- xgboost_train(train10, train10$visitors)
model11 <- xgboost_train(train11, train11$visitors)
model12 <- xgboost_train(train12, train12$visitors)

xgboost_test <- function( model, testin, testlabel) {
  
  d1 <- xgb.DMatrix(
    as.matrix(testin),
    label = testlabel
  )
  testin$visitors <- NULL
  pred_val <- predict(model, as.matrix(testin))
  return (pred_val)
}


test1ids <- test1$id
nrow(test1ids)
test1$id <- NULL
predictTest1 <- xgboost_test(model1, test1, test1$visitors)
test2ids <- test2$id
test2$id <- NULL
predictTest2 <- xgboost_test(model2, test2, test2$visitors)
test3ids <- test3$id
test3$id <- NULL
predictTest3 <- xgboost_test(model3, test3, test3$visitors)
test4ids <- test4$id
test4$id <- NULL
predictTest4 <- xgboost_test(model4, test4, test4$visitors)
test5ids <- test5$id
test5$id <- NULL
predictTest5 <- xgboost_test(model5, test5, test5$visitors)
test6ids <- test6$id
test6$id <- NULL
predictTest6 <- xgboost_test(model6, test6, test6$visitors)
test7ids <- test7$id
test7$id <- NULL
if(nrow(test7) > 0) {
  predictTest7 <- xgboost_test(model7, test7, test7$visitors)
}
test8ids <- test8$id
test8$id <- NULL
predictTest8 <- xgboost_test(model8, test8, test8$visitors)
test9ids <- test9$id
test9$id <- NULL
predictTest9 <- xgboost_test(model9, test9, test9$visitors)
test10ids <- test10$id
test10$id <- NULL
if(nrow(test10) > 0) {
  predictTest10 <- xgboost_test(model10, test10, test10$visitors)
}

test11ids <- test11$id
test11$id <- NULL
predictTest11 <- xgboost_test(model11, test11, test11$visitors)
test12ids <- test12$id
test12$id <- NULL
predictTest12 <- xgboost_test(model12, test12, test12$visitors)

allPredictions <- c(predictTest1, predictTest2,predictTest3, predictTest4,predictTest5, predictTest6, predictTest7, predictTest8,predictTest9,predictTest11, predictTest12)
d <- data.frame(allPredictions)
d$allPredictions <- expm1(d$allPredictions)
allIndex <- c(test1ids, test2ids,test3ids, test4ids, test5ids, test6ids,test7ids, test8ids,test9ids, test10ids,test11ids, test12ids)
datalist <- as.data.frame(nrow=31591)
for(i in 1:length(allIndex)) {
  datalist$data <- rbind(datalist,d$allPredictions[allIndex[[i]]])
}
head(datalist)
print(paste('validation error:', round(sd(big_data - test$visitors), 4), sep = ' '))

prx <- data.frame(
  id = paste(tes$air_store_id, tes$visit_date , sep = '_'),
  visitors = (big_data)
)

head(prx, n = 100)

write.csv(
  prx,
  paste0("xgb_basic_",Sys.Date(),".csv"), 
  row.names = F, 
  quote = F
)
