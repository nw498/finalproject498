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

temp <- tra %>%
  group_by(air_store_id, dow) %>% 
  summarize(count=sum(is.na(visitors)))

colnames(temp)[3] <- "count_observations"
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
            head(weather_data)
            filterdata <- filter(dataset, dataset$air_store_id == unique_air_store_ids[[i]])
            merged <- left_join(filterdata,weather_data, by=c('visit_date'))
            dataset_with_weather[dataset_with_weather$air_store_id == unique_air_store_ids[[i]],]$precipitation <- merged$precipitation
            dataset_with_weather[dataset_with_weather$air_store_id == unique_air_store_ids[[i]],]$avg_temperature <- merged$avg_temperature
        }
        return(dataset_with_weather)
}


merge_data_frames <- rbind(prep_df,predict_data)
dim(merge_data_frames)
merge_data_frames <- add_weather(merge_data_frames)

merge_data_frames$longlat <- as.numeric(sum(merge_data_frames$latitude, merge_data_frames$longitude))
merge_data_frames$latitude <- NULL
merge_data_frames$longitude <- NULL

#merge_data_frames <- cbind(merge_data_frames, dummy(merge_data_frames$air_area_name, sep = "-", drop = TRUE))
#merge_data_frames <- cbind(merge_data_frames, dummy(merge_data_frames$air_genre_name, sep = "_", drop = TRUE))
merge_data_frames <- cbind(merge_data_frames, dummy(merge_data_frames$day_of_week, sep = "_", drop = TRUE))

prep_df <- merge_data_frames[merge_data_frames$visitors > 0,]
predict_data <- merge_data_frames[merge_data_frames$visitors == 0,]

prep_df$air_genre_name <- NULL
prep_df$air_area_name <- NULL
prep_df$day_of_week <- NULL

str(prep_df)
ncol(prep_df)

predict_data$air_genre_name <- NULL
predict_data$air_area_name <- NULL
predict_data$day_of_week <- NULL

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


prep_df[is.na(prep_df)] <- as.integer(0)
predict_data[is.na(predict_data)] <- as.integer(0)

ncol(predict_data)
ncol(prep_df)
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