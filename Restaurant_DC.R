#Restaurant Visits Forecasting

ls()
rm(list=ls())
setwd("/Users/decolvin/Box Sync/1 Northwestern MSPA/PREDICT 498_DL Capstone/Final Project/")

#################################################################################################
#Load data and summary
air.reserve <- read.csv("Data/air_reserve.csv", header=T, blank.lines.skip=T)
hpg.reserve <- read.csv("Data/hpg_reserve.csv", header=T, blank.lines.skip=T)
air.store <- read.csv("Data/air_store_info.csv", header=T, blank.lines.skip=T)
hpg.store <- read.csv("Data/hpg_store_info.csv", header=T, blank.lines.skip=T)
store.id <- read.csv("Data/store_id_relation.csv", header=T, blank.lines.skip=T)
air.visits <- read.csv("Data/air_visit_data.csv", header=T, blank.lines.skip=T)

dim(air.reserve)
dim(hpg.reserve)
dim(air.store)
dim(hpg.store)
dim(store.id)
dim(air.visits)

summary(air.reserve)
summary(air.store)
summary(air.visits)
summary(hpg.reserve)
summary(hpg.store)
summary(store.id)

#################################################################################################
#Merge data
store.id$common_id <- rep(1,nrow(store.id))

air <- merge(air.reserve, store.id[,c(1,3)], by="air_store_id", all = T)
air <- merge(air, air.store, by = "air_store_id")
#air <- merge(air, air.visits, by = "air_store_id")
dim(air)
colnames(air) <- c("store_id","visit_datetime","reserve_datetime",
                   "reserve_visitors","common_id", "genre_name","area_name",
                   "latitude","longitude")

hpg <- merge(hpg.reserve, store.id[,c(2,3)], by="hpg_store_id", all = T)
hpg <- merge(hpg, hpg.store, by = "hpg_store_id")
dim(hpg)
colnames(hpg) <- c("store_id","visit_datetime","reserve_datetime",
                   "reserve_visitors","common_id","genre_name","area_name",
                   "latitude","longitude")

mydata <- rbind(air,hpg)
summary(mydata)

#################################################################################################
#Format data types
str(mydata)
mydata$visit_datetime <- strptime(mydata$visit_datetime,"%Y-%m-%d %X")
mydata$reserve_datetime <- strptime(mydata$reserve_datetime,"%Y-%m-%d %X")
str(mydata)

mydata2 <- mydata[order(mydata$visit_datetime,decreasing=F), ]

#################################################################################################
#Replace and Remove NA
sum(is.na(mydata2$reserve_visitors))

mydata2$common_id[is.na(mydata2$common_id)] <- 0 # 1 if duplicate
mydata2 <- mydata2[! is.na(mydata2$visit_datetime), ] # 21 NA
dim(mydata2)
summary(mydata2)

#################################################################################################
#Add variables
mydata2$diff <- (mydata2$visit_datetime - mydata2$reserve_datetime)

#################################################################################################
#Identify outliers
plot(mydata2$reserve_visitors, type='l')
boxplot(mydata2$reserve_visitors, notch = T, col = "light blue")

#################################################################################################
#Plot on Map
##Simple Map
#library(rworldmap)
#newmap <- getMap(resolution = "high")
#plot(newmap, xlim = c(130,145), ylim = c(30,45), asp = 1,
#     main="Restaurant Location")
#points(unique(mydata2$longitude), unique(mydata2$latitude), col = 'red', pch=16)

##Google Map
#library(ggmap)
#map <- get_map(location = 'Japan', zoom=5, maptype = "roadmap")
#mapPoints <- ggmap(map) +
#  geom_point(aes(x=longitude, y=latitude),
#             data=mydata2, col="red", pch=16, cex=1)
#mapPoints

#################################################################################################
#No repeat restaurants

mydata3 <- mydata2[mydata2$common_id==0,]
plot(mydata3$reserve_visitors, type='l')

#################################################################################################
#Plots
plot(mydata3$visit_datetime,mydata3$reserve_visitors, type='l', col='blue3')
plot(mydata3$reserve_datetime,mydata3$reserve_visitors, type='l', col='blue4')
hist(mydata3$reserve_visitors)
hist(log(mydata3$reserve_visitors))
hist(BoxCox(mydata3$reserve_visitors, lambda=BoxCox.lambda(mydata3$reserve_visitors)))

many.visitors <- mydata3[mydata3$reserve_visitors>50,]
less.visitors <- mydata3[mydata3$reserve_visitors<50,]
plot(many.visitors$visit_datetime,
     many.visitors$reserve_visitors, type='l')
plot(less.visitors$visit_datetime,
     less.visitors$reserve_visitors, type='l')

sort(table(mydata3$store_id), decreasing = T)[1:10]
sort(table(mydata3$genre_name), decreasing = T)[1:10]
sort(table(mydata3$area_name), decreasing = T)[1:10]

#################################################################################################
#################################################################################################
#################################################################################################
#Aggregate to daily
mydata3$visit_day <- as.Date(mydata3$visit_datetime)
mydata.day <- aggregate(reserve_visitors ~ visit_day,data=mydata3, FUN="sum")
plot(mydata.day$visit_day ,mydata.day$reserve_visitors, type='l',
     main="Daily Visitors", ylab="Visitors", xlab="")

tail(mydata.day)

mydata.day[mydata.day$reserve_visitors > 50000,]

#################################################################################################
#################################################################################################
#################################################################################################
#Time series analysis
library(forecast)
mydata.ts <- ts(mydata.day$reserve_visitors,
                frequency=findfrequency(mydata.day$reserve_visitors))

tsdisplay(mydata.ts)
tsdisplay(diff(mydata.ts))

set.seed(1)
train <- sample(length(mydata.ts), length(mydata.ts)*0.8, replace = F)
valid <- -train


             