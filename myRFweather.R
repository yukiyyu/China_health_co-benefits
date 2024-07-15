### This studdy applied the based on the "rmweather" package from Grange et al. (2018):https://github.com/skgrange/rmweather
# Todo£ºchange workpath
setwd("/Users/huangyingjian/desktop/RFmodel/")
workingDirectory<<-"/Users/huangyingjian/desktop/RFmodel/"

library(normalweatherr)
library(rmweather) ### Random Forest/ rmweather from Grange Github
library (openair) ### For Theil-sen analysis
library(dplyr)
library(lubridate)
# library(xlsx)

cities <- read.csv('station.csv')
city_lst <-unique(cities['³ÇÊÐ'])
rownames(city_lst)<-1:nrow(city_lst)
###01. Import the data set which contains: date, PM2.5 and MET weathers
# data_PM2.5<-import("åŒ—äº¬PM25data.csv", date="date", date.format = "%Y-%m-%d")


my_RMWnormalize <- function(city){
  filename0 = ("2015_2019_city_mean.csv")
  PM2.5_mean <- import(filename0,date="date", date.format = "%Y/%m/%d")
  PM2.5_city <-data.frame(PM2.5_mean[,city],PM2.5_mean[,'date'])
  colnames(PM2.5_city)=c(city,'date')

  PM2.5_city<- add_date_variables(PM2.5_city)
  
  filename = paste0(city,"PM25data.csv")
  data_PM2.5<-import(filename, date="date", date.format = "%Y-%m-%d")
  data_PM2.5<-add_date_variables(data_PM2.5)
  # data_PM2.5$value<-data_PM2.5$pm
  # data preparation
  data_prepared_PM2.5 <- data_PM2.5 %>% 
    filter(!is.na("ws")) %>% 
    rename(value = "pm") %>% 
    rmw_prepare_data(na.rm = TRUE)
  
  #Variables in the data set: date, d, w, m,y
  #MET variable: temperature (air_temp), Relative Humidity (RH), wind speed (ws), wind direction (wd),air pressure(pres).
  set.seed(123)
  
  
  ###02.  Build RF model: 
  
  RF_PM2.5_model <- rmw_do_all(
    data_prepared_PM2.5,
    variables = c(
      "day_julian", "week","month","Y", "air_temp", "RH", "wd", "ws",
      "atmos_pres"
    ),
    n_trees = 200,
    n_samples = 300,
    verbose = FALSE
  )
  # performanceRF(data_PM2.5)
  ### 03. Predict the level of a pollutant in different weather condition
  
  # Initial MET data 
  MET_PM <- PM2.5_city
  MET_PM[is.na(MET_PM)] <- 0
  MET_2015_2019 <-data_PM2.5
  # Initial_nomarlised_data<-import("data_normalisation_initial_1000.csv", date="date", date.format = "%d/%m/%Y %H:%M")### A blank matrix, nrow= nrow(PM2.5_MET_2013_2017), ncol=1000
  nomarlised_prediction<- PM2.5_mean %>% select(1)
  # Use Parallel computing: 
  library(doParallel)
  registerDoParallel(cores = detectCores() - 1)
  #Predict the level of a pollutant in different weather condition
  Pollutant_prediction <-function (n){    ###n is the number of re-sample MET data set
    for (j in 1:n){
      for (i in 1:1826){           ### 365 is number of daily obserbvation in 2019                   
        ### "hour" variable is in the 5th column in the data set  
        # day_1 <-MET_PM[i,8] ### "day_julian" variable is in the 8th column in the dataset
        week_1<-MET_2015_2019[i,10] ### "week" variable is in the 3rd column in the data set
        ### Randomly sample weather data from 2015-2019 using + 2 weeks before & after
        if(week_1==1){
         MET_sample<- MET_2015_2019   %>% filter(week>=52|week <= 3)  %>% sample_frac ()}
        if(week_1==2){
          MET_sample<- MET_2015_2019   %>% filter(week>=53|week <= 4)  %>% sample_frac ()}
        if(week_1==52) {
          MET_sample<- MET_2015_2019   %>% filter(week>=50|week <= 1)  %>% sample_frac ()}
        if(week_1==53) {
          MET_sample<- MET_2015_2019  %>% filter(week>=51|week <= 2)  %>% sample_frac ()}
        if(week_1>2 & week_1<51){
         MET_sample<- MET_2015_2019 %>% filter(week>= week_1-2 & week <= week_1+2) %>% sample_frac ()}
        # if(day_1 <= 14){
        #   MET_sample<-MET_2015_2019 %>%  filter(day_julian >= 365 + day_1-14 |day_julian <= day_1 +14)  %>% sample_frac ()}
        # if(day_1 > 14 & day_1<352){
        #   MET_sample<-MET_2015_2019 %>% filter(day_julian >=day_1-14 & day_julian <= day_1+14)  %>% sample_frac ()}
        # if(day_1 >=352) {
        #   MET_sample<-MET_2015_2019 %>% filter(day_julian >= day_1-14|day_julian <= day_1 +14-365)  %>% sample_frac ()}
        
        ### Generate the new dataset of MET data in 2015 by 2015-2019  
        r<-sample(1:nrow(MET_sample), 1, replace=FALSE) ### Randomly select 1 row of MET
        MET_2015_2019[i,2:6]<-MET_sample[r,2:6]} # Generate the new data met for 2019 by 2019-2021
      MET_new_365 <- MET_2015_2019[1:365,]
      MET_new_365[,'pm'] <- MET_PM[,city]
      predict_PM2.5_level<- rmw_predict( ### RUN Random Forest model with new MET dataset
        RF_PM2.5_model$model, 
        df=rmw_prepare_data(MET_new_365,value = c("pm")))
      nomarlised_prediction <-cbind(nomarlised_prediction, predict_PM2.5_level)}
    nomarlised_prediction
  }
  
  
  
  # Use Parallel computing: 
  library(doParallel)
  registerDoParallel(cores = detectCores() - 1)
  
  ### Final_weather_normalised_PM2.5 by aggregating 1000 single predictions.
  # Pollutant_prediction (1000) ### random by 100 times
  nomarlised_prediction <- Pollutant_prediction (100)
  final_weather_nomarlised_PM2.5 <- apply(nomarlised_prediction[,2:101],1,mean, na.rm=TRUE) ### Mean value of 1000 predictions
  # final_weather_nomarlised_PM2.5['date'] <- normalised_prediction['date']
  f = data.frame(final_weather_nomarlised_PM2.5)
  f['date'] <- nomarlised_prediction['date']
  newname = paste0(city,"normal.csv")
  write.table(f,newname,row.names=FALSE,col.names=TRUE,sep=",")
}


### COMPARiSON obseved and nomarlised concentration. May correct with testing data.
# data_compare<-merge(data_PM2.5,f, by="date" ) 
# 
# data_compare <- data_compare %>% dplyr:: rename(Modeled.PM2.5=final,Observed.PM2.5=PM2.5) %>%
#   select(date,Observed.PM2.5,Modeled.PM2.5)
# 
# timePlot(data_compare,pollutant=c("Observed.PM2.5", "Modeled.PM2.5"), 
#          lwd=c(1,2), group=TRUE, lty=c(1,1),avg.time ="month",
#          key.position="top",cols=c("darkgreen","firebrick4"),
#          ylab=expression("PM2.5 concentration"* " (" * mu * "g m" ^-3 * ")"))
# 
# timePlot(data_compare)

### performance of random Forest algorithm test
performanceRF<-function(data_PM2.5){
  # data_PM2.5<-import("data_PM2.5_2013_2017.csv", date="date", date.format = "%d/%m/%Y %H:%M")
  # data_PM2.5<-data_PM2_5_2013_2017
  # data_PM2.5<-selectByDate(data_PM2.5, start="1/1/2021", end="31/12/2021")
  # data_PM2.5<-add_date_variables(data_PM2.5)
  data_PM2.5$value<-data_PM2.5$pm
  list_input_data <- split_input_data(data_PM2.5, fraction=0.7)
  variables <- c("wd","ws", "air_temp","RH","atmos_pres","date_unix","day_julian", "weekday","week","Y","month")
  set.seed(123)
  model_rf_PM2.5 <- calculate_model(   ###### BUILD THE MODEL
    list_input_data, 
    variables = variables, 
    mtry = 4,
    nodesize = 3,
    ntree=200,
    model = "rf")
  model_rf_PM2.5$model                     ##### CHECK THE MODEL PERFORMANCE FOR TRAINING 
  
  testing<-list_input_data$testing
  testing_model<- normalise_for_meteorology(      ##### RUN MODEL WITH THE testing data set
    model_rf_PM2.5$model, 
    testing, 
    variables = setdiff(variables,variables),n = 1)
  testing$predictions<-testing_model$value_predict
  testing$observations<-testing$value
  
  # scatterPlot(testing, x = "observations", y = "predictions", col= "jet" ,method = "density") ### scatter plot
  # timePlot(testing, pollutant=c("observations","predictions"), group=TRUE) ### time series plot
  modStats(testing, mod = "predictions", obs = "observations", statistic = c("n", "FAC2",
                                                                             "MB", "MGE", "NMB", "NMGE", "RMSE", "r", "COE", "IOA"),
           type = "default", rank.name = NULL)
  
  fit_PM2.5 <- lm(predictions ~ observations, data = testing) ### linear regression between predictions and observations
  summary(fit_PM2.5)  ### for slop and intercept
  
}


# city_lst<-city_lst[1,1]
for (i in 1:nrow(city_lst)){
  city = city_lst[i,1]
  my_RMWnormalize(city)
  print(paste(city,"done"))
}
### use deweather package to remove weather impact on pm2.5
