# Created by Bin Dai, Siyuan Guo, Mar 2014.

require(nnet)
for (run_num in 1:100) {
  # clear workspace except "run_num"
  rm(list=setdiff(ls(), "run_num"))
  # read and trim data
  data<-read.table("joinedtable_use_this_for_final_analysis.csv", stringsAsFactors=T, header=T)
  data$daterange2 <- relevel(data$daterange, ref = "pre-1839")
  data<- data[,4:28]
  
  # random sample 80% as training data, rest as testing data
  bound <- floor((nrow(data)/5)*4) 
  data <- data[sample(nrow(data)), ]    
  data.train <- data[1:bound, ]
  data.test <- data[(bound+1):nrow(data), ]
  
  # build logistic regression model
  test <- multinom(daterange2 ~., data = data.train)
  # make prediction
  result<-predict(test, newdata = data.test, "probs")
  # select item with highest likelihood as predicted outcome
  x <- apply(result,1,function(x){return(which(x==max(x)))})
  int2data<- data.frame(int = c(1:12),date = c("pre-1839","1840-1860","1861-1876","1877-1887","1888-1895","1896-1901","1902-1906","1907-1910","1911-1914","1915-1918","1919-1922","1923-present"))
  pred <- c()
  gs <- c()
  for (i in 1:length(x)){ 
    gs <- c(gs, toString(data.test[rownames(data.test)==strtoi(names(x)[i]),25]))
    pred <- c(pred, toString(int2data[int2data$int==x[[names(x)[i]]] ,2]))
  }
  
  # naive model prediction
  pred_naive <- c()
  for (i in 1:length(data.test[,1])){
    label <- colnames(data.test)[which(sapply(data.test[i,],toString)=="TRUE")]
    pred_naive <- c(pred_naive,substr(label,1,nchar(label)-4))
  }
  pred_naive <- gsub('[.]','-',pred_naive)
  pred_naive <- gsub('X','',pred_naive)
  
  outcome <- data.frame(gs,pred,pred_naive)
  fn <- paste("outcome_",run_num,".csv",sep='')
  write.csv(outcome, file=fn, row.names=FALSE)
}