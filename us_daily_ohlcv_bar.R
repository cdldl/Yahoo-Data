#rm(list=ls())
### DO NOT RUN UNDER WINDOWS ENVIRONMENT 

#PARAMETERS
START_DATE = "2010-01-01"

# DOWNLOAD / LOAD LIBRARIES 
list.of.packages <- c("data.table", "fasttime",'plyr',"PerformanceAnalytics",
                      'imputeTS',"parallel","doParallel","doMC",'lubridate',
                      'anytime','xts','TTR','missRanger','quantmod')
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)


# LOAD ENVIRONMENT
if(Sys.info()['sysname'] == "Windows" ) {
  library(doParallel)
  registerDoParallel(cores=detectCores())
} else {
  library(doMC)
  registerDoMC(cores=detectCores())
}

CORES = detectCores()

# GET TICKERS 
tickers = stockSymbols(exchange = c("AMEX","NASDAQ","NYSE"), sort.by = c("Exchange","Symbol"), quiet = FALSE) 
symbols = tickers$Symbol
fwrite(tickers,'tickers.csv',nThread =CORES)


# DOWNLOAD TICKERS
system.time({prices <- mclapply(symbols, function(x) tryCatch({
  getSymbols(x, src="yahoo", from = START_DATE,auto.assign=FALSE)
},
error=function(e) {
  NULL
}),mc.cores=CORES)})

# REMOVE EMPTY TICKERS
to_remove = sapply(prices,is.null)
whi = which(to_remove == TRUE)
if(length(whi) != 0) {
  data = prices[-whi]
} else {
  data = prices  
}

# CONCATENATE ALL STOCKS
grid.param <- expand.grid(1:length(data))
fe <- foreach(param = iter(grid.param, by = "row"), 
              .verbose = TRUE, .errorhandling = "pass",  
              .multicombine = TRUE, .maxcombine = max(2, nrow(grid.param)))
              #.export='
fe$args <- fe$args[1]
fe$argnames <- fe$argnames[1]


results <- fe %dopar% {
  tmp_data = as.data.table(cbind(data[[param[1]]][,c(1:6)]))
  name = strsplit(colnames(data[[param[1]]])[6],'[.]')[[1]][1]
  all_tmp = data.table(tmp_data,name)
  names(all_tmp) = c("index","Open","High","Low",'Close',"volume",'adjusted','symbol')
  all_tmp
}
data = do.call(rbind,results)
colnames(data)[1] = c('time')

# Get BOOK value
tickers$MarketCap = gsub('[$]',"",tickers$MarketCap)
tickers$ma = ifelse(substring(tickers$MarketCap,nchar(tickers$MarketCap))=="M",1000000,1000000000)
tickers$MarketCap = gsub('M|B',"",tickers$MarketCap)
tickers$MarketCap = as.numeric(tickers$MarketCap) *tickers$ma
tickers$ma = NULL
tickers$book = tickers$MarketCap / tickers$LastSale


# GET INDUSTRY / SECTOR / Market Cap
colnames(tickers)[1] = 'symbol'
all = merge(Data,tickers[,c('symbol','Sector','Industry','book')],by="symbol",allow.cartesian=TRUE)

all[,size:=Close*book]
all[,value:=book/Close]

fwrite(all,'us_prices_and_industry.csv',nThread =CORES)
