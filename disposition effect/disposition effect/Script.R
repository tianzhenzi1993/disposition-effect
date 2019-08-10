#if (FALSE) {
library(data.table)
library(stringr)
setwd("C:\\Users\\RuXin\\Desktop\\R&Python\\data")
#对rb进行基本的数据清洗
rb <- load("r.cube.rb.mst.sp.1806.Rdata")
rb <- eval(parse(text = rb))
info <- load("r.cube.info.mst.1806.Rdata")
info <- eval(parse(text = info))
ret <- load("r.cube.ret.mst.sp.1806.Rdata")
ret <- eval(parse(text = ret))
stock <- load("r.user.stock.mst.1806.Rdata")
stock <- eval(parse(text = stock))
rb[, created.at := as.Date(created.at)]
rb[, stock.symbol := str_replace(stock.symbol, "(SZ)|(SH)", "")]
rb <- rb[, .(cube.symbol,stock.symbol, created.at, price, target.weight, prev.weight.adjusted)]
setkey(rb, stock.symbol, cube.symbol, created.at)
rb <- rb[!(.(0,0)), .SD, by = .(stock.symbol, cube.symbol, created.at), on = c("target.weight", "prev.weight.adjusted")]
rb <- rb[target.weight - prev.weight.adjusted != 0,]
rb <- rb[!.(0),on=c("price")]
rb <- rb[!is.na(prev.weight.adjusted),]
rb <- rb[!is.nan(prev.weight.adjusted),]
rb <- rb[!is.null(prev.weight.adjusted),]
rb <- rb[prev.weight.adjusted >= 0]
rb <- rb[target.weight >= 0]
a <- rb[, !duplicated(.SD, by = c("target.weight", "prev.weight.adjusted")), by = .(stock.symbol, cube.symbol, created.at)]
rb <- rb[a$V1,]
fwrite(rb,"clean_rb1")
#股票交易价格处理：dividends&splits,利用csmar中的“考虑现金红利再投资的收盘价的可比价格”
rb <- fread("clean_rb1.csv", colClasses = c(stock.symbol = "character"))
daily_return <- fread("daily_return.txt", colClasses = c(Stkcd = "character"))
daily_return <- daily_return[, .(Stkcd, Trddt, Clsprc, Adjprcwd)][order(Stkcd, Trddt)]
rb <- daily_return[rb, on = c("Stkcd" = "stock.symbol", "Trddt" = "created.at")]
rb <- rb[!is.na(Clsprc) & !is.na(Adjprcwd),]
setnames(rb, 1:2, c("stock.symbol", "created.at"))
rb[, Adjprcwd := as.double(Adjprcwd)];rb[, Clsprc := as.double(Clsprc)]
rb[, adju_price := Adjprcwd / Clsprc * price]
rb[,Adjprcwd := NULL];rb[,Clsprc := NULL]
fwrite(rb,"clean_rb.csv")

#追踪每只股票的建仓时刻
#将每只股票每个投资者的最早一条交易变为preve==0，前面的条目删掉
rb <- fread("clean_rb.csv", colClasses = c(stock.symbol = "character"))
SetupTrack <- function(DT) {
    len <- nrow(DT)
    vec <- vector(mode = "logical", length = len)
    if (nrow(DT[prev.weight.adjusted == 0, ]) == 0) {
        vec[] <- FALSE
        return(vec)
    } else if (DT[, prev.weight.adjusted][1] == 0) {
            vec[] <- TRUE
            return(vec)
        } else {
            for (i in 1:len) {
                if (DT[, prev.weight.adjusted][i] == 0) {
                    vec[1:i - 1] <- FALSE
                    vec[i:len] <- TRUE
                    break
                }
            }
            return(vec)
        }
}
bbb <- rb[, SetupTrack(.SD), by = .(stock.symbol, cube.symbol)]
total_positions <- rb[bbb$V1,]
total_positions[,created.at := as.Date(created.at)]
fwrite(total_positions, "total_posi1.csv")

#给所有的position编号，新的stock_investor或清仓操作标志新的position
#当天建仓就清仓的position予以删除
total_positions[, big_class := .GRP, by = .(stock.symbol, cube.symbol)]
total_positions[,if_liqui := c(FALSE)][target.weight==0,if_liqui:=c(TRUE)]
posi_number <- 1
index_num <- 1
num_of_posi <- vector(mode = "numeric",length = nrow(total_positions))
big_class_unique <- unique(total_positions$big_class)
setindex(total_positions,big_class)
for (i in 1:length(big_class_unique)) {
    id <- big_class_unique[i]
    judge_vec <- total_positions[.(id), if_liqui, on = c("big_class")]
    for (j in 1:length(judge_vec)) {
        num_of_posi[index_num] <- posi_number
        if (judge_vec[j] == TRUE) {posi_number <- posi_number + 1} 
        index_num <- index_num + 1
    }
    posi_number <- posi_number + 1
}
total_positions[, posi.id := num_of_posi][, big_class := NULL][, if_liqui := NULL]
total_positions[, created.at := as.Date(created.at)]
fwrite(total_positions, "total_posi2.csv")
#在一个posi.id里面，初始条目prev!=0的，整个posi删掉
bbb <- total_positions[, SetupTrack(.SD), by = posi.id]
total_positions <- total_positions[bbb$V1,]
fwrite(total_positions, "total_posi3.csv")
#在同一个position中，若存在不止一个建仓操作，则将整个posi删掉
total_positions <- fread("total_posi3.csv", colClasses = c(stock.symbol = "character"))
posi_select <- vector(mode = "logical", length = nrow(total_positions))
uni_posi.id <- unique(total_positions$posi.id)
index <- 1
for (i in 1:length(uni_posi.id)) {
    slice <- total_positions[posi.id == uni_posi.id[i],]
    len <- nrow(slice)  
    if (nrow(slice[prev.weight.adjusted == 0, ]) == 1) {
        posi_select[index:(index+len-1) ] <- TRUE
    } 
    index <- index + len
}
total_positions <- total_positions[as.logical(posi_select),]
fwrite(total_positions, "total_posi4.csv")

#计算每个条目的调整(adjust for splits and dividens)后加权平均建仓成本(weighted average price)
#设S为股票持有量，第x期资产Ax=pricex*Sx/targetx=pricex*Sx-1/prevex
#可得Sx=targetx/prevex*Sx-1;则x期股票购买量Sx-Sx-1=(targetx/prevex -1)Sx-1
#x期总购买成本（权重）为Sx*price
total_positions <- fread("total_posi4.csv", colClasses = c(stock.symbol = "character"))
total_positions <- total_positions[target.weight - prev.weight.adjusted != 0,]
AveWeiPrice <- function(DT) {
    len_DT <- nrow(DT)
    vec <- vector(mode = "numeric", length = nrow(DT))
    buy_DT <- DT[target.weight - prev.weight.adjusted > 0, ]
    buy_times <- nrow(buy_DT)
    if (buy_times == 1) {
        vec[] <- DT[,adju_price][1]
        return(vec)
    } else {
        target <- DT[, target.weight];preve <- DT[, prev.weight.adjusted]
        price_vec <- buy_DT[, price]; adju_price_vec <- buy_DT[, adju_price]
        rela_cost_vec <- vector(mode = "numeric", length = buy_times)
        S <- vector(mode = "numeric", length = buy_times)
        S[1] <- 1
        rela_cost_vec[] <- 0;rela_cost_vec[1] <- 1*price_vec[1]
        price_now <- adju_price_vec[1]
        vec[1] <- price_now
        j<-1
        for (i in 2:len_DT) {
            delta_weight <- target[i] - preve[i]
            if (delta_weight < 0) {
                vec[i] <- price_now
            } else {
                j <- j + 1
                S[j] <- (target[i] / preve[i] -1) * S[j-1]
                rela_cost_vec[j] <- S[j] * price_vec[j]
                price_now <- weighted.mean(adju_price_vec, rela_cost_vec)
                vec[i] <- price_now
            }
        }
    }
    return(vec)
}
aveweiprice_vec <- total_positions[, AveWeiPrice(.SD), by = posi.id]
total_positions[, adju_avewei_price := aveweiprice_vec$V1]
fwrite(total_positions, "total_posi5.csv")
#计算每个条目的return
#建仓时间赋给setup_date,计算hold_days
total_positions <- fread("total_posi5.csv", colClasses = c(stock.symbol = "character"))
total_positions[, return := (adju_price-adju_avewei_price)/adju_price]
total_positions[, setup_date := created.at[1], by = .(posi.id)]
total_positions[,setup_date:=as.Date(setup_date)][,created.at:=as.Date(created.at)]
total_positions[, hold_days := as.numeric(created.at - setup_date)]
total_positions <- total_positions[hold_days != 0,]

#计算Panel A of Table 1中的AnnouncementWindow Sample
announcement_date <- fread(file = "announcement_date.csv", colClasses = c(Stkcd = "character"))
announcement_date[, Actudt := as.Date(Actudt)]
bind1_announce <- announcement_date[, .(Stkcd, Actudt = Actudt - 1, announ.date = Actudt)]
bind2_announce <- announcement_date[, .(Stkcd, Actudt = Actudt + 1, announ.date = Actudt)]
announcement_date[,announ.date := Actudt]
l <- list(bind1_announce, announcement_date, bind2_announce)
announcement_date <- rbindlist(l)
rm(bind1_announce);rm(bind2_announce);rm(l)
setkey(announcement_date, Stkcd, Actudt);setindex(announcement_date,Stkcd)
total_positions <- fread("total_posi.csv")
symbol_unique <- unique(total_positions$stock.symbol)
positions_of_window <- data.table()
for (i in 1:length(symbol_unique)) {
    id <- symbol_unique[i]
    rb1 <- total_positions[.(id), on = "stock.symbol"]
    rb1 <- rb1[announcement_date[id,on="Stkcd"], on = c("created.at"="Actudt"), nomatch = FALSE]
    positions_of_window <- rbindlist(list(positions_of_window,rb1))
}
rm(rb1);rm(symbol_unique);positions_of_window[, Stkcd := NULL]
fwrite(positions_of_window, "wind_posi.csv")
sellposi_of_window <- positions_of_window[prev.weight.adjusted-target.weight>0,]

