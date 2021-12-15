library(odbc)
library(DBI)
library(tidyverse)
library(dbplyr)
library(readxl)
library(lubridate)
library(janitor)
library(openxlsx)
library(httr)
library(jsonlite)
library(data.table)
library(stringr)


as400 <- dbConnect(odbc::odbc(), "Sunpac", uid = "DATAP015",
                   pwd = "TannerP513")

renameFunction <- function(SYSIBMtable, SYSIBMschema){

  
  AssetNames <- tbl(as400, sql(str_interp("select distinct *
    from SYSIBM.SQLCOLUMNS where Table_NAME = '${SYSIBMtable}' and 
    Table_SCHEM = '${SYSIBMschema}'")))
  GetAssetNames <- as.data.frame(AssetNames, row.names = NULL, optional = FALSE) %>%
    select(COLUMN_NAME, COLUMN_TEXT)
  
  
  
  AssetSpread <- data.frame(as.list(GetAssetNames$COLUMN_TEXT))
  
  
  
  
  ASSETS <- tbl(as400, in_schema(SYSIBMschema, SYSIBMtable))
  ASSETS_df <- as.data.frame(ASSETS, row.names = NULL, optional = FALSE) 
  
  
  
  Assets <- ASSETS_df %>%
    set_names(colnames(AssetSpread))
  
  columnNames <- sub("X\\.", "", names(Assets))
  setnames(Assets, columnNames)
  
  columnNames <- sub("\\.$", "",trimws(names(Assets)))
  setnames(Assets, columnNames)
  
  return(Assets)
  
  
}

SummaryBalanceReport <- function(TestFile){
  Data <- TestFile %>%
    select(ACCOUNT.NUMBER,oldLocation, Dep,
           Segment1, Segment2, Segment3, Segment4, Segment5, Segment6,
           Segment7, Segment8, Segment9, `Entered Debit Amount`, 
           `Entered Credit Amount`)
  
  Fund <- Data %>%
    group_by(Segment1) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    ungroup() %>%
    select(Segment1, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  Purpose <- Data %>%
    group_by(Segment2) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Segment2, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  PRC <- Data %>%
    group_by(Segment3) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Segment3, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  Object <- Data %>%
    group_by(Segment4) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Segment4, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  Location <- Data %>%
    group_by(Segment5) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Segment5, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  CostCenter <- Data %>%
    group_by(Segment6) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Segment6, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  Project <- Data %>%
    group_by(Segment7) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Segment7, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  LocalUse <- Data %>%
    group_by(Segment8) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Segment8, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  InterFund <- Data %>%
    group_by(Segment9) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Segment9, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  OldLocation <- Data %>%
    group_by(oldLocation) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(oldLocation, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  OldDep <- Data %>%
    group_by(Dep) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(Dep, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  AccountNumber <- Data %>%
    group_by(ACCOUNT.NUMBER) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(ACCOUNT.NUMBER, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  OldFund <- Data %>%
    mutate(OldFund = substring(ACCOUNT.NUMBER, 1, 1)) %>%
    group_by(OldFund) %>%
    mutate(DebitSum = sum(`Entered Debit Amount`)) %>%
    mutate(CreditSum = sum(`Entered Credit Amount`)) %>%
    select(OldFund, DebitSum, CreditSum) %>%
    distinct() %>%
    mutate(CreditSum = as.character(CreditSum)) %>%
    mutate(DebitSum = as.character(DebitSum)) %>%
    mutate(CreditSum = as.numeric(CreditSum)) %>%
    mutate(DebitSum = as.numeric(DebitSum)) %>%
    mutate(DebitDiff = DebitSum-CreditSum) %>%
    mutate(CreditDiff = CreditSum-DebitSum)
  
  OUT <- createWorkbook()
  # Add some sheets to the workbook
  addWorksheet(OUT, "Data")
  RedStyle <- createStyle(fontColour = "#000000", fgFill = "yellow")
  addWorksheet(OUT, "Fund", tabColour = MatchingCD(Fund))
  addWorksheet(OUT, "Purpose", tabColour = MatchingCD(Purpose))
  addWorksheet(OUT, "Object", tabColour = MatchingCD(Object))
  addWorksheet(OUT, "Location", tabColour = MatchingCD(Location))
  addWorksheet(OUT, "Cost Center", tabColour = MatchingCD(CostCenter))
  addWorksheet(OUT, "Project", tabColour = MatchingCD(Project))
  addWorksheet(OUT, "LocalUse", tabColour = MatchingCD(LocalUse))
  addWorksheet(OUT, "Inter Fund", tabColour = MatchingCD(InterFund))
  addWorksheet(OUT, "OldLocation", tabColour = MatchingCD(OldLocation))
  addWorksheet(OUT, "OldDep", tabColour = MatchingCD(OldDep))
  addWorksheet(OUT, "AccountNumber", tabColour = MatchingCD(AccountNumber))
  addWorksheet(OUT, "OldFund", tabColour = MatchingCD(OldFund))
  
  # Write the data to the sheets
  writeData(OUT, sheet = "Data", x = Data)
  writeData(OUT, sheet = "Fund", x = Fund)
  writeData(OUT, sheet = "Purpose", x = Purpose)
  writeData(OUT, sheet = "Object", x = Object)
  writeData(OUT, sheet = "Location", x = Location)
  writeData(OUT, sheet = "Cost Center", x = CostCenter)
  writeData(OUT, sheet = "Project", x = Project)
  writeData(OUT, sheet = "LocalUse", x = LocalUse)
  writeData(OUT, sheet = "Inter Fund", x = InterFund)
  writeData(OUT, sheet = "OldLocation", x = OldLocation)
  writeData(OUT, sheet = "OldDep", x = OldDep)
  writeData(OUT, sheet = "AccountNumber", x = AccountNumber)
  writeData(OUT, sheet= "OldFund", x = OldFund)
  addStyle(OUT, "Fund", cols = 1:ncol(Fund), rows = 
           data.frame(which(Fund$DebitSum != Fund$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "Purpose", cols = 1:ncol(Purpose), rows = 
             data.frame(which(Purpose$DebitSum != Purpose$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "Object", cols = 1:ncol(Object), rows = 
             data.frame(which(Object$DebitSum != Object$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "Location", cols = 1:ncol(Location), rows = 
             data.frame(which(Location$DebitSum != Location$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "Cost Center", cols = 1:ncol(CostCenter), rows = 
             data.frame(which(CostCenter$DebitSum != CostCenter$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "Project", cols = 1:ncol(Project), rows = 
             data.frame(which(Project$DebitSum != Project$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "LocalUse", cols = 1:ncol(LocalUse), rows = 
             data.frame(which(LocalUse$DebitSum != LocalUse$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "Inter Fund", cols = 1:ncol(InterFund), rows = 
             data.frame(which(InterFund$DebitSum != InterFund$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "OldLocation", cols = 1:ncol(OldLocation), rows = 
             data.frame(which(OldLocation$DebitSum != OldLocation$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "OldDep", cols = 1:ncol(OldDep), rows = 
             data.frame(which(OldDep$DebitSum != OldDep$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "AccountNumber", cols = 1:ncol(AccountNumber), rows = 
             data.frame(which(AccountNumber$DebitSum != AccountNumber$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  addStyle(OUT, "OldFund", cols = 1:ncol(OldFund), rows = 
             data.frame(which(OldFund$DebitSum != OldFund$CreditSum, arr.ind=TRUE))[,1]+1,
           style = RedStyle, gridExpand = TRUE)
  return(OUT)
}

MatchingCD <- function(obj) {
  ColorRows <- data.frame(which(obj$DebitSum != obj$CreditSum, arr.ind=TRUE))[,1]+1
  if (length(ColorRows) == 0){
    return('#00FF00')
  }
  else{
    return('#FF0000')
  }
}


CreateGLBalance <- function(Data, name) {
  GeneralLedgerBalancesImportJULY <- Data %>%
    mutate(`Status Code` = "NEW") %>%
    mutate(`Ledger ID` = "300000002108029") %>% 
    mutate(Date = Date) %>% #paste0("20",substring(Date,4,5), #Jan-Sep
    #"20",substring(Date,5,6), #Oct-Dec
    #"/0",substring(Date,1,1), #Jan-Sep
    #"/",substring(Date,1,2), #Oct-Dec
    #"/",substring(Date,2,3)) #Jan-Sep
    #"/",substring(Date,3,4))
    #) %>% #Oct-Dec 
    mutate(`Effective Date of Transaction` = "2020/07/31") %>%
    mutate(`Journal Source` = "SUNPAC") %>%
    mutate(`Journal Category` = "Conversion") %>%
    mutate(`Currency Code` = "USD") %>%
    mutate(`Journal Entry Creation Date` = "2020/07/31") %>% #??????????? will they modify to the correct date
    mutate(`Actual Flag` = "A") %>%
    #mutate(`ACCOUNT.NUMBER` =  gsub("-","",`Account.Number`)) %>% 
    #get old location
    mutate(oldLocation  = substring(ACCOUNT.NUMBER, 12, 14)) %>% 
    mutate(`Segment1` = paste0(substring(ACCOUNT.NUMBER, 1, 1),0)) %>% #fund 
    mutate(`Segment1` = ifelse(`Segment1` == "50", "51", `Segment1`)) %>% 
    mutate(`Segment1` = ifelse(`Segment1` == "60", "53", `Segment1`)) %>% 
    mutate(`Segment2` = paste0(substring(ACCOUNT.NUMBER, 2, 5),0)) %>% #purpose begining with 91 fail 
    mutate(`Segment2` = ifelse(`Segment2` == "68570", "68571", `Segment2`)) %>% 
    mutate(`Segment2` = ifelse(`Segment2` == "51130", "51330",`Segment2`)) %>%
    mutate(`Segment2` = ifelse(`Segment2` == "51120", "51320",`Segment2`)) %>%
    mutate(`Segment3` = paste0("0",substring(ACCOUNT.NUMBER, 6, 8))) %>% #prc 
    mutate(`Segment4` = paste0(substring(ACCOUNT.NUMBER, 9, 11),"0")) %>% #object 
    # if 312 then 312 + current tail
    mutate(Segment4 = ifelse(Segment4 == "3120", paste0("312",substring(ACCOUNT.NUMBER, 19, 19)),Segment4)) %>% 
    # if 461 -> 4611 
    mutate(Segment4 = ifelse(Segment4 == "4610", "4611", Segment4)) %>% 
    mutate(Segment4 = ifelse(Segment4 == "5410", "5411", Segment4)) %>%
    mutate(`Segment5` = as.character(substring(ACCOUNT.NUMBER, 12, 14))) %>% #location 
    mutate(`Segment6` = "000") %>% #cost center
    mutate(`Segment6` = ifelse(grepl("^7|^8|^06", substring(ACCOUNT.NUMBER,18,19)),
                               paste0("9",substring(ACCOUNT.NUMBER,18,19)),
                               Segment6)) %>%
    mutate(`Segment7` = ifelse(Segment1 == "30" | Segment1 == "40", paste0(Segment1, substring(ACCOUNT.NUMBER, 6, 8)),
                               "00000")) %>%  #project if fund 3 or 4 then fund + PRC
    mutate(`Segment8` = paste0(substring(ACCOUNT.NUMBER, 15, 17), "00")) %>% #local use 
    #change local use mapping based on Nero's mapping
    mutate(`Segment9` = `Segment1`) %>% #interfund 
    #set location mapping
    left_join(Location, by=c("Segment5" = "oldLocation")) %>% 
    mutate(Segment5 = as.character(newLocation)) %>% 
    #if no location mapping default to 81003
    mutate(Segment5 = ifelse(is.na(Segment5), "81003", Segment5)) %>%
    #if the location is 000 then 000000
    mutate(Segment5 = ifelse("000"  == substring(ACCOUNT.NUMBER, 12, 14), "00000", Segment5)) %>% 
    select(-newLocation) %>% 
    #gets old department
    mutate(Dep = substring(ACCOUNT.NUMBER, 15, 17)) %>% 
    left_join(depjointable, by=c("Dep" = "Department.Code")) %>% 
    #if location == "00000" then use department location
    #mutate(Segment5 = ifelse(Segment5 == "00000", Location_added_zeros, Segment5)) %>% 
    #change 810s to follow department
    #mutate(Segment5 = ifelse(oldLocation == "810", Location_added_zeros, Segment5)) %>% 
    #if there was no department location set location back to "00000"
    mutate(Segment5 = ifelse(is.na(Segment5), "00000", Segment5)) %>% 
    #set 81007 locations by department
    mutate(Segment5 = ifelse(Segment8 == "72200" | Segment8 == "72100" | Segment8 == "72000" |
                               Segment8 == "27000", "81007", Segment5)) %>% 
    #if there was no department location set location back to "00000"
    mutate(Segment5 = ifelse(is.na(Segment5), "81003", Segment5)) %>% 
    mutate(Tail = substring(ACCOUNT.NUMBER, 18, 19)) %>% 
    #change local use
    mutate(Segment8 = ifelse(Segment3 == "037" & Segment8 == "00000" &
                               Tail == "00", "73000", 
                             ifelse(Segment3 == "037" & Segment5 != "00000" & Segment8 == "00000", Segment5,
                                    Segment8))) %>% 
    #take all the XXX1 off local use
    mutate(Segment8 = ifelse(as.numeric(substring(as.character(Segment8),5,5)) > 0 & (Segment8 != "60001"), 
                             substring(Segment8, 1,4), Segment8)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "77200" & oldLocation == "000", "00000", Segment5)) %>% 
    mutate(Segment8 = ifelse(Segment8 =="8100", "81000", Segment8)) %>% 
    mutate(Segment8 = ifelse(Segment8 == "27000" | Segment8 == "27400" | Segment8 == "27500" |
                               Segment8 == "29000" | Segment8 == "29100" | Segment8 == "29500" |
                               Segment8 == "29600" | Segment8 == "29700" | Segment8 == "29800" | 
                               Segment8 == "29900", "83000", Segment8)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27000", "81007",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27400", "81009",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27500", "81015",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29000", "81002",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29100", "81010",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29500", "81001",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29600", "81011",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29700", "81003",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29800", "81008",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29900", "81016",Segment5)) %>% 
    mutate(Segment8 = ifelse(Segment8 == "60000", "60001",Segment8)) %>% 
    mutate(Segment8 = ifelse(grepl("^[345]", `Segment5`) & Segment8 == "00000", Segment5, Segment8)) %>% 
    mutate(Segment2 = ifelse(Segment2 == "61320", "61120", Segment2)) %>%
    mutate(Segment3 = ifelse(substring(Segment2, 3,5) == "80", "0819", Segment3)) %>%
    mutate(Segment5 = ifelse(Segment8 == "29400", "81014", Segment5)) %>%
    mutate(Segment8 = ifelse(Segment8 == "29400", "00000", Segment8)) %>%
    #mutate(Segment4 = ifelse(Segment4 == "1100", "4530", Segment4)) %>% #AUG
    mutate(Segment4 = ifelse(Segment4 == "1100", "4510", Segment4)) %>% #SEP
    mutate(Segment8 = ifelse(Segment8 == "91200", "96000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment8 == "62300", "74500", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment8 == "61320", "61120", Segment8)) %>%
    mutate(Segment7 = ifelse(grepl("^[12]", Segment2), "00000", Segment7)) %>%
    mutate(Segment8 = ifelse(Segment1 == "51", "96000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment1 == "53", "62100", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment3 == "0029", "61000", Segment8)) %>%
    left_join(FundObjPRCToLocalUse, by=c("Segment1" = "Fund", "Segment4" = "Obj", "Segment3" = "PRC")) %>%
    mutate(Segment8 = ifelse(!is.na(LocalUse), LocalUse, Segment8)) %>%
    mutate(Segment8 = ifelse(Segment1 == "10" & Segment2 == "63000" & Segment3 == "0069" &
                               Segment5 == "81003", "90000", Segment8)) %>%
    left_join(LocalUseCrossWalk, by=c("Segment8" = "Local.Use.Code", "Segment3" = "PRC",
                                      "Segment2" = "Purpose.Code")) %>%
    mutate(Segment8 = ifelse(!is.na(new.Local.Use.Code), new.Local.Use.Code, Segment8)) %>%
    left_join(LocalPRCToLocalUse, by=c("Segment3" = "PRC", "Segment8" = "oldLocalUse")) %>%
    mutate(Segment8 = ifelse(!is.na(newLocalUse), newLocalUse, Segment8)) %>%
    mutate(Segment2 = ifelse(Segment1 == "80" & Segment2 == "61320", "61120", Segment2)) %>%
    mutate(Segment8 = ifelse(Segment8 == "00000", "98000", Segment8)) %>%
    mutate(Segment5 = ifelse(grepl("^[345]", Segment8) & Segment5 == "00000", Segment8, Segment5)) %>%
    #set Department based on Organization Look-up on Segment8
    left_join(newdepcodes, by=c("Segment8" = "Department.Code")) %>%
    mutate(Segment5 = ifelse(grepl("^810", `Segment5`), Location, Segment5)) %>%
    mutate(Segment5 = ifelse(Segment5 == "00000" & !grepl("^65", `Segment2`)
                             & !grepl("^64", `Segment2`), Location, Segment5)) %>%
    #DPI rules for purpose
    mutate(`Segment5` = ifelse((Segment2 >= 50000 & Segment2 < 60000) &
                                 !(grepl("^[345]", `Segment5`)), 
                               "00000",`Segment5`)) %>% 
    mutate(Segment5 = ifelse ((Segment2 >= 60000 & Segment2 < 70000) & 
                                oldLocation == "439", "81011", Segment5)) %>% 
    #default location to 81003 if purpose requires 810 location
    mutate(`Segment5` = ifelse((Segment2 >= 60000 & Segment2 < 70000) & 
                                 (!(Segment2 >= 64000 & Segment2 < 66000) |
                                    Segment4 < 1200) & !(grepl("^810", `Segment5`)),
                               Location,Segment5)) %>% 
    mutate(Segment5 = ifelse(oldLocation == "860", "86001", Segment5)) %>%
    mutate(Segment4 = ifelse(Segment4 == "3127", "3120", Segment4)) %>%
    mutate(Segment5 = ifelse(grepl("^1|^2|^3|^4", Segment2), "00000", Segment5)) %>%
    mutate(`Segment10` = "") %>%
    mutate(`Segment11` = "") %>%
    mutate(`Segment12` = "") %>%
    mutate(`Segment13` = "") %>%
    mutate(`Segment14` = "") %>%
    mutate(`Segment15` = "") %>%
    mutate(`Segment16` = "") %>%
    mutate(`Segment17` = "") %>%
    mutate(`Segment18` = "") %>%
    mutate(`Segment19` = "") %>%
    mutate(`Segment20` = "") %>%
    mutate(`Segment21` = "") %>%
    mutate(`Segment22` = "") %>%
    mutate(`Segment23` = "") %>%
    mutate(`Segment24` = "") %>%
    mutate(`Segment25` = "") %>%
    mutate(`Segment26` = "") %>%
    mutate(`Segment27` = "") %>%
    mutate(`Segment28` = "") %>%
    mutate(`Segment29` = "") %>%
    mutate(`Segment30` = "") %>%
    filter(!is.na(`Transactions`) | trimws(`Transactions`) == "-") %>% 
    mutate(`Entered Debit Amount` = ifelse(sign(as.numeric(Transactions)) == 1, as.numeric(Transactions), 0)) %>%
    mutate(`Entered Credit Amount` = ifelse(sign(as.numeric(Transactions)) == -1, as.numeric(Transactions) * (-1), 0)) %>% 
    mutate(`Converted Debit Amount` = "") %>%
    mutate(`Converted Credit Amount` = "") %>%
    mutate(`REFERENCE1 (Batch Name)` = "SunPac Conversion Batch") %>%
    mutate(`REFERENCE2 (Batch Description)` = "SunPac Conversion Batch - To bring in historical transactions") %>%
    mutate(`REFERENCE3` = "") %>%
    mutate(`REFERENCE4 (Journal Entry Name)` = "SunPac Conversion Journal Entry") %>%
    mutate(`REFERENCE5 (Journal Entry Description)` = "SunPac Conversion Journal Entry - To bring in historical transactions") %>%
    mutate(`REFERENCE6 (Journal Entry Reference)` = "") %>%
    mutate(`REFERENCE7 (Journal Entry Reversal flag)` = "") %>%
    mutate(`REFERENCE8 (Journal Entry Reversal Period)` = "") %>%
    mutate(`REFERENCE9 (Journal Reversal Method)` = "") %>%
    mutate(`REFERENCE10 (Journal Entry Line Description)` = "") %>%
    mutate(`Reference column 1` = "") %>%
    mutate(`Reference column 2` = "") %>%
    mutate(`Reference column 3` = "") %>%
    mutate(`Reference column 4` = "") %>%
    mutate(`Reference column 5` = "") %>%
    mutate(`Reference column 6` = "") %>%
    mutate(`Reference column 7` = "") %>%
    mutate(`Reference column 8` = "") %>%
    mutate(`Reference column 9` = "") %>%
    mutate(`Reference column 10` = "") %>%
    mutate(`Statistical Amount` = "") %>%
    mutate(`Currency Conversion Type` = "") %>%
    mutate(`Currency Conversion Date` = "") %>%
    mutate(`Currency Conversion Rate` = "") %>%
    mutate(`Interface Group Identifier` = "") %>%
    mutate(`Context field for Journal Entry Line DFF` = "") %>%
    mutate(`ATTRIBUTE1 Value for Journal Entry Line DFF` = "") %>%
    mutate(`ATTRIBUTE2 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute3 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute4 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute5 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute6 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute7 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute8 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute9 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute10 Value for Journal Entry Line DFF` = "") %>%
    mutate(`Attribute11 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute12 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute13 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute14 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute15 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute16 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute17 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute18 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute19 Value for Captured Information DFF` = "") %>%
    mutate(`Attribute20 Value for Captured Information DFF` = "") %>%
    mutate(`Context field for Caputred Information DFF` = "") %>%
    mutate(`Average Journal Flag` = "") %>%
    mutate(`Clearing Company` = "") %>%
    mutate(`Ledger Name` = "GCS Primary Ledger") %>%
    mutate(`Encumbrance Type ID` = "") %>%
    mutate(`Reconciliation Reference` = "") %>%
    mutate(`Period Name` = format.Date(as.Date(`Effective Date of Transaction`), "%b")) %>%
    mutate(`Period Name` = paste0("GCS-",`Period Name`, "-20")) %>% 
    filter(!is.na(`Entered Debit Amount`)) %>% 
    filter(!is.na(`Entered Credit Amount`)) %>% 
    select(ACCOUNT.NUMBER,oldLocation, Dep,`Status Code`,`Ledger ID`,`Effective Date of Transaction`,`Journal Source`,`Journal Category`,
           `Currency Code`,`Journal Entry Creation Date`,`Actual Flag`,`Segment1`,`Segment2`,
           `Segment3`,`Segment4`,`Segment5`,`Segment6`,`Segment7`,`Segment8`,`Segment9`,`Segment10`,`Segment11`,
           `Segment12`,`Segment13`,`Segment14`,`Segment15`,`Segment16`,`Segment17`,`Segment18`,`Segment19`,`Segment20`,
           `Segment21`,`Segment22`,`Segment23`,`Segment24`,`Segment25`,`Segment26`,`Segment27`,`Segment28`,`Segment29`,`Segment30`,
           `Entered Debit Amount`,`Entered Credit Amount`,`Converted Debit Amount`,`Converted Credit Amount`,`REFERENCE1 (Batch Name)`,
           `REFERENCE2 (Batch Description)`,`REFERENCE3`, `REFERENCE4 (Journal Entry Name)`,`REFERENCE5 (Journal Entry Description)`,
           `REFERENCE6 (Journal Entry Reference)`,`REFERENCE7 (Journal Entry Reversal flag)`,`REFERENCE8 (Journal Entry Reversal Period)`,
           `REFERENCE9 (Journal Reversal Method)`,`REFERENCE10 (Journal Entry Line Description)`,
           `Reference column 1`,`Reference column 2`,`Reference column 3`,`Reference column 4`,`Reference column 5`,`Reference column 6`,`Reference column 7`,`Reference column 8`,`Reference column 9`,`Reference column 10`,`Statistical Amount`,
           `Currency Conversion Type`,`Currency Conversion Date`,`Currency Conversion Rate`,`Interface Group Identifier`,`Context field for Journal Entry Line DFF`,
           `ATTRIBUTE1 Value for Journal Entry Line DFF`,`ATTRIBUTE2 Value for Journal Entry Line DFF`,`Attribute3 Value for Journal Entry Line DFF`,`Attribute4 Value for Journal Entry Line DFF`,`Attribute5 Value for Journal Entry Line DFF`,
           `Attribute6 Value for Journal Entry Line DFF`,`Attribute7 Value for Journal Entry Line DFF`,`Attribute8 Value for Journal Entry Line DFF`,
           `Attribute9 Value for Journal Entry Line DFF`,`Attribute10 Value for Journal Entry Line DFF`,`Attribute11 Value for Captured Information DFF`,`Attribute12 Value for Captured Information DFF`,`Attribute13 Value for Captured Information DFF`,
           `Attribute14 Value for Captured Information DFF`,`Attribute15 Value for Captured Information DFF`,`Attribute16 Value for Captured Information DFF`,
           `Attribute17 Value for Captured Information DFF`,`Attribute18 Value for Captured Information DFF`,`Attribute19 Value for Captured Information DFF`,`Attribute20 Value for Captured Information DFF`,`Context field for Caputred Information DFF`,
           `Average Journal Flag`,`Clearing Company`,`Ledger Name`,`Encumbrance Type ID`,`Reconciliation Reference`,`Period Name`) %>% 
    mutate(Segment5 = ifelse(Segment5 == "86000", "86001", Segment5))
  
  Credit <- GeneralLedgerBalancesImportJULY %>% 
    group_by(Segment1, Segment2, Segment3, Segment4, Segment5, Segment6, Segment7, Segment8, Segment9, `Period Name`) %>%
    mutate(`Entered Credit Amount` = format(round(sum(`Entered Credit Amount`), 2), nsmall = 2)) %>% 
    mutate(`Entered Debit Amount` = format(round(0.00, 2), nsmall = 2)) %>%
    distinct(Segment1, Segment2, Segment3, Segment4, Segment5, Segment6, Segment7, Segment8, Segment9,
             `Entered Credit Amount`, `Entered Debit Amount`, `Period Name`, .keep_all = TRUE)
  
  Debit <- GeneralLedgerBalancesImportJULY %>% 
    group_by(Segment1, Segment2, Segment3, Segment4, Segment5, Segment6, Segment7, Segment8, Segment9, `Period Name`) %>%
    mutate(`Entered Credit Amount` = format(round(0.00, 2), nsmall = 2)) %>% 
    mutate(`Entered Debit Amount` = format(round(sum(`Entered Debit Amount`), 2), nsmall = 2)) %>%
    distinct(Segment1, Segment2, Segment3, Segment4, Segment5, Segment6, Segment7, Segment8, Segment9,
             `Entered Credit Amount`, `Entered Debit Amount`, `Period Name`, .keep_all = TRUE) 
  
  GLBudget <- rbind(Credit, Debit) %>% 
    filter(!(`Entered Debit Amount` == "0.00" & `Entered Credit Amount` == "0.00")) %>% 
    filter(!(`Entered Debit Amount` == "0" & `Entered Credit Amount` == "0")) %>% 
    mutate(`Entered Debit Amount` = as.numeric(`Entered Debit Amount`)) %>% 
    mutate(`Entered Credit Amount` = as.numeric(`Entered Credit Amount`)) %>% 
    #mutate(`Entered Credit Amount` = format(round(`Entered Credit Amount`, 2), nsmall = 2)) %>% 
    #mutate(`Entered Debit Amount` = format(round(`Entered Debit Amount`, 2), nsmall = 2)) %>% 
    filter(Segment1 != "40") %>% 
    mutate(Segment7 = ifelse(Segment1 == "51", "96000", Segment7)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "53200", "53200", Segment5)) %>% 
    mutate(Segment2 = ifelse(Segment1 == "80" & Segment2 == "61320", "61120", Segment2)) %>%
    mutate(Segment7 = ifelse(Segment3 == "0035", "50035", Segment7)) %>%
    mutate(Segment7 = ifelse(Segment1 == "30" && Segment3 == "0163", "30163", Segment7))
  
  saveWorkbook(SummaryBalanceReport(GLBudget), paste(name,"BalanceReport ", Sys.Date(),".xlsx"), overwrite = TRUE)
}

#Pass Data, FileName, Version
CreateControlBudger <- function(Data, name, i) {
  GeneralLedgerCurrentBudgetImport <- Data %>%
    mutate(`Source Budget Type` = "OTHER") %>%
    mutate(`Source Budget Name` = "Source Budgets") %>%
    mutate(`Budget Entry Name` = "FY22 Revision 2021-08") %>%
    mutate(`Line Number` = row_number()) %>%
    mutate(`Amount` = TRANS.AMOUNT) %>%
    mutate(`Currency Code` = "USD") %>%
    mutate(`Period Name` = Period) %>%
    mutate(`UOM Code` = "") %>%
    mutate(`Segment1` = paste0(substring(ACCOUNT.NUMBER, 1, 1),0)) %>% #fund 
    mutate(`Segment1` = ifelse(`Segment1` == "50", "51", `Segment1`)) %>% 
    mutate(`Segment1` = ifelse(`Segment1` == "60", "53", `Segment1`)) %>% 
    mutate(`Segment2` = paste0(substring(ACCOUNT.NUMBER, 2, 5),0)) %>% #purpose begining with 91 fail 
    mutate(`Segment2` = ifelse(`Segment2` == "68570", "68571", `Segment2`)) %>% 
    mutate(`Segment2` = ifelse(`Segment2` == "51130", "51330",`Segment2`)) %>%
    mutate(`Segment2` = ifelse(`Segment2` == "51120", "51320",`Segment2`)) %>%
    mutate(`Segment3` = paste0("0",substring(ACCOUNT.NUMBER, 6, 8))) %>% #prc 
    mutate(`Segment4` = paste0(substring(ACCOUNT.NUMBER, 9, 11),"0")) %>% #object 
    # if 312 then 312 + current tail
    mutate(Segment4 = ifelse(Segment4 == "3120", paste0("312",substring(ACCOUNT.NUMBER, 19, 19)),Segment4)) %>% 
    # if 461 -> 4611 
    mutate(Segment4 = ifelse(Segment4 == "4610", "4611", Segment4)) %>% 
    mutate(Segment4 = ifelse(Segment4 == "5410", "5411", Segment4)) %>%
    mutate(`Segment5` = as.character(substring(ACCOUNT.NUMBER, 12, 14))) %>% #location 
    mutate(`Segment6` = "000") %>% #cost center
    mutate(`Segment6` = ifelse(grepl("^7|^8|^06", substring(ACCOUNT.NUMBER,18,19)),
                               paste0("9",substring(ACCOUNT.NUMBER,18,19)),
                               Segment6)) %>%
    mutate(`Segment7` = ifelse(Segment1 == "30" | Segment1 == "40", paste0(Segment1, substring(ACCOUNT.NUMBER, 6, 8)),
                               "00000")) %>%  #project if fund 3 or 4 then fund + PRC
    mutate(`Segment8` = paste0(substring(ACCOUNT.NUMBER, 15, 17), "00")) %>% #local use 
    #change local use mapping based on Nero's mapping
    mutate(`Segment9` = `Segment1`) %>% #interfund 
    #set location mapping
    left_join(Location, by=c("Segment5" = "oldLocation")) %>% 
    mutate(Segment5 = as.character(newLocation)) %>% 
    #if no location mapping default to 81003
    mutate(Segment5 = ifelse(is.na(Segment5), "81003", Segment5)) %>%
    #if the location is 000 then 000000
    mutate(Segment5 = ifelse("000"  == substring(ACCOUNT.NUMBER, 12, 14), "00000", Segment5)) %>% 
    select(-newLocation) %>% 
    #gets old department
    mutate(Dep = substring(ACCOUNT.NUMBER, 15, 17)) %>% 
    left_join(depjointable, by=c("Dep" = "Department.Code")) %>% 
    #if location == "00000" then use department location
    #mutate(Segment5 = ifelse(Segment5 == "00000", Location_added_zeros, Segment5)) %>% 
    #get old location
    mutate(oldLocation  = substring(ACCOUNT.NUMBER, 12, 14)) %>% 
    #change 810s to follow department
    #mutate(Segment5 = ifelse(oldLocation == "810", Location_added_zeros, Segment5)) %>% 
    #if there was no department location set location back to "00000"
    mutate(Segment5 = ifelse(is.na(Segment5), "00000", Segment5)) %>% 
    #set 81007 locations by department
    mutate(Segment5 = ifelse(Segment8 == "72200" | Segment8 == "72100" | Segment8 == "72000" |
                               Segment8 == "27000", "81007", Segment5)) %>% 
    #if there was no department location set location back to "00000"
    mutate(Segment5 = ifelse(is.na(Segment5), "81003", Segment5)) %>% 
    mutate(Tail = substring(ACCOUNT.NUMBER, 18, 19)) %>% 
    #change local use
    mutate(Segment8 = ifelse(Segment3 == "037" & Segment8 == "00000" &
                               Tail == "00", "73000", 
                             ifelse(Segment3 == "037" & Segment5 != "00000" & Segment8 == "00000", Segment5,
                                    Segment8))) %>% 
    #take all the XXX1 off local use
    mutate(Segment8 = ifelse(as.numeric(substring(as.character(Segment8),5,5)) > 0 & (Segment8 != "60001"), 
                             substring(Segment8, 1,4), Segment8)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "77200" & oldLocation == "000", "00000", Segment5)) %>% 
    mutate(Segment8 = ifelse(Segment8 =="8100", "81000", Segment8)) %>% 
    mutate(Segment8 = ifelse(Segment8 == "27000" | Segment8 == "27400" | Segment8 == "27500" |
                               Segment8 == "29000" | Segment8 == "29100" | Segment8 == "29500" |
                               Segment8 == "29600" | Segment8 == "29700" | Segment8 == "29800" | 
                               Segment8 == "29900", "83000", Segment8)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27000", "81007",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27400", "81009",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27500", "81015",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29000", "81002",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29100", "81010",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29500", "81001",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29600", "81011",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29700", "81003",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29800", "81008",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29900", "81016",Segment5)) %>% 
    mutate(Segment8 = ifelse(Segment8 == "60000", "60001",Segment8)) %>% 
    mutate(Segment8 = ifelse(grepl("^[345]", `Segment5`) & Segment8 == "00000", Segment5, Segment8)) %>% 
    mutate(Segment2 = ifelse(Segment2 == "61320", "61120", Segment2)) %>%
    mutate(Segment3 = ifelse(substring(Segment2, 3,5) == "80", "0819", Segment3)) %>%
    mutate(Segment5 = ifelse(Segment8 == "29400", "81014", Segment5)) %>%
    mutate(Segment8 = ifelse(Segment8 == "29400", "00000", Segment8)) %>%
    #mutate(Segment4 = ifelse(Segment4 == "1100", "4530", Segment4)) %>% #AUG
    mutate(Segment4 = ifelse(Segment4 == "1100", "4510", Segment4)) %>% #SEP
    mutate(Segment8 = ifelse(Segment8 == "91200", "96000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment8 == "62300", "74500", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment8 == "61320", "61120", Segment8)) %>%
    mutate(Segment7 = ifelse(grepl("^[12]", Segment2), "00000", Segment7)) %>%
    mutate(Segment8 = ifelse(Segment1 == "51", "96000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment1 == "53", "62100", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment3 == "0029", "61000", Segment8)) %>%
    left_join(FundObjPRCToLocalUse, by=c("Segment1" = "Fund", "Segment4" = "Obj", "Segment3" = "PRC")) %>%
    mutate(Segment8 = ifelse(!is.na(LocalUse), LocalUse, Segment8)) %>%
    mutate(Segment8 = ifelse(Segment1 == "10" & Segment2 == "63000" & Segment3 == "0069" &
                               Segment5 == "81003", "90000", Segment8)) %>%
    left_join(LocalUseCrossWalk, by=c("Segment8" = "Local.Use.Code", "Segment3" = "PRC",
                                      "Segment2" = "Purpose.Code")) %>%
    mutate(Segment8 = ifelse(!is.na(new.Local.Use.Code), new.Local.Use.Code, Segment8)) %>%
    left_join(LocalPRCToLocalUse, by=c("Segment3" = "PRC", "Segment8" = "oldLocalUse")) %>%
    mutate(Segment8 = ifelse(!is.na(newLocalUse), newLocalUse, Segment8)) %>%
    mutate(Segment2 = ifelse(Segment1 == "80" & Segment2 == "61320", "61120", Segment2)) %>%
    mutate(Segment8 = ifelse(Segment8 == "00000", "98000", Segment8)) %>%
    mutate(Segment5 = ifelse(grepl("^[345]", Segment8) & Segment5 == "00000", Segment8, Segment5)) %>%
    #set Department based on Organization Look-up on Segment8
    left_join(newdepcodes, by=c("Segment8" = "Department.Code")) %>%
    mutate(Segment5 = ifelse(grepl("^810", `Segment5`), Location, Segment5)) %>%
    mutate(Segment5 = ifelse(Segment5 == "00000" & !grepl("^65", `Segment2`)
                             & !grepl("^64", `Segment2`), Location, Segment5)) %>%
    #DPI rules for purpose
    mutate(`Segment5` = ifelse((Segment2 >= 50000 & Segment2 < 60000) &
                                 !(grepl("^[345]", `Segment5`)), 
                               "00000",`Segment5`)) %>% 
    mutate(Segment5 = ifelse ((Segment2 >= 60000 & Segment2 < 70000) & 
                                oldLocation == "439", "81011", Segment5)) %>% 
    #default location to 81003 if purpose requires 810 location
    mutate(`Segment5` = ifelse((Segment2 >= 60000 & Segment2 < 70000) & 
                                 (!(Segment2 >= 64000 & Segment2 < 66000) |
                                    Segment4 < 1200) & !(grepl("^810", `Segment5`)),
                               Location,Segment5)) %>% 
    mutate(Segment5 = ifelse(oldLocation == "860", "86001", Segment5)) %>%
    mutate(Segment4 = ifelse(Segment4 == "3127", "3120", Segment4)) %>%
    mutate(`Segment10` = "") %>%
    mutate(`Segment11` = "") %>%
    mutate(`Segment12` = "") %>%
    mutate(`Segment13` = "") %>%
    mutate(`Segment14` = "") %>%
    mutate(`Segment15` = "") %>%
    mutate(`Segment16` = "") %>%
    mutate(`Segment17` = "") %>%
    mutate(`Segment18` = "") %>%
    mutate(`Segment19` = "") %>%
    mutate(`Segment20` = "") %>%
    mutate(`Segment21` = "") %>%
    mutate(`Segment22` = "") %>%
    mutate(`Segment23` = "") %>%
    mutate(`Segment24` = "") %>%
    mutate(`Segment25` = "") %>%
    mutate(`Segment26` = "") %>%
    mutate(`Segment27` = "") %>%
    mutate(`Segment28` = "") %>%
    mutate(`Segment29` = "") %>%
    mutate(`Segment30` = "") %>%
    mutate(`Comment` = "") %>%
    mutate(`Attribute Category` = "") %>%
    mutate(`Attribute Char1` = "") %>%
    mutate(`Attribute Char2` = "") %>%
    mutate(`Attribute Char3` = "") %>%
    mutate(`Attribute Char4` = "") %>%
    mutate(`Attribute Char5` = "") %>%
    mutate(`Attribute Char6` = "") %>%
    mutate(`Attribute Char7` = "") %>%
    mutate(`Attribute Char8` = "") %>%
    mutate(`Attribute Char9` = "") %>%
    mutate(`Attribute Char10` = "") %>%
    mutate(`Attribute number1` = "") %>%
    mutate(`Attribute number2` = "") %>%
    mutate(`Attribute number3` = "") %>%
    mutate(`Attribute number4` = "") %>%
    mutate(`Attribute number5` = "") %>%
    mutate(`Attribute Date1` = "") %>%
    mutate(`Attribute Date2` = "") %>%
    mutate(`Attribute Date3` = "") %>%
    mutate(`Attribute Date4` = "") %>%
    mutate(`Attribute Date5` = "") %>%
    select(ACCOUNT.NUMBER, oldLocation, Dep, `Source Budget Type`,`Source Budget Name`,`Budget Entry Name`,`Line Number`,`Amount`,
           `Currency Code`,`Period Name`,`UOM Code`,`Segment1`,`Segment2`,
           `Segment3`,`Segment4`,`Segment5`,`Segment6`,`Segment7`,`Segment8`,`Segment9`,`Segment10`,`Segment11`,
           `Segment12`,`Segment13`,`Segment14`,`Segment15`,`Segment16`,`Segment17`,`Segment18`,`Segment19`,`Segment20`,
           `Segment21`,`Segment22`,`Segment23`,`Segment24`,`Segment25`,`Segment26`,`Segment27`,`Segment28`,`Segment29`,`Segment30`,
           `Comment`,`Attribute Category`,`Attribute Char1`,`Attribute Char2`,`Attribute Char3`,
           `Attribute Char4`,`Attribute Char5`,`Attribute Char6`,`Attribute Char7`,`Attribute Char8`,
           `Attribute Char9`,`Attribute Char10`,`Attribute number1`,`Attribute number2`,
           `Attribute number3`,`Attribute number4`,`Attribute number5`,`Attribute Date1`,`Attribute Date2`,
           `Attribute Date3`,`Attribute Date4`,`Attribute Date5`) %>% 
    #filter(substring(as.character(`Segment2`),1,1) != "3" & substring(as.character(`Segment2`),1,1) != "4") %>% 
    mutate(`Line Number` = row_number()) %>% 
    filter(as.numeric(substring(Segment2,1,1)) > 4) %>%
    mutate(END = substring(ACCOUNT.NUMBER,18,19))
  
  DataTest <- GeneralLedgerCurrentBudgetImport %>% 
    mutate(ACCOUNT.NUMBER = trimws(ACCOUNT.NUMBER)) %>% 
    mutate(col5  = substring(ACCOUNT.NUMBER, 12, 14)) %>% 
    mutate(dep = substring(ACCOUNT.NUMBER, 15, 17)) %>% 
    select(ACCOUNT.NUMBER, col5, dep, Segment2, Segment5, Segment8) %>% 
    distinct("Location"=col5, dep, Segment5, Segment8, .keep_all = TRUE)
  
  GLRevision <- GeneralLedgerCurrentBudgetImport %>% 
    mutate(ACCOUNT.NUMBER = trimws(ACCOUNT.NUMBER)) %>% 
    select(-ACCOUNT.NUMBER, -oldLocation, -oldLocation, -Dep) %>%
    filter(Segment3 %in% ControlBudgetPRCs$PRC | Segment1 != "30") %>%
    mutate(`Line Number` = row_number()) %>%
    filter(Segment1 != "30")
  
  write.xlsx(GLRevision, paste0(name,"_GL_Control_Budget_V",i, Sys.Date(),".xlsx"), overwrite = TRUE)
  
  TestFile <- GeneralLedgerCurrentBudgetImport %>% 
    mutate(ACCOUNT.NUMBER = trimws(ACCOUNT.NUMBER)) %>% 
    select(ACCOUNT.NUMBER, oldLocation, "oldDepartment" = Dep, Segment1, Segment2, Segment3, Segment4, Segment5, Segment6,
           Segment7, Segment8, Segment9, Amount) %>%
    filter(Segment3 %in% ControlBudgetPRCs$PRC | Segment1 != "30") %>%
    mutate(Purpose1st = substring(Segment2,1,1)) %>%
    filter(Segment1 != "30")
  
  Fund <- TestFile %>%
    group_by(Segment1) %>%
    mutate(Total = sum(Amount)) %>%
    distinct(Segment1, Amount)
  
  Purpose <- TestFile %>%
    group_by(Segment2) %>%
    mutate(Total = sum(Amount)) %>%
    distinct(Segment2, Amount)
  
  PRC <- TestFile %>%
    group_by(Segment3) %>%
    mutate(Total = sum(Amount)) %>%
    distinct(Segment3, Amount)
  
  Object <- TestFile %>%
    group_by(Segment4) %>%
    mutate(Total = sum(Amount)) %>%
    distinct(Segment4, Amount)
  
  Location <- TestFile %>%
    group_by(Segment5) %>%
    mutate(Total = sum(Amount)) %>%
    distinct(Segment5, Amount)
  
  Project <- TestFile %>%
    group_by(Segment7) %>%
    mutate(Total = sum(Amount)) %>%
    distinct(Segment7, Amount)
  
  LocalUse <- TestFile %>%
    group_by(Segment8) %>%
    mutate(Total = sum(Amount)) %>%
    distinct(Segment8, Amount)
  
  OUT <- createWorkbook()
  
  # Add some sheets to the workbook
  addWorksheet(OUT, "Data")
  addWorksheet(OUT, "Fund")
  addWorksheet(OUT, "Purpose")
  addWorksheet(OUT, "Object")
  addWorksheet(OUT, "Location")
  addWorksheet(OUT, "Project")
  addWorksheet(OUT, "LocalUse")
  
  # Write the data to the sheets
  writeData(OUT, sheet = "Data", x = TestFile)
  writeData(OUT, sheet = "Fund", x = Fund)
  writeData(OUT, sheet = "Purpose", x = Purpose)
  writeData(OUT, sheet = "Object", x = Object)
  writeData(OUT, sheet = "Location", x = Location)
  writeData(OUT, sheet = "Project", x = Project)
  writeData(OUT, sheet = "LocalUse", x = LocalUse)
  
  # Export the file
  saveWorkbook(OUT, paste(name,"_ControlBudgetReport V",i, Sys.Date(),".xlsx"), overwrite = TRUE)
}

CreateGLBudget <- function(Data, Name, i){
  GeneralLedgerBudgetBalances <- Data %>% 
    mutate(`Run Name` = "GCS_REV_FY2022_Revenues") %>% 
    mutate(`Status` = "NEW") %>% 
    mutate(`Ledger Id` = "300000002108029") %>% 
    mutate(`Budget Name` = "Revision Budget") %>% 
    mutate(`Period` = Period) %>% 
    mutate(`Currency` = "USD") %>% 
    mutate(`Segment1` = paste0(substring(ACCOUNT.NUMBER, 1, 1),0)) %>% #fund 
    mutate(`Segment1` = ifelse(`Segment1` == "50", "51", `Segment1`)) %>% 
    mutate(`Segment1` = ifelse(`Segment1` == "60", "53", `Segment1`)) %>% 
    mutate(`Segment2` = paste0(substring(ACCOUNT.NUMBER, 2, 5),0)) %>% #purpose begining with 91 fail 
    mutate(`Segment2` = ifelse(`Segment2` == "68570", "68571", `Segment2`)) %>% 
    mutate(`Segment2` = ifelse(`Segment2` == "51130", "51330",`Segment2`)) %>%
    mutate(`Segment2` = ifelse(`Segment2` == "51120", "51320",`Segment2`)) %>%
    mutate(`Segment3` = paste0("0",substring(ACCOUNT.NUMBER, 6, 8))) %>% #prc 
    mutate(`Segment4` = paste0(substring(ACCOUNT.NUMBER, 9, 11),"0")) %>% #object 
    # if 312 then 312 + current tail
    mutate(Segment4 = ifelse(Segment4 == "3120", paste0("312",substring(ACCOUNT.NUMBER, 19, 19)),Segment4)) %>% 
    # if 461 -> 4611 
    mutate(Segment4 = ifelse(Segment4 == "4610", "4611", Segment4)) %>% 
    mutate(Segment4 = ifelse(Segment4 == "5410", "5411", Segment4)) %>%
    mutate(`Segment5` = as.character(substring(ACCOUNT.NUMBER, 12, 14))) %>% #location 
    mutate(`Segment6` = "000") %>% #cost center
    mutate(`Segment6` = ifelse(grepl("^7|^8|^06", substring(ACCOUNT.NUMBER,18,19)),
                               paste0("9",substring(ACCOUNT.NUMBER,18,19)),
                               Segment6)) %>%
    mutate(`Segment7` = ifelse(Segment1 == "30" | Segment1 == "40", paste0(Segment1, substring(ACCOUNT.NUMBER, 6, 8)),
                               "00000")) %>%  #project if fund 3 or 4 then fund + PRC
    mutate(`Segment8` = paste0(substring(ACCOUNT.NUMBER, 15, 17), "00")) %>% #local use 
    #change local use mapping based on Nero's mapping
    mutate(`Segment9` = `Segment1`) %>% #interfund 
    #set location mapping
    left_join(Location, by=c("Segment5" = "oldLocation")) %>% 
    mutate(Segment5 = as.character(newLocation)) %>% 
    #if no location mapping default to 81003
    mutate(Segment5 = ifelse(is.na(Segment5), "81003", Segment5)) %>%
    #if the location is 000 then 000000
    mutate(Segment5 = ifelse("000"  == substring(ACCOUNT.NUMBER, 12, 14), "00000", Segment5)) %>% 
    select(-newLocation) %>% 
    #gets old department
    mutate(Dep = substring(ACCOUNT.NUMBER, 15, 17)) %>% 
    left_join(depjointable, by=c("Dep" = "Department.Code")) %>% 
    #if location == "00000" then use department location
    #mutate(Segment5 = ifelse(Segment5 == "00000", Location_added_zeros, Segment5)) %>% 
    #get old location
    mutate(oldLocation  = substring(ACCOUNT.NUMBER, 12, 14)) %>% 
    #change 810s to follow department
    #mutate(Segment5 = ifelse(oldLocation == "810", Location_added_zeros, Segment5)) %>% 
    #if there was no department location set location back to "00000"
    mutate(Segment5 = ifelse(is.na(Segment5), "00000", Segment5)) %>% 
    #set 81007 locations by department
    mutate(Segment5 = ifelse(Segment8 == "72200" | Segment8 == "72100" | Segment8 == "72000" |
                               Segment8 == "27000", "81007", Segment5)) %>% 
    #if there was no department location set location back to "00000"
    mutate(Segment5 = ifelse(is.na(Segment5), "81003", Segment5)) %>% 
    mutate(Tail = substring(ACCOUNT.NUMBER, 18, 19)) %>% 
    #change local use
    mutate(Segment8 = ifelse(Segment3 == "037" & Segment8 == "00000" &
                               Tail == "00", "73000", 
                             ifelse(Segment3 == "037" & Segment5 != "00000" & Segment8 == "00000", Segment5,
                                    Segment8))) %>% 
    #take all the XXX1 off local use
    mutate(Segment8 = ifelse(as.numeric(substring(as.character(Segment8),5,5)) > 0 & (Segment8 != "60001"), 
                             substring(Segment8, 1,4), Segment8)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "77200" & oldLocation == "000", "00000", Segment5)) %>% 
    mutate(Segment8 = ifelse(Segment8 =="8100", "81000", Segment8)) %>% 
    mutate(Segment8 = ifelse(Segment8 == "27000" | Segment8 == "27400" | Segment8 == "27500" |
                               Segment8 == "29000" | Segment8 == "29100" | Segment8 == "29500" |
                               Segment8 == "29600" | Segment8 == "29700" | Segment8 == "29800" | 
                               Segment8 == "29900", "83000", Segment8)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27000", "81007",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27400", "81009",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "27500", "81015",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29000", "81002",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29100", "81010",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29500", "81001",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29600", "81011",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29700", "81003",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29800", "81008",Segment5)) %>% 
    mutate(Segment5 = ifelse(Segment8 == "29900", "81016",Segment5)) %>% 
    mutate(Segment8 = ifelse(Segment8 == "60000", "60001",Segment8)) %>% 
    mutate(Segment8 = ifelse(grepl("^[345]", `Segment5`) & Segment8 == "00000", Segment5, Segment8)) %>% 
    mutate(Segment2 = ifelse(Segment2 == "61320", "61120", Segment2)) %>%
    mutate(Segment3 = ifelse(substring(Segment2, 3,5) == "80", "0819", Segment3)) %>%
    mutate(Segment5 = ifelse(Segment8 == "29400", "81014", Segment5)) %>%
    mutate(Segment8 = ifelse(Segment8 == "29400", "00000", Segment8)) %>%
    #mutate(Segment4 = ifelse(Segment4 == "1100", "4530", Segment4)) %>% #AUG
    mutate(Segment4 = ifelse(Segment4 == "1100", "4510", Segment4)) %>% #SEP
    mutate(Segment8 = ifelse(Segment8 == "91200", "96000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment8 == "62300", "74500", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment8 == "61320", "61120", Segment8)) %>%
    mutate(Segment7 = ifelse(grepl("^[12]", Segment2), "00000", Segment7)) %>%
    mutate(Segment8 = ifelse(Segment1 == "51", "96000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment1 == "53", "62100", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment3 == "0029", "61000", Segment8)) %>%
    left_join(FundObjPRCToLocalUse, by=c("Segment1" = "Fund", "Segment4" = "Obj", "Segment3" = "PRC")) %>%
    mutate(Segment8 = ifelse(!is.na(LocalUse), LocalUse, Segment8)) %>%
    mutate(Segment8 = ifelse(Segment1 == "10" & Segment2 == "63000" & Segment3 == "0069" &
                               Segment5 == "81003", "90000", Segment8)) %>%
    left_join(LocalUseCrossWalk, by=c("Segment8" = "Local.Use.Code", "Segment3" = "PRC",
                                      "Segment2" = "Purpose.Code")) %>%
    mutate(Segment8 = ifelse(!is.na(new.Local.Use.Code), new.Local.Use.Code, Segment8)) %>%
    left_join(LocalPRCToLocalUse, by=c("Segment3" = "PRC", "Segment8" = "oldLocalUse")) %>%
    mutate(Segment8 = ifelse(!is.na(newLocalUse), newLocalUse, Segment8)) %>%
    mutate(Segment2 = ifelse(Segment1 == "80" & Segment2 == "61320", "61120", Segment2)) %>%
    mutate(Segment8 = ifelse(Segment8 == "00000", "98000", Segment8)) %>%
    mutate(Segment5 = ifelse(grepl("^[345]", Segment8) & Segment5 == "00000", Segment8, Segment5)) %>%
    #set Department based on Organization Look-up on Segment8
    left_join(newdepcodes, by=c("Segment8" = "Department.Code")) %>%
    mutate(Segment5 = ifelse(grepl("^810", `Segment5`), Location, Segment5)) %>%
    mutate(Segment5 = ifelse(Segment5 == "00000" & !grepl("^65", `Segment2`)
                             & !grepl("^64", `Segment2`), Location, Segment5)) %>%
    #DPI rules for purpose
    mutate(`Segment5` = ifelse((Segment2 >= 50000 & Segment2 < 60000) &
                                 !(grepl("^[345]", `Segment5`)), 
                               "00000",`Segment5`)) %>% 
    mutate(Segment5 = ifelse ((Segment2 >= 60000 & Segment2 < 70000) & 
                                oldLocation == "439", "81011", Segment5)) %>% 
    #default location to 81003 if purpose requires 810 location
    mutate(`Segment5` = ifelse((Segment2 >= 60000 & Segment2 < 70000) & 
                                 (!(Segment2 >= 64000 & Segment2 < 66000) |
                                    Segment4 < 1200) & !(grepl("^810", `Segment5`)),
                               Location,Segment5)) %>% 
    mutate(Segment5 = ifelse(oldLocation == "860", "86001", Segment5)) %>%
    mutate(Segment4 = ifelse(Segment4 == "3127", "3120", Segment4)) %>% 
    mutate(Segment5 = ifelse(Segment5 != paste(oldLocation,"00") & grepl("^1|^2|^3|^4", Segment2), "00000", Segment5)) %>%
    mutate(`Segment10` = "") %>%
    mutate(`Segment11` = "") %>%
    mutate(`Segment12` = "") %>%
    mutate(`Segment13` = "") %>%
    mutate(`Segment14` = "") %>%
    mutate(`Segment15` = "") %>%
    mutate(`Segment16` = "") %>%
    mutate(`Segment17` = "") %>%
    mutate(`Segment18` = "") %>%
    mutate(`Segment19` = "") %>%
    mutate(`Segment20` = "") %>%
    mutate(`Segment21` = "") %>%
    mutate(`Segment22` = "") %>%
    mutate(`Segment23` = "") %>%
    mutate(`Segment24` = "") %>%
    mutate(`Segment25` = "") %>%
    mutate(`Segment26` = "") %>%
    mutate(`Segment27` = "") %>%
    mutate(`Segment28` = "") %>%
    mutate(`Segment29` = "") %>%
    mutate(`Segment30` = "") %>%
    mutate(`Budget Amount` = TRANS.AMOUNT*(-1)) %>%
    mutate(`Ledger Name` = "GCS Primary Ledger") %>%
    mutate(`End of CSV` = "End of Line") %>%
    select(`Run Name`,`Status`,`Ledger Id`,`Budget Name`,`Period`,Currency,`Segment1`,`Segment2`,
           `Segment3`,`Segment4`,`Segment5`,`Segment6`,`Segment7`,`Segment8`,`Segment9`,`Segment10`,`Segment11`,
           `Segment12`,`Segment13`,`Segment14`,`Segment15`,`Segment16`,`Segment17`,`Segment18`,`Segment19`,`Segment20`,
           `Segment21`,`Segment22`,`Segment23`,`Segment24`,`Segment25`,`Segment26`,`Segment27`,`Segment28`,`Segment29`,`Segment30`,
           `Budget Amount`,`Ledger Name`,`End of CSV`) %>% 
    filter(as.numeric(substring(Segment2,1,1)) > 1 & as.numeric(substring(Segment2,1,1)) < 5) %>% 
    mutate(Segment5 = ifelse(Segment5 == "86000", "86001", Segment5)) %>%
    #distinct(`Segment1`,`Segment2`, `Segment3`,`Segment4`,`Segment5`,`Segment6`,
    #         `Segment7`,`Segment8`,`Segment9`, `Budget Amount`) %>%
    mutate(Segment5 = "00000") %>%
    mutate(Segment8 = ifelse(Segment3 == "0082", "61000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment3 == "0115", "62000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment3 == "0165", "62000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment3 == "0166", "62000", Segment8)) %>%
    mutate(Segment8 = ifelse(Segment3 == "0178", "70000", Segment8)) %>%
    filter(Segment1 != "30")
  
  
  write.xlsx(GeneralLedgerBudgetBalances, paste0(Name,"-GL-RevenueV",i," ",Sys.Date(),".xlsx"), overwrite = TRUE)
}







