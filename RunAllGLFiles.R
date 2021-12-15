
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
source('renameFunction.R')

as400 <- dbConnect(odbc::odbc(), "Sunpac", uid = Sys.getenv("as_400_user"), 
                   pwd = Sys.getenv("as_400_password"))

Ledger2 <- renameFunction("LEDGER", "SFDATA2")

for (i in 7:11){
  i <- str_pad(i,2,side = c("left"),pad="0")
  Ledger_Filter <- Ledger2 %>%
    filter(TRANS.DATE %like% paste0("2021",i)) %>% 
    filter(LEDGER.TYPE == "T") %>%
    mutate(Date = gsub("-","/",ymd(TRANS.DATE))) %>%
    mutate(Transactions = TRANS.AMOUNT) %>%
    mutate(ACCOUNT.NUMBER = trimws(ACCOUNT.NUMBER)) %>%
    filter(!is.na(`Transactions`) | trimws(`Transactions`) == "-") %>% 
    mutate(`Entered Debit Amount` = ifelse(sign(as.numeric(Transactions)) == 1, as.numeric(Transactions), 0)) %>%
    mutate(`Entered Credit Amount` = ifelse(sign(as.numeric(Transactions)) == -1, as.numeric(Transactions) * (-1), 0))
  
  CreateGLBalance(Ledger_Filter, paste0("2021-",i))
  
  BB <- Budgets %>% 
    filter(TRANS.DATE %like% paste0("2021",i)) %>% 
    filter(TRANS.TYPE == "BB" | TRANS.TYPE == "BT" | TRANS.TYPE == "BA")
  CreateControlBudger(BB, paste0(i,"-21"), "1")
  CreateGLBudget(BB, paste0(i,"-21"), "1")
}