OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-AssetCreditorContract.csv"
truncate
INTO TABLE AICS_ASSET_CREDITORCONTRACT
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
 creditorcontractcode   ,
  creditorcontractnum   ,
  loanano               ,
  loanaccountcode       ,
  headbank              ,
  branchbank            ,
  contractstartdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  contractenddate         DATE"YYYY-MM-DD HH24:MI:SS" ,
  corpusamount           ,
  interest               ,
  defaultinterest        ,
  compoundinterest       ,
  iscalcinterest         ,
  contractsigndate        DATE"YYYY-MM-DD HH24:MI:SS" ,
  isbantransfer          ,
  lawdeaddate             DATE"YYYY-MM-DD HH24:MI:SS" ,
  creditorcontractphase  ,
  creditorcontractstatus ,
  rejecttype             ,
  tempcode               ,
  enteruser              ,
  entertime              DATE"YYYY-MM-DD HH24:MI:SS" ,
  lastupdateuser         ,
  lastupdatetime         DATE"YYYY-MM-DD HH24:MI:SS" ,
  credittype             ,
  cykind                 ,
  propertynature         ,
  escrowstatus           ,
  contractloandate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  contractnum            ,
  projectcode            ,
  contractid             ,
  creditorassettype      ,
  creditorassetcode      ,
  creditor_cost          ,
          

datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
