OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-AssetGuaranteeContract.csv"
truncate
INTO TABLE AICS_ASSET_GUARANTEECONTRACT
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  guaranteecontractcode    ,
  guaranteecontractno      ,
  guaranteecontractnum     ,
  guaranteetype            ,
  relationcode             ,
  guaranteeamount          ,
  remainingguaranteeamount ,
  guaranteestartdate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  guaranteeenddate         DATE"YYYY-MM-DD HH24:MI:SS" ,
  guaranteeperiod          ,
  guaranteestatus          ,
  enteruser                ,
  entertime                DATE"YYYY-MM-DD HH24:MI:SS" ,
  lastupdateuser           ,
  lastupdatetime           DATE"YYYY-MM-DD HH24:MI:SS" ,
  operatingstatus          ,
  qualification            ,
  recoverablebalancec      ,
  disposalmoney            ,
  disposalyears            ,
  balancecost              ,
  currentbalance           ,
  deductionten             ,
  lasttimeloss             ,
  lastbalance              ,
  isguaranteetype          ,
  escrowstatus             ,
  cykind                   ,
  limitationexpdate        DATE"YYYY-MM-DD HH24:MI:SS" ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
