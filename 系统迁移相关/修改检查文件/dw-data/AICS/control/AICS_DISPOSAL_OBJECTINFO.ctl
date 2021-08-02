OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190416/csv/20190416-DisposalObjectinfo.csv"
truncate
INTO TABLE AICS_DISPOSAL_OBJECTINFO
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  relationcode      ,
  projectcode       ,
  storeprojectcode  ,
  objectcode        ,
  objecttype        ,
  memo              char(3000),
  enteruser         ,
  entertime         DATE"YYYY-MM-DD HH24:MI:SS" ,
  lastupdateuser    ,
  lastupdatetime    DATE"YYYY-MM-DD HH24:MI:SS" ,
  isallrelated      ,
  isvalid           ,
  initstatus        ,
  objectname        char(500),
  corpusamount      ,
  interest          ,
  appraisementvalue ,
  overcost          ,
  isneedshow        ,
  contractid        ,
  mindealprice      ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
