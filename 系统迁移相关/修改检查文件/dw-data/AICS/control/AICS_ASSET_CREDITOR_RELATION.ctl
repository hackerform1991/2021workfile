OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-AssetCreditorRelation.csv"
truncate
INTO TABLE AICS_ASSET_CREDITOR_RELATION
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
   relationcode        ,
  relationtype          ,
  creditorcontractcode  ,
  guaranteecontractcode ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
