OPTIONS (skip=1)
LOAD DATA
INFILE "/dw-data/AICS/20190423/csv/20190423-CreditorVaryRelation.csv"
truncate
INTO TABLE AICS_CREDITOR_VARY_RELATION
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  relationid           ,
  ijourid              ,
  creditorcontractcode ,
  ofeesetcorpusamount  ,
  ofeesetinterest      ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
