OPTIONS (skip=0)
LOAD DATA
INFILE "/dw-data/AICS/20190519/csv/20190519-AssetMortgageContract.csv"
truncate
INTO TABLE AICS_ASSET_MORTGAGECONTRACT
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(

   mortgagecontractcode ,
  mortgagecontractno  ,
  mortgagetype         ,
  mortgagestartdate     DATE"YYYY-MM-DD HH24:MI:SS" ,
  mortgageenddate       DATE"YYYY-MM-DD HH24:MI:SS" ,
  mortgagorcode       ,
  mortgagemaxmoney    ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
