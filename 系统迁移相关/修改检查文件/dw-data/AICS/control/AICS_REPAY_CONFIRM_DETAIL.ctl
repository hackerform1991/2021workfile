OPTIONS (skip=1)
LOAD DATA
INFILE "/dw-data/AICS/20190423/csv/20190423-RepayConfirmDetail.csv"
truncate
INTO TABLE AICS_REPAY_CONFIRM_DETAIL
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  virtual_colum FILLER,
  id                 ,
  confirmid            ,
  type                 ,
  obj_id               ,
  loan_amount          ,
  loan_cost            ,
  repay_amount         ,
  amount               ,
  debts                ,
  net_repay            ,
  special_remark       ,
  isfinished           ,
  penalty              ,
  remain_penalty       ,
  overdue_fee          ,
  creditorcontractcode ,
  compoundinterest     ,
  pre_compoundinterest ,
  out_income           ,
  out_tax              ,
  create_date          DATE"YYYY-MM-DD HH24:MI:SS",
  current_loan_amount  ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
