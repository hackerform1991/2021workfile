OPTIONS (skip=1)
LOAD DATA
INFILE "/dw-data/AICS/20190423/csv/20190423-RepayConfirm.csv"
truncate
INTO TABLE AICS_REPAY_CONFIRM
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
  virtual_colum FILLER,
  confirmid           ,
  approveid           ,
  arrivalid           ,
  projectcode         ,
  repay_type          ,
  end_data_date            DATE"YYYY-MM-DD HH24:MI:SS",
  liquidate_unit      ,
  liquidate_type      ,
  pay_type            ,
  plan_name           ,
  isloan              ,
  source              ,
  source_code         ,
  service_free        ,
  bail_type           ,
  payer_name          ,
  contract_type       ,
  contract_id         ,
  delist              ,
  isbailtofee         ,
  distributionorder   ,
  status              ,
  userid              ,
  orgid               ,
  confirm_date        DATE"YYYY-MM-DD HH24:MI:SS",
  process_instanceid  ,
  isdirect            ,
  repay_nature        ,
  out_tax             ,
  summarymoney        ,
  service_free2       ,
  amount              ,
  inputtype           ,
  vouchers_state      ,
  incometax_type      ,
  associated_asset_id ,
  debt_settlement     ,
  approve_no          ,
datadate  "(select data_date from etl_control.ctl_general_parameter)",
etl_date "sysdate "
)
