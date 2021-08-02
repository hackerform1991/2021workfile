OPTIONS (skip=1)
LOAD DATA
INFILE "/dw-data/EBS-REPORT-AUD/20190410/csv/20190410_bssheet_2019-03.csv"
append
INTO TABLE EBS_BSSHEET_LINE
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
report_id,
ITEM_NAME,
LINE_NUM,
BEGINNING_BALANCES "to_number(:BEGINNING_BALANCES)",
ENDING_BALANCES "to_number(:ENDING_BALANCES)",
etl_date "sysdate",
period_date "'2019-03'",
data_date "(select data_date from etl_control.ctl_general_parameter)"
)