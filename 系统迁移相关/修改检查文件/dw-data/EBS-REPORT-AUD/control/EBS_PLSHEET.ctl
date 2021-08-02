OPTIONS (skip=1)
LOAD DATA
INFILE "/dw-data/EBS-REPORT-AUD/20190410/csv/20190410_plsheet_2019-03.csv"
append
INTO TABLE EBS_PLSHEET_LINE
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
report_id,
ITEM_NAME,
org_code,
dept_code,
LINE_NUM,
dept_ptd,
dept_ytd,
etl_date "sysdate",
period_date "'2019-03'",
data_date "(select data_date from etl_control.ctl_general_parameter)"
)
