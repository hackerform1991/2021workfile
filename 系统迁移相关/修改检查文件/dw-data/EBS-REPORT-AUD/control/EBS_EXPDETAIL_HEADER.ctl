OPTIONS (load=1)
LOAD DATA
INFILE "/dw-data/EBS-REPORT-AUD/20190410/csv/20190410_expdetail_2019-03.csv"
append
INTO TABLE EBS_EXPDETAIL_HEADER
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
report_id,
report_flag,
period_date,
data_date DATE"YYYYMMDD",
com_code,
dept_code,
line_count_num "to_number(:line_count_num)",
etl_date "sysdate"
)
