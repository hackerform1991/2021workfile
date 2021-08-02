OPTIONS (load=1)
LOAD DATA
INFILE "/dw-data/EBS-REPORT/20190116/csv/20190116_plsheet_2018-12.csv"
append
INTO TABLE EBS_PLSHEET_HEADER
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
