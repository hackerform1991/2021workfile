OPTIONS (load=1)
LOAD DATA
INFILE "/dw-data/AICS/testcsv/tprojectinfo.csv"
append
INTO TABLE AICS_DATA_HEADER
fields terminated by "^*"
Optionally enclosed by '"'
trailing nullcols
(
virtual_colum FILLER,
report_id,
report_flag,
period_date,
data_date DATE"YYYYMMDD",
line_count_num "to_number(:line_count_num)",
etl_date "sysdate"
)
