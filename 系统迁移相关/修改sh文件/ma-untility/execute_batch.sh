# This program calls sys_req_process_pck.execute_program.
# It will execute a current program registered in MA System
#  $1 - User ID who makes the call
#  $2 - Batch RID        

#! /bin/bash
. ~/.profile

# CALL DB PROCEDURE  
sqlplus maadmin/maadmin<<EOF
begin
  sys_req_process_pck.exec_batch('$1', $2);
end;
/
EOF
