# This program calls sys_req_process_pck.execute_program.
# It will execute a current program registered in MA System
#  $1 - User ID who makes the call
#  $2 - Call ID for the current Program
#  $3 - Current Program ID registered in MA System
#! /bin/bash
. ~/.profile

# CALL DB PROCEDURE  
sqlplus maadmin/maadmin<<EOF
begin
  sys_req_process_pck.execute_program('$1', '$2', '$3');
end;
/
EOF
