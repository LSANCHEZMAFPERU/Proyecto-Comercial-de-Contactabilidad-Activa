CREATE OR REPLACE TASK {task_name}
  WAREHOUSE = {warehouse}
  SCHEDULE = 'USING CRON {schedule}'  
AS
  CALL {procedure_name}();