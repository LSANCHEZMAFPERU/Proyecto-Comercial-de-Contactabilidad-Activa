import os

import src.tools as TOOLS
import src.snowflake_utils as SNW
import src.cicd_steps as CICD

# Obtener ruta raíz del entorno de ejecución
project_root = TOOLS.get_project_root()

# Conectar a Snowflake
conn, cur = SNW.connect_to_snowflake(
    user = os.getenv('SNOWFLAKE_USER'),
    password = os.getenv('SNOWFLAKE_PASSWORD'),
    account = os.getenv('SNOWFLAKE_ACCOUNT'),
    role = os.getenv('SNOWFLAKE_ROLE'),
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
    database = os.getenv('SNOWFLAKE_DATABASE'),
    schema = os.getenv('SNOWFLAKE_SCHEMA')
)

# RUTINA TEMPORAL
schedule = "0 0 28 * * UTC"

# Nombre de Stage, Procedure, Task en SNOWFLAKE
stage_name = "ETG_COM_CON_ACT_STAGE"

procedure_name = "ETG_COM_CON_ACT_PROCEDURE"

procedure_packages = [
    "snowflake-snowpark-python",
    "python-dateutil"
]

task_name = "ETG_COM_CON_ACT_TASK"

# Nombre de archivos de script
pipeline_file = 'etg_com_con_act_pipeline.py'

query_files = [
    '1-etg_com_con_act_pri_fil_dur.sql',
    '1.5-etg_com_con_act_his.sql',
    '2-etg_com_con_act_var_scr.sql'
]

query_id = 'pipeline_query' # Identificador de los queries en el pipeline

procedure_file = 'etg_com_con_act_procedure.sql'

task_file = 'etg_com_con_act_task.sql'

# Ruta de los archivos a subir al Stage en tu repositorio local
files_directories = ['queries', 'pipelines', 'procedures', 'tasks']
files_directories = [project_root + "/" + folder for folder in files_directories]

# Subir todos los archivos necesarios en Stage
CICD.upload_to_stage(cur, files_directories, stage_name)

# Obtener Script Del Procedure con el Pipeline con todos sus queries
procedure_script = CICD.build_procedure_script(cur, stage_name, procedure_name, procedure_file, procedure_packages, pipeline_file, query_files, query_id)

# Crear o actualizar PROCEDURE
CICD.update_procedure(cur, os.getenv('SNOWFLAKE_SCHEMA'), procedure_name, procedure_script)

# Obtener Script del task
task_script = CICD.build_task_script(cur, os.getenv('SNOWFLAKE_WAREHOUSE'), stage_name, procedure_name, task_name, task_file, schedule)

# Crear o actualizar TASK y Activar
CICD.update_task(cur, os.getenv('SNOWFLAKE_SCHEMA'), task_name, task_script)

# Cerrar la conexión
SNW.close_snowflake(conn, cur)