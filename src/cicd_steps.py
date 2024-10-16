import os

import src.tools as TOOLS

# FUNCIÓN PARA SUBIR LOS ARCHIVOS DESEADOS A UN STAGE DE SNOWFLAKE
def upload_to_stage(cursor, files_directories, stage_name):
    # Verificar si el Stage ya existe
    cursor.execute(f"SHOW STAGES LIKE '{stage_name}';")
    stage_exists = cursor.fetchone()

    # Si no existe, crear el Stage
    if not stage_exists:
        print(f"Stage '{stage_name}' no existe. Creándolo ahora...")
        cursor.execute(f"CREATE OR REPLACE STAGE {stage_name};")
        print(f"Stage '{stage_name}' ha sido creado.")
    else:
        print(f"Stage '{stage_name}' ya existe.")

    # Obtener la lista de archivos en el Stage
    cursor.execute(f"LIST @{stage_name}")
    files_in_stage = [row[0] for row in cursor.fetchall()]  # Nombre de archivos en el Stage

    # Cargar y verificar cada archivo
    for directory in files_directories:
        for file_name in os.listdir(directory):
            file_path = os.path.join(directory, file_name)
            
            # Si el archivo ya existe en el Stage
            if f"@{stage_name}/{file_name}" in files_in_stage:
                # Obtener el hash local del archivo
                local_file_md5 = TOOLS.calculate_md5(file_path)

                # Verificar el hash del archivo en el Stage (descargar temporalmente para comparar)
                tmp_file_path = f"/tmp/{file_name}"
                cursor.execute(f"GET @{stage_name}/{file_name} file://{tmp_file_path}")
                stage_file_md5 = TOOLS.calculate_md5(file_name)

                if local_file_md5 == stage_file_md5:
                    print(f"{file_name} ya existe y es idéntico. No se vuelve a cargar.")
                else:
                    # El archivo ha cambiado, por lo que lo volvemos a cargar
                    cursor.execute(f"PUT file://{file_path} @{stage_name} OVERWRITE = TRUE")
                    print(f"{file_name} ha sido actualizado en el Stage.")
                    
                # Eliminar el archivo temporal después de compararlo
                if os.path.exists(tmp_file_path):
                    os.remove(tmp_file_path)
                    print(f"Archivo temporal '{tmp_file_path}' eliminado.")
                    
            else:
                # El archivo no existe en el Stage, lo subimos
                cursor.execute(f"PUT file://{file_path} @{stage_name}")
                print(f"{file_name} ha sido subido al Stage.")
        
def get_string_script(cursor, stage_name, file_name):
    tmp_file_path = f"/tmp/{file_name}"   
    
    # Descargar el archivo de pipeline desde el Stage a una ruta local temporal
    cursor.execute(f"GET {stage_name}/{file_name} file://{tmp_file_path}")  
    
    # Leer el archivo SQL descargado como texto
    with open(tmp_file_path, 'r') as file:
        file_script = file.read() 
        
    # Eliminar el archivo temporal después de compararlo
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)
        print(f"Archivo temporal '{tmp_file_path}' eliminado.")
        
    return file_script

def build_procedure_script(cursor, stage_name, procedure_name, procedure_file, procedure_packages, pipeline_file, query_files, query_id):
    pipeline_script = get_string_script(cursor, stage_name, pipeline_file)
    query_scripts = [get_string_script(cursor, stage_name, query_file) for query_file in query_files]

    for i in range(0,len(query_scripts)):
        pipeline_script = pipeline_script.replace(
        "{" + query_id + "[" + str(i) + "]}", 
        query_scripts[i]
    )

    procedure_packages_str = "(" + ", ".join(["'" + package + "'" for package in procedure_packages]) + ")"

    procedure_script = get_string_script(cursor, stage_name, procedure_file)

    procedure_script = procedure_script.replace("{procedure_name}", procedure_name)
    procedure_script = procedure_script.replace("{procedure_packages}", procedure_packages_str)
    procedure_script = procedure_script.replace("{python_script}", pipeline_script)
    
    return procedure_script

def build_task_script(cursor, warehouse, stage_name, procedure_name, task_name, task_file, schedule):
    task_script = get_string_script(cursor, stage_name, task_file)

    task_script = task_script.replace("{task_name}", task_name)
    task_script = task_script.replace("{warehouse}", warehouse)
    task_script = task_script.replace("{schedule}", schedule)
    task_script = task_script.replace("{procedure_name}", procedure_name)
    
    return task_script

def update_procedure(cursor, schema, procedure_name, procedure_script):
    # Verificar si el Procedure ya existe
    check_procedure = f"""
        SELECT *
        FROM INFORMATION_SCHEMA.ROUTINES
        WHERE ROUTINE_NAME = '{procedure_name}'
        AND ROUTINE_TYPE = 'PROCEDURE'
        AND SPECIFIC_SCHEMA = '{schema}';
    """

    cursor.execute(check_procedure)
    proc_exists = cursor.fetchone()

    if proc_exists:
        print(f"La Stored Procedure '{procedure_name}' ya existe.")

        # Comparar el código actual con el nuevo código
        current_definition = proc_exists[0]

        if current_definition.strip() == procedure_script.strip():
            print("No hay cambios en la definición. La Stored Procedure queda igual.")
        else:
            print("La Stored Procedure ha cambiado. Actualizando...")
            cursor.execute(procedure_script)
            print("Stored Procedure actualizada.")
    else:
        print(f"La Stored Procedure '{procedure_name}' no existe. Creando una nueva...")
        cursor.execute(procedure_script)
        print("Stored Procedure creada.")
        
def update_task(cursor, schema, task_name, task_script):
    # Verificar si el Task ya existe
    check_task = f"""
        SELECT DEFINITION
        FROM INFORMATION_SCHEMA.TASKS
        WHERE NAME = '{task_name}'
        AND SCHEMA_NAME = '{schema}';
    """

    cursor.execute(check_task)
    task_exists = cursor.fetchone()

    if task_exists:
        print(f"El Task '{task_name}' ya existe.")

        # Comparar el código actual con el nuevo código
        current_definition = task_exists[0]

        if current_definition.strip() == task_script.strip():
            print("No hay cambios en la definición. El Task queda igual.")
        else:
            print("El Task ha cambiado. Actualizando...")
            cursor.execute(task_script)
            print("Task actualizada.")
    else:
        print(f"El Task '{task_name}' no existe. Creando una nueva...")
        cursor.execute(task_script)
        print("Stored Procedure creada.")