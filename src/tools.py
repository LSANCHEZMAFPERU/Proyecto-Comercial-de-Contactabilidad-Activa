import os
import sys
from hashlib import md5

# Función para calcular el hash MD5 de un archivo
def calculate_md5(file_path):
    hash_md5 = md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# Función para obtener la raíz del proyecto o entorno de ejecución
def get_project_root(): 
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.append(project_root)
    return project_root
