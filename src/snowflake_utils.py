import snowflake.connector

def connect_to_snowflake(user, password, account, role, warehouse, database, schema):
    # Conectar a Snowflake
    connection = snowflake.connector.connect(
        user = user,
        password = password,
        account = account,
        role = role,
        warehouse = warehouse,
        database = database,
        schema = schema
    )

    # Crear un cursor para ejecutar comandos en Snowflake
    cursor = connection.cursor()
    
    return connection, cursor

def close_snowflake(connection, cursor):
    # Cerrar la conexión
    cursor.close()
    connection.close()
    