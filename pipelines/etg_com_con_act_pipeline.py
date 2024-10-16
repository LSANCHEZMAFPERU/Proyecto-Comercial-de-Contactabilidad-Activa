import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from datetime import datetime
from dateutil.relativedelta import relativedelta

def main(session: snowpark.Session): 
    
    def tabla_existe(session, table_name):
        result = session.sql(f"SHOW TABLES LIKE '{table_name}'").collect()
        return len(result) > 0
        
    meses_anteriores_general = 1
    meses_anteriores_rcc = 2 + meses_anteriores_general
    meses_anteriores_car_maf = 1 + meses_anteriores_general
    num_meses_actuales_rcc = 3
    num_meses_historico_sbs = 24
    
    ahora = datetime.now()
    fec_actual = ahora - relativedelta(months=meses_anteriores_general)
    fec_rcc = ahora - relativedelta(months=meses_anteriores_rcc)
    fecs_rcc = [ahora - relativedelta(months=(meses_anteriores_rcc + i)) for i in range(num_meses_actuales_rcc)]
    fec_car_maf = ahora - relativedelta(months=meses_anteriores_car_maf)
    fecs_sbs = [ahora - relativedelta(months=meses_anteriores_rcc + i) for i in range(num_meses_historico_sbs)]
    
    mes_actual = fec_actual.strftime('%Y%m')
    anio_actual = fec_actual.strftime('%Y')
    mes_rcc = fec_rcc.strftime('%Y%m')
    meses_rcc = [fec.strftime('%Y%m') for fec in fecs_rcc]
    mes_car_maf = fec_car_maf.strftime('%Y%m')
    meses_sbs = [fec.strftime('%Y%m') for fec in fecs_sbs]
    
    tabla_rcc = f'RCC.RCC_BCO_{mes_rcc}'
    tablas_rcc = [f'RCC.RCC_BCO_{mes}' for mes in meses_rcc]
    tablas_rcc_sbs = [f'RCC.CLIENTE_SALDO_SBS_{mes}' for mes in meses_rcc]
    tabla_anio_nac = f'NEGOCIO.CLI_EXP'
    tabla_base_neg = f'NEGOCIO.CLI_NEG'
    tabla_car_maf = f'NEGOCIO.CAR_MAF'
    tabla_zona_peli = f'NEGOCIO.CLI_ZNA_EMG'
    tablas_sbs = [f'RCC.CLIENTE_SALDO_SBS_{mes}' for mes in meses_sbs]
    
    Primeros_Filtros_Duros = f"""
        {pipeline_query[0]}
    """
    
    Historico_Rcc = " UNION ALL ".join([
        f"""
            {pipeline_query[1]}
        """
        for tabla in tablas_sbs
    ])

    Variables_Score = f"""
        {pipeline_query[2]}
    """
    
    table_primeros_filtros_duros = "ANALYTICS.ETG_COM_PRI_FIL_DUR"
    table_variables_score = "ANALYTICS.ETG_COM_VAR_SCR"
    
    # Crear o insertar en la tabla según si existe o no
    if tabla_existe(session, table_primeros_filtros_duros):
        # Insertar si la tabla ya existe
        dataframe = session.sql(f"INSERT INTO {table_primeros_filtros_duros} {Primeros_Filtros_Duros}").collect()
        print(f"Datos insertados en la tabla {table_primeros_filtros_duros}.")
    else:
        # Crear la tabla si no existe
        dataframe = session.sql(f"CREATE TABLE {table_primeros_filtros_duros} AS {Primeros_Filtros_Duros}").collect()
        print(f"Tabla {table_primeros_filtros_duros} creada y datos insertados.")

    #print("PRIMEROS FILTROS COMPLETADO")

    # Crear o insertar en la tabla según si existe o no
    if tabla_existe(session, table_variables_score):
        # Insertar si la tabla ya existe
        dataframe = session.sql(f"INSERT INTO {table_variables_score} {Variables_Score}").collect()
        print(f"Datos insertados en la tabla {table_variables_score}.")
    else:
        # Crear la tabla si no existe
        dataframe = session.sql(f"CREATE TABLE {table_variables_score} AS {Variables_Score}").collect()
        print(f"Tabla {table_variables_score} creada y datos insertados.")

    print("VARIABLES SCORE COMPLETADO")
    
    return dataframe