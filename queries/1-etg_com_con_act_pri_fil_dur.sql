WITH
FIL_SALDOS AS(
    SELECT 
        SBS0.Codi_SBS
    FROM {tablas_rcc_sbs[0]} AS SBS0
    LEFT JOIN {tablas_rcc_sbs[1]} AS SBS1
        ON SBS0.Codi_SBS = SBS1.Codi_SBS
    LEFT JOIN {tablas_rcc_sbs[2]} AS SBS2
        ON SBS0.Codi_SBS = SBS2.Codi_SBS
    GROUP BY SBS0.Codi_SBS
    HAVING 
        MAX(CASE 
            WHEN (SUBSTRING(SBS0.Codi_Cuen,1,2)||'0'||SUBSTRING(SBS0.Codi_Cuen,4,1)) IN ('1405', '1406', '1404', '8103')
            THEN SBS0.Saldo/100
            ELSE 0
        END) <= 500
        AND    
        MAX(CASE 
            WHEN (SUBSTRING(SBS1.Codi_Cuen,1,2)||'0'||SUBSTRING(SBS1.Codi_Cuen,4,1)) IN ('1406', '1404', '8103')
            THEN SBS1.Saldo/100
            ELSE 0
        END) <= 500
        AND
        MAX(CASE 
            WHEN (SUBSTRING(SBS2.Codi_Cuen,1,2)||'0'||SUBSTRING(SBS2.Codi_Cuen,4,1)) IN ('1406', '1404', '8103')
            THEN SBS2.Saldo/100
            ELSE 0
        END) <= 500
),

--FIL_SALDOS AS(
--    SELECT RCC0.*
--    FROM {tablas_rcc[0]} AS RCC0
--    LEFT JOIN {tablas_rcc[1]} AS RCC1
--        ON RCC0.Num_Doc_Cli = RCC1.Num_Doc_Cli
--    LEFT JOIN {tablas_rcc[2]} AS RCC2
--        ON RCC0.Num_Doc_Cli = RCC2.Num_Doc_Cli
--    WHERE RCC0.Sld_Ven <= 500 AND RCC0.Sld_Cas <= 500 AND RCC0.Sld_Jud <= 500 AND RCC0.Sld_Ref <= 500
--        AND RCC1.Sld_Cas <= 500 AND RCC1.Sld_Jud <= 500 AND RCC1.Sld_Ref <= 500 
--        AND RCC.Sld_Cas <= 500 AND RCC2.Sld_Jud <= 500 AND RCC2.Sld_Ref <= 500
--),

CRUCE_RCC_SBS AS(
    SELECT RCC.Cod_Cli_Sbs, RCC.Num_Doc_Cli
    FROM {tabla_rcc} AS RCC
    INNER JOIN FIL_SALDOS AS SBS
        ON RCC.Cod_Cli_Sbs = SBS.Codi_SBS
    GROUP BY RCC.Cod_Cli_Sbs, RCC.Num_Doc_Cli
),

CRUCE_ANIO_NAC AS(
    SELECT 
        RCC.Cod_Cli_Sbs, RCC.Num_Doc_Cli,
        ({anio_actual} - EXP.Anio_Nac) AS Edad 
    FROM CRUCE_RCC_SBS AS RCC
    INNER JOIN {tabla_anio_nac} AS EXP
        ON RCC.Num_Doc_Cli = EXP.Num_Doc_Cli
),

FIL_EDAD_FIL_NEG AS(
    SELECT NAC.*
    FROM CRUCE_ANIO_NAC AS NAC
    LEFT JOIN {tabla_base_neg} AS NEG 
        ON NAC.Num_Doc_Cli = NEG.Num_Doc_Cli
    WHERE Edad >= 21 AND Edad <= 75
        AND NEG.Num_Doc_Cli IS NULL 
),

PRE_FIL_CAR_MAF AS(
    SELECT CAR.Num_Doc_Cli
    FROM {tabla_car_maf} AS CAR
    WHERE Cod_Mes = {mes_car_maf} AND Num_Cuo_Pag = 0
    GROUP BY CAR.Num_Doc_Cli
),

FIL_NUM_CUOTA AS(
    SELECT NEG.*
    FROM FIL_EDAD_FIL_NEG AS NEG
    LEFT JOIN PRE_FIL_CAR_MAF AS CAR 
        ON NEG.Num_Doc_Cli = CAR.Num_Doc_Cli
    WHERE CAR.Num_Doc_Cli IS NULL
),

FIL_ZONA_PELIGRO AS(
    SELECT CUO.*
    FROM FIL_NUM_CUOTA AS CUO
    LEFT JOIN {tabla_zona_peli} AS ZON 
        ON CUO.Num_Doc_Cli = ZON.Num_Doc_Cli
    WHERE ZON.Num_Doc_Cli IS NULL
)

SELECT {mes_actual} AS Periodo, {mes_rcc} AS Cod_Mes, SBS.*, PRI.Num_Doc_Cli
FROM {tablas_sbs[0]} AS SBS
LEFT JOIN FIL_ZONA_PELIGRO AS PRI
    ON SBS.Codi_SBS = PRI.Cod_Cli_Sbs
WHERE PRI.Num_Doc_Cli IS NOT NULL