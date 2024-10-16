WITH 
PRIMEROS_FILTROS_CODI AS(
    SELECT *
    FROM ANALYTICS.ETG_COM_PRI_FIL_DUR
    WHERE Periodo = {mes_actual}
        --AND Codi_SBS = 5487
),

HISTORIA_RCC AS(
    {Historico_Rcc}
),

MEAN_R_L_CRED_24M_CTE AS(
    SELECT Codi_SBS, Cod_Mes,
        AVG(SALDO_TC) AS MEAN_R_L_CRED_24M
    FROM(
        SELECT a.Codi_SBS, a.Cod_Mes, a.fecha,
            SUM(CASE WHEN SUBSTRING(a.Codi_Cuen, 1, 2) = '81' AND SUBSTRING(a.Codi_Cuen, 4, 3) = '923' AND a.Condi <= 1825 THEN (a.Saldo/100) ELSE 0 END) AS SALDO_TC
        FROM HISTORIA_RCC AS a
        WHERE DATE(a.fecha)
            BETWEEN LAST_DAY(DATEADD(month,-24,TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD')))+1 
            AND LAST_DAY(TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD'))
        GROUP BY a.Codi_SBS, a.Cod_Mes, a.fecha, a.Codi_Cuen, a.Condi
    ) AS b
    GROUP BY Codi_SBS, Cod_Mes
),
RU_MEAN_R_TARJ_CRED_9M_CTE AS(
    SELECT Codi_SBS, Cod_Mes, 
        COALESCE(AVG(CONSUMO_TC) / NULLIF(AVG(SALDO_TC), 0), 0) AS RU_MEAN_R_TARJ_CRED_9M
    FROM (
        SELECT a.Codi_SBS, a.Cod_Mes, a.fecha,
            SUM(CASE WHEN SUBSTRING(a.Codi_Cuen, 1, 2) = '81' AND SUBSTRING(a.Codi_Cuen, 4, 3) = '923' AND a.Condi <= 1825 THEN (a.Saldo/100) ELSE 0 END) AS SALDO_TC,
            SUM(CASE WHEN a.Codi_Cuen LIKE '14%' AND SUBSTRING(a.Codi_Cuen, 4, 1) < '7' AND RIGHT(LEFT(a.Codi_Cuen, 8), 2) = '02' AND TRIM(a.Tipo_Cred) IN ('11', '12') AND a.Condi <= 1825 THEN (a.Saldo/100) ELSE 0 END) AS CONSUMO_TC
        FROM HISTORIA_RCC AS a
        WHERE DATE(a.fecha) 
            BETWEEN LAST_DAY(DATEADD(month,-9,TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD')))+1 
            AND LAST_DAY(TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD'))
        GROUP BY a.Codi_SBS, a.Cod_Mes, a.fecha, a.fecha, a.Codi_Cuen, a.Condi
    ) AS b
    GROUP BY Codi_SBS, Cod_Mes
),
MEAN_R_COM_18M_CTE AS(
    SELECT Codi_SBS, Cod_Mes, 
        COALESCE(AVG(CONSUMO_NO_REV), 0) AS MEAN_R_COM_18M
    FROM (
        SELECT a.Codi_SBS, a.Cod_Mes, a.fecha,
            SUM(CASE WHEN a.Codi_Cuen LIKE '14%' AND SUBSTRING(a.Codi_Cuen, 4, 1) < '7' AND RIGHT(LEFT(a.Codi_Cuen, 8), 4) = '0306' AND (SUBSTRING(a.Codi_Cuen, 5, 2) <> '04' AND RIGHT(LEFT(a.Codi_Cuen, 10), 6) != '030602') AND a.Condi <= 1825 THEN (a.Saldo/100) ELSE 0 END) +
            SUM(CASE WHEN a.Codi_Cuen LIKE '14%' AND SUBSTRING(a.Codi_Cuen, 4, 1) < '7' AND RIGHT(LEFT(a.Codi_Cuen, 8), 2) = '02' AND TRIM(a.Tipo_Cred) IN ('11', '12') AND a.Condi <= 1825 THEN (a.Saldo/100) ELSE 0 END) +
            SUM(CASE WHEN a.Codi_Cuen LIKE '14%' AND SUBSTRING(a.Codi_Cuen, 4, 1) < '7' AND SUBSTRING(a.Codi_Cuen, 5, 6) = '030602' THEN (a.Saldo/100) ELSE 0 END) AS CONSUMO_NO_REV
        FROM HISTORIA_RCC AS a 
        WHERE DATE(a.fecha) 
            BETWEEN LAST_DAY(DATEADD(month,-18,TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD')))+1 
            AND LAST_DAY(TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD'))
        GROUP BY a.Codi_SBS, a.Cod_Mes, a.fecha, a.Codi_Cuen, a.Condi, a.Tipo_Cred
    ) AS b
    GROUP BY Codi_SBS, Cod_Mes
),
MEAN_R_N_ENT_D_DIREC_U6M_CTE AS(
    SELECT Codi_SBS, Cod_Mes, 
        AVG(CANT_DEUDA_DIRECTA*1.0) AS MEAN_R_N_ENT_D_DIREC_U6M
    FROM (
        SELECT a.Codi_SBS, a.Cod_Mes, a.fecha,
            CASE WHEN SUM(CASE WHEN (SUBSTRING(a.Codi_Cuen, 1, 2) = '14' AND SUBSTRING(a.Codi_Cuen, 4, 1) < '8') AND a.Condi <= 1825 THEN (a.Saldo/100) ELSE 0 END) > 0 THEN 1 ELSE 0 END AS CANT_DEUDA_DIRECTA
        FROM HISTORIA_RCC AS a 
        WHERE DATE(a.fecha) 
            BETWEEN LAST_DAY(DATEADD(month,-6,TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD')))+1
            AND LAST_DAY(TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD'))
        GROUP BY a.Codi_SBS, a.Cod_Mes, a.fecha, a.Codi_Cuen, a.Condi
    ) AS b
    GROUP BY Codi_SBS, Cod_Mes
),
RATIO_MAX_R_D_DIREC_3M_6M_CTE AS(
    SELECT b.Codi_SBS, b.Cod_Mes,
        COALESCE(MAX(DEUDA_DIRECTA_3) / NULLIF(MAX(DEUDA_DIRECTA_X), 0), 0) AS RATIO_MAX_R_D_DIREC_3M_6M
    FROM(
        SELECT a.Codi_SBS, a.Cod_Mes, a.fecha,
            SUM(CASE WHEN (SUBSTRING(a.Codi_Cuen, 1, 2) = '14' AND SUBSTRING(a.Codi_Cuen, 4, 1) < '8') AND a.Condi <= 1825 THEN a.Saldo / 100 ELSE 0 END) AS DEUDA_DIRECTA_X
        FROM HISTORIA_RCC AS a
        WHERE DATE(a.fecha) 
            BETWEEN LAST_DAY(DATEADD(month,-6,TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD')))+1
            AND LAST_DAY(TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD'))
        GROUP BY a.Codi_SBS, a.Cod_Mes, a.fecha, a.Codi_Cuen, a.Condi
    ) AS b
    INNER JOIN (
        SELECT a.Codi_SBS, a.Cod_Mes, a.fecha,
            SUM(CASE WHEN (SUBSTRING(a.Codi_Cuen, 1, 2) = '14' AND SUBSTRING(a.Codi_Cuen, 4, 1) < '8') AND a.Condi <= 1825 THEN a.Saldo / 100 ELSE 0 END) AS DEUDA_DIRECTA_3
        FROM HISTORIA_RCC AS a
        WHERE DATE(a.fecha)
            BETWEEN LAST_DAY(DATEADD(month,-3,TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD')))+1
            AND LAST_DAY(TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD'))
        GROUP BY a.Codi_SBS, a.Cod_Mes, a.fecha, a.Codi_Cuen, a.Condi
    ) AS c ON b.Codi_SBS = c.Codi_SBS AND b.Cod_Mes = c.Cod_Mes
    GROUP BY b.Codi_SBS, b.Cod_Mes
),
RATIO_MEAN_R_PEOR_CAL_12M_U18M_CTE AS(
    SELECT b.Codi_SBS, b.Cod_Mes,
        COALESCE(AVG(DEUDA_DIRECTA_3*0.1) / NULLIF(AVG(DEUDA_DIRECTA_X*0.1), 0), 0) AS RATIO_MEAN_R_PEOR_CAL_12M_U18M
    FROM (
        SELECT a.Codi_SBS, a.Cod_Mes, a.fecha,
            MAX(CASE WHEN CAST((CASE WHEN a.Condi <= 1825 THEN TRIM(a.Clasi) ELSE 9 END) AS INT) + 1 > 5 THEN 0 ELSE (CAST(TRIM(a.Clasi) AS INT) + 1) END) AS DEUDA_DIRECTA_X
        FROM HISTORIA_RCC AS a 
        WHERE DATE(a.fecha) 
            BETWEEN LAST_DAY(DATEADD(month,-18,TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD')))+1
            AND LAST_DAY(TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD'))
        GROUP BY a.Codi_SBS, a.Cod_Mes, a.fecha, a.Codi_Cuen, a.Condi
    ) AS b
    INNER JOIN (
        SELECT a.Codi_SBS, a.Cod_Mes, a.fecha,
            MAX(CASE WHEN CAST((CASE WHEN a.Condi <= 1825 THEN TRIM(a.Clasi) ELSE 9 END) AS INT) + 1 > 5 THEN 0 ELSE (CAST(TRIM(a.Clasi) AS INT) + 1) END) AS DEUDA_DIRECTA_3
        FROM HISTORIA_RCC AS a 
        WHERE DATE(a.fecha) 
            BETWEEN LAST_DAY(DATEADD(month,-12,TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD')))+1
            AND LAST_DAY(TO_DATE(CONCAT(CAST(a.Cod_Mes AS STRING), '01'),'YYYYMMDD'))
        GROUP BY a.Codi_SBS, a.Cod_Mes, a.fecha, a.Codi_Cuen, a.Condi
    ) AS c ON b.Codi_SBS = c.Codi_SBS AND b.Cod_Mes = c.Cod_Mes
    GROUP BY b.Codi_SBS, b.Cod_Mes
)

SELECT
    {mes_actual} AS Periodo,
    COALESCE(m.Codi_SBS, r.Codi_SBS, c.Codi_SBS, d.Codi_SBS, e.Codi_SBS, f.Codi_SBS) AS Codi_SBS,
    COALESCE(m.Cod_Mes, r.Cod_Mes, c.Cod_Mes, d.Cod_Mes, e.Cod_Mes, f.Cod_Mes) AS Cod_Mes,
    COALESCE(m.MEAN_R_L_CRED_24M, 0) AS MEAN_R_L_CRED_24M,
    COALESCE(r.RU_MEAN_R_TARJ_CRED_9M, 0) AS RU_MEAN_R_TARJ_CRED_9M,
    COALESCE(c.MEAN_R_COM_18M, 0) AS MEAN_R_COM_18M,
    COALESCE(d.MEAN_R_N_ENT_D_DIREC_U6M, 0) AS MEAN_R_N_ENT_D_DIREC_U6M,
    COALESCE(e.RATIO_MAX_R_D_DIREC_3M_6M, 0) AS RATIO_MAX_R_D_DIREC_3M_6M,
    COALESCE(f.RATIO_MEAN_R_PEOR_CAL_12M_U18M, 0) AS RATIO_MEAN_R_PEOR_CAL_12M_U18M
FROM MEAN_R_L_CRED_24M_CTE m
INNER JOIN RU_MEAN_R_TARJ_CRED_9M_CTE r ON m.Codi_SBS = r.Codi_SBS AND m.Cod_Mes = r.Cod_Mes
INNER JOIN MEAN_R_COM_18M_CTE c ON m.Codi_SBS = c.Codi_SBS AND m.Cod_Mes = c.Cod_Mes
INNER JOIN MEAN_R_N_ENT_D_DIREC_U6M_CTE d ON m.Codi_SBS = d.Codi_SBS AND m.Cod_Mes = d.Cod_Mes
INNER JOIN RATIO_MAX_R_D_DIREC_3M_6M_CTE e ON m.Codi_SBS = e.Codi_SBS AND m.Cod_Mes = e.Cod_Mes
INNER JOIN RATIO_MEAN_R_PEOR_CAL_12M_U18M_CTE f ON m.Codi_SBS = f.Codi_SBS AND m.Cod_Mes = f.Cod_Mes