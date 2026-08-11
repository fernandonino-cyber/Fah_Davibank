
-- Fase 2: Filtrado
CREATE OR REPLACE TABLE `datalake2-prototipado.cur_administrativa.work_davibank_filtrado`
PARTITION BY periodo AS
SELECT
    FECHA_EFECTIVA AS fecha_efectiva,
    FECHA_POSTEO AS fecha_posteo,
    CTACON AS ctacon,
    MONEDA AS moneda_origen_raw,
    CTRCTO AS ctrcto,
    CONSECUTIVO AS consecutivo,
    DESCRIPCION AS descripcion_origen,
    VLRDEB AS vlrdeb,
    VLRCRE AS vlrcre,
    PERIODO AS periodo,
    CONSECUTIVO_EXTRACCION AS consecutivo_extraccion
FROM `datalake2-prototipado.cur_administrativa.trf_davibank_fah`
WHERE PERIODO = @fecha_proceso;
