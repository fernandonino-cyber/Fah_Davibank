
-- Fase 4: Transformación 2 (Balanceo de Archivos)
CREATE OR REPLACE TABLE `datalake2-prototipado.cur_administrativa.work_davibank_transformacion_2`
PARTITION BY periodo
AS
SELECT * FROM ( 
  WITH
    ConteoTransacciones AS (
      SELECT codigo_transaccion, COUNT(*) AS total_registros
      FROM `datalake2-prototipado.cur_administrativa.work_davibank_transformacion_1`
      WHERE periodo = @fecha_proceso
      GROUP BY codigo_transaccion
    ),
    GruposGrandes AS (
      SELECT codigo_transaccion, DENSE_RANK() OVER (ORDER BY codigo_transaccion) AS grupo_salida
      FROM ConteoTransacciones WHERE total_registros > 150000
    ),
    GruposPequenos AS (
      SELECT
        codigo_transaccion,
        (SELECT COALESCE(MAX(grupo_salida), 0) FROM GruposGrandes) + 
        CAST(FLOOR((SUM(total_registros) OVER (ORDER BY codigo_transaccion) - 1) / 150000) + 1 AS INT64) AS grupo_salida
      FROM ConteoTransacciones WHERE total_registros <= 150000
    ),
    AsignacionFinal AS (
      SELECT codigo_transaccion, grupo_salida FROM GruposGrandes
      UNION ALL
      SELECT codigo_transaccion, grupo_salida FROM GruposPequenos
    )
  SELECT
    stg.*,
    af.grupo_salida
  FROM `datalake2-prototipado.cur_administrativa.work_davibank_transformacion_1` AS stg
  JOIN AsignacionFinal AS af ON stg.codigo_transaccion = af.codigo_transaccion
  WHERE stg.periodo = @fecha_proceso
);
