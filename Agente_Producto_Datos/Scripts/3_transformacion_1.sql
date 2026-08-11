
-- Fase 3: Transformación 1 (Cálculos y Homologación)
CREATE OR REPLACE TABLE `datalake2-prototipado.cur_administrativa.work_davibank_transformacion_1`
PARTITION BY periodo AS
SELECT * FROM ( 
  WITH 
  LogInfo AS (
      SELECT COALESCE(MAX(NUM_VECES_REPROCESADO) + 1, 1) AS CodigoReproceso
      FROM `datalake2-prototipado.cur_activo.STG_LOG_PROCESOS_FAH_PRUEBA`
      WHERE FUENTE = 'DAVIBANK' AND PERIODO_EJECUTADO_FAH = @fecha_proceso
  ),
  UnpivotedData AS (
      SELECT *, vlrdeb AS valor_movimiento, '1' AS naturaleza
      FROM `datalake2-prototipado.cur_administrativa.work_davibank_filtrado`
      WHERE periodo = @fecha_proceso AND vlrdeb > 0
      UNION ALL
      SELECT *, vlrcre AS valor_movimiento, '2' AS naturaleza
      FROM `datalake2-prototipado.cur_administrativa.work_davibank_filtrado`
      WHERE periodo = @fecha_proceso AND vlrcre > 0
  ),
  transformacion_base AS (
      SELECT
          t.fecha_efectiva, t.ctacon AS cuenta, 'COP' AS moneda,
          t.moneda_origen_raw AS moneda_origen, t.ctrcto AS dependencia, t.consecutivo,
          'DAVIBANK' AS descripcion,
          ROW_NUMBER() OVER (PARTITION BY LPAD('AK', 2, '0') || FORMAT_DATE('%Y%m%d', t.fecha_efectiva) || FORMAT_DATE('%Y%m%d', t.fecha_posteo) || LPAD(CAST(t.consecutivo AS STRING), 4, '0') || LPAD(CAST(LI.CodigoReproceso AS STRING), 1, '0') ORDER BY t.consecutivo_extraccion, t.naturaleza) AS contador_linea,
          t.valor_movimiento, t.naturaleza, t.fecha_posteo, 'AK' AS tipo_de_comprobante,
          CAST(NULL AS STRING) AS referencia,
          LPAD('AK', 2, '0') || FORMAT_DATE('%Y%m%d', t.fecha_efectiva) || FORMAT_DATE('%Y%m%d', t.fecha_posteo) || LPAD(CAST(t.consecutivo AS STRING), 4, '0') || LPAD(CAST(LI.CodigoReproceso AS STRING), 1, '0') AS codigo_transaccion,
          'DAVIBANK' AS tipo_evento, @libro AS tipo_libro_contable,
          'DAVIBANK' AS origen_transaccion,
          'MOVIMIENTO DAVIBANK' AS desc_origen_transaccion,
          t.periodo
      FROM UnpivotedData AS t
      CROSS JOIN LogInfo AS LI
  )
  SELECT
    t_base.fecha_efectiva, COALESCE(hom.VALOR_SALIDA, CAST(t_base.cuenta AS STRING)) AS cuenta, t_base.moneda,
    t_base.moneda_origen, t_base.dependencia, t_base.consecutivo, t_base.descripcion, t_base.contador_linea,
    t_base.valor_movimiento, t_base.naturaleza, t_base.fecha_posteo, t_base.tipo_de_comprobante,
    t_base.referencia, t_base.codigo_transaccion, t_base.tipo_evento, t_base.tipo_libro_contable,
    t_base.origen_transaccion, t_base.desc_origen_transaccion, t_base.periodo
  FROM transformacion_base AS t_base
  LEFT JOIN `datalake2-prototipado.cur_administrativa.mnl_homologaciones_fah` AS hom
  ON hom.FUENTE = 'DAVIBANK' AND hom.CAMPO_ORIGEN = 'Cuenta' AND hom.VALOR_ORIGEN = CAST(t_base.cuenta AS STRING)
);