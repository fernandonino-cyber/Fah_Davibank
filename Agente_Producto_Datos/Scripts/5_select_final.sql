
-- Fase 5: Select Final
CREATE OR REPLACE TABLE `datalake2-prototipado.cur_administrativa.DAVIBANK_FAH`
PARTITION BY periodo
AS
SELECT * FROM ( 
  SELECT
      fecha_efectiva,
      cuenta,
      moneda,
      moneda_origen,
      dependencia,
      consecutivo,
      descripcion,
      contador_linea,
      valor_movimiento,
      naturaleza,
      fecha_posteo,
      tipo_de_comprobante,
      referencia,
      codigo_transaccion,
      tipo_evento,
      tipo_libro_contable,
      origen_transaccion,
      desc_origen_transaccion,
      grupo_salida,
      periodo
  FROM `datalake2-prototipado.cur_administrativa.work_davibank_transformacion_2`
  WHERE periodo = @fecha_proceso
);
