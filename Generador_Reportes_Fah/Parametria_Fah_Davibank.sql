/* ---SQL_MASTER--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report`
WHERE ID_PRODUCTO_DATOS = 19
  AND NOMBRE_TABLA = 'DAVIBANK_FAH';


-- METADATA
INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report` (
    ID_PRODUCTO_DATOS,
    ID_TIPO_PROCESO,
    NOMBRE_PRODUCTO_DATOS,
    TAG_NEGOCIO,
    DATASET_DESTINO_MST,
    NOMBRE_TABLA,
    CATEGORIA_VOLUMETRIA,
    QUERY,
    AUTOMATIZACION,
    PERIODICIDAD,
    FLAG_ACTIVO,
    FLAG_NOTIFICACION,
    CORREO_NOTIFICACION,
    ASUNTO,
    CUERPO,
    FLG_ADJUNTOS,
    FECHA_REGISTRO
  )
VALUES
  (
    19,
    1,
    'FAH',
    '{"formato": "ARCHIVO_METADATA_DAVIBANK", "entidad": "ARCHIVO"}',
    'cur_administrativa',
    'DAVIBANK_FAH',
    'BAJA',
    """SELECT
    1 AS ORDEN,
    CONCAT('Metadata version number : ', CAST('3' AS STRING)) AS file_content_string,
    grupos.GRUPO_SALIDA
	FROM (
    SELECT DISTINCT GRUPO_SALIDA
    FROM `datalake2-prototipado.cur_administrativa.DAVIBANK_FAH`
    WHERE PERIODO = '$YYYYMMDD'
      AND GRUPO_SALIDA IS NOT NULL
	) grupos

UNION ALL

	SELECT
    2 AS ORDEN,
    CONCAT('Application Short Name : ', CAST('DAVIBANK' AS STRING)) AS file_content_string,
    grupos.GRUPO_SALIDA
	FROM (
    SELECT DISTINCT GRUPO_SALIDA
    FROM `datalake2-prototipado.cur_administrativa.DAVIBANK_FAH`
    WHERE PERIODO = '$YYYYMMDD'
      AND GRUPO_SALIDA IS NOT NULL
	) grupos

ORDER BY GRUPO_SALIDA, ORDEN""",
    NULL,
    'Diaria',
    TRUE,
    FALSE,
    NULL,
    NULL,
    NULL,
    FALSE,
    CURRENT_DATE()
  );

/* ---SQL_DOC--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte`
WHERE
  ID_PRODUCTO_DATOS = 19
  AND NOMBRE_REPORTE = 'ARCHIVO_METADATA_DAVIBANK';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte` (
    ID_PRODUCTO_DATOS,
    NOMBRE_PRODUCTO_DATOS,
    ID_REPORTE,
    NOMBRE_REPORTE,
    TIPO_SALIDA,
    TIPO_EJECUCION,
    SCRIPT_PATH,
    FILTRO,
    ENCODING,
    FORMATO_NOMBRE_ARCHIVO,
    EXTENSION,
    DELIMITADOR,
    FLAG_ENCABEZADO,
    RUTA_DESTINO,
    FORMATO_FECHA,
    N_REGISTROS_X_ARCHIVO,
    FLAG_COMPLETITUD_INFORMACION,
    FLAG_CONSECUTIVO,
    CONSECUTIVO_INICIO,
    CONSECUTIVO_FIN,
    FLAG_ACTIVO,
    FECHA_REGISTRO
  )
VALUES
  (
    19,
    'FAH',
    'RPT-DAVIBANK-ARCHIVO-V1',
    'ARCHIVO_METADATA_DAVIBANK',
    'Archivo',
    'SQL',
    NULL,
    NULL,
    'ISO-8859-1',
    'Metadata_DAVIBANK_{FORMATO_FECHA}_{CONTADOR}_de_{TOTAL_ARCHIVOS}_{VAR_REPROCESO}.{EXTENCION}',
    'txt',
    ',',
    FALSE,
    'gs://par_dav_proto_configuraciones/8096_tec_planeacion_y_riesgo/Woombat/codigo_fuente/Servicio_de_entrega_de_datos/Generador_Reportes/Salidas/fah/DAVIBANk',
    'YYYYMMDD',
    NULL,
    TRUE,
    FALSE,
    NULL,
    NULL,
    TRUE,
    CURRENT_DATE()
  );

-- HEADER

/* ---SQL_MASTER--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report`
WHERE
  ID_PRODUCTO_DATOS = 20
  AND NOMBRE_TABLA = 'DAVIBANK_FAH';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report` (
    ID_PRODUCTO_DATOS,
    ID_TIPO_PROCESO,
    NOMBRE_PRODUCTO_DATOS,
    TAG_NEGOCIO,
    DATASET_DESTINO_MST,
    NOMBRE_TABLA,
    CATEGORIA_VOLUMETRIA,
    QUERY,
    AUTOMATIZACION,
    PERIODICIDAD,
    FLAG_ACTIVO,
    FLAG_NOTIFICACION,
    CORREO_NOTIFICACION,
    ASUNTO,
    CUERPO,
    FLG_ADJUNTOS,
    FECHA_REGISTRO
  )
VALUES
  (
    20,
    1,
    'FAH',
    '{"formato": "ARCHIVO_HEADER_DAVIBANK", "entidad": "ARCHIVO"}',
    'cur_administrativa',
    'DAVIBANK_FAH',
    'BAJA',
    """
	SELECT 
	DISTINCT CODIGO_TRANSACCION AS TRANSACTION_NUMBER,
	TIPO_EVENTO AS EVENT_TYPE_CODE     ,
	TIPO_LIBRO_CONTABLE AS LEDGER_NAME ,
	FORMAT_DATE('%Y-%m-%d', FECHA_EFECTIVA) AS TRANSACTION_DATE ,
	'N' AS TRANSACTION_REVERSAL_FLAG   ,
	DESC_ORIGEN_TRANSACCION AS DESCRIPCION_ORIGEN_TRANSACCION,
	TIPO_DE_COMPROBANTE AS COMPROBANTE_CONTABLE,
	ORIGEN_TRANSACCION AS ORIGEN_TRANSACCION,
	MONEDA AS MONEDA                   ,
	NULL AS Character5                 ,
	NULL AS Character6                 ,
	NULL AS Character7                 ,
	NULL AS Character8                 ,
	NULL AS Character9                 ,
	NULL AS Character10                ,
	NULL AS Character11                ,
	NULL AS Character12                ,
	NULL AS Character13                ,
	NULL AS Character14                ,
	NULL AS Character15                ,
	NULL AS Character16                ,
	NULL AS Character17                ,
	NULL AS Character18                ,
	NULL AS Character19                ,
	NULL AS Character20                ,
	NULL AS Character21                ,
	NULL AS Character22                ,
	NULL AS Character23                ,
	NULL AS Character24                ,
	NULL AS Character25                ,
	NULL AS Character26                ,
	NULL AS Character27                ,
	NULL AS Character28                ,
	NULL AS Character29                ,
	NULL AS Character30                ,
	NULL AS Character31                ,
	NULL AS Character32                ,
	NULL AS Character33                ,
	NULL AS Character34                ,
	NULL AS Character35                ,
	NULL AS Character36                ,
	NULL AS Character37                ,
	NULL AS Character38                ,
	NULL AS Character39                ,
	NULL AS Character40                ,
	NULL AS Character41                ,
	NULL AS Character42                ,
	NULL AS Character43                ,
	NULL AS Character44                ,
	NULL AS Character45                ,
	NULL AS Character46                ,
	NULL AS Character47                ,
	NULL AS Character48                ,
	NULL AS Character49                ,
	NULL AS Character50                ,
	NULL AS Number1                    ,
	NULL AS Number2                    ,
	NULL AS Number3                    ,
	NULL AS Number4                    ,
	NULL AS Number5                    ,
	NULL AS Number6                    ,
	NULL AS Number7                    ,
	NULL AS Number8                    ,
	NULL AS Number9                    ,
	NULL AS Number10                   ,
	NULL AS Date1                      ,
	NULL AS Date2                      ,
	NULL AS Date3                      ,
	NULL AS Date4                      ,
	NULL AS Date5                      ,
	NULL AS Date6                      ,
	NULL AS Date7                      ,
	NULL AS Date8                      ,
	NULL AS Date9                      ,
	NULL AS Date10                     ,
	NULL AS `Long Character1`          ,
	NULL AS `Long Character2`          ,
	NULL AS `Long Character3`          ,
	NULL AS `Long Character4`          ,
	NULL AS `Long Character5`  	   ,
	grupo_salida as grupo
	FROM 
		`datalake2-prototipado.cur_administrativa.DAVIBANK_FAH` AS davibank
	WHERE
		davibank.PERIODO = '$YYYYMMDD'
	""",
    NULL,
    'Diaria',
    TRUE,
    FALSE,
    NULL,
    NULL,
    NULL,
    FALSE,
    CURRENT_DATE()
  );

/* ---SQL_DOC--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte`
WHERE
  ID_PRODUCTO_DATOS = 20
  AND NOMBRE_REPORTE = 'ARCHIVO_HEADER_DAVIBANK';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte` (
    ID_PRODUCTO_DATOS,
    NOMBRE_PRODUCTO_DATOS,
    ID_REPORTE,
    NOMBRE_REPORTE,
    TIPO_SALIDA,
    TIPO_EJECUCION,
    SCRIPT_PATH,
    FILTRO,
    ENCODING,
    FORMATO_NOMBRE_ARCHIVO,
    EXTENSION,
    DELIMITADOR,
    FLAG_ENCABEZADO,
    RUTA_DESTINO,
    FORMATO_FECHA,
    N_REGISTROS_X_ARCHIVO,
    FLAG_COMPLETITUD_INFORMACION,
    FLAG_CONSECUTIVO,
    CONSECUTIVO_INICIO,
    CONSECUTIVO_FIN,
    FLAG_ACTIVO,
    FECHA_REGISTRO
  )
VALUES
  (
    20,
    'FAH',
    'RPT-DAVIBANK-ARCHIVO-V1',
    'ARCHIVO_HEADER_DAVIBANK',
    'Archivo',
    'SQL',
    NULL,
    NULL,
    'UTF-8',
    'XlaTrxH_DAVIBANK_{FORMATO_FECHA}_{CONTADOR}_de_{TOTAL_ARCHIVOS}_{VAR_REPROCESO}.{EXTENCION}',
    'csv',
    ',',
    TRUE,
    'gs://par_dav_proto_configuraciones/8096_tec_planeacion_y_riesgo/Woombat/codigo_fuente/Servicio_de_entrega_de_datos/Generador_Reportes/Salidas/fah/DAVIBANk',
    'YYYYMMDD',
    NULL,
    TRUE,
    FALSE,
    NULL,
    NULL,
    TRUE,
    CURRENT_DATE()
  );

-- LINES

/* ---SQL_MASTER--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report`
WHERE
  ID_PRODUCTO_DATOS = 21
  AND NOMBRE_TABLA = 'DAVIBANK_FAH';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report` (
    ID_PRODUCTO_DATOS,
    ID_TIPO_PROCESO,
    NOMBRE_PRODUCTO_DATOS,
    TAG_NEGOCIO,
    DATASET_DESTINO_MST,
    NOMBRE_TABLA,
    CATEGORIA_VOLUMETRIA,
    QUERY,
    AUTOMATIZACION,
    PERIODICIDAD,
    FLAG_ACTIVO,
    FLAG_NOTIFICACION,
    CORREO_NOTIFICACION,
    ASUNTO,
    CUERPO,
    FLG_ADJUNTOS,
    FECHA_REGISTRO
  )
VALUES
  (
    21,
    1,
    'FAH',
    '{"formato": "ARCHIVO_LINES_DAVIBANK", "entidad": "ARCHIVO"}',
    'cur_administrativa',
    'DAVIBANK_FAH',
    'BAJA',
    """
	SELECT 
	CODIGO_TRANSACCION AS TRANSACTION_NUMBER    ,
	CONTADOR_LINEA AS LINE_NUMBER               ,
	NULL AS COD_SUCURSAL                        ,
	DEPENDENCIA AS OFICINA_ORIGEN_TRANSACCION   ,
	CUENTA AS CUENTA_CONTABLE                   ,
	NATURALEZA AS NATURALEZA                    ,
	DESC_ORIGEN_TRANSACCION AS DESC_TRANSACCION ,
	REFERENCIA  AS REFERENCIA_01                ,
	'0' AS REFERENCIA_02                        ,
	TIPO_DE_COMPROBANTE AS TIPO_COMPROBANTE     ,
	NULL AS Character9                          ,
	NULL AS Character10                         ,
	NULL AS Character11                         ,
	NULL AS Character12                         ,
	NULL AS Character13                         ,
	NULL AS Character14                         ,
	NULL AS Character15                         ,
	NULL AS Character16                         ,
	NULL AS Character17                         ,
	NULL AS Character18                         ,
	NULL AS Character19                         ,
	NULL AS Character20                         ,
	NULL AS Character21                         ,
	NULL AS Character22                         ,
	NULL AS Character23                         ,
	NULL AS Character24                         ,
	NULL AS Character25                         ,
	NULL AS Character26                         ,
	NULL AS Character27                         ,
	NULL AS Character28                         ,
	NULL AS Character29                         ,
	NULL AS Character30                         ,
	NULL AS Character31                         ,
	NULL AS Character32                         ,
	NULL AS Character33                         ,
	NULL AS Character34                         ,
	NULL AS Character35                         ,
	NULL AS Character36                         ,
	NULL AS Character37                         ,
	NULL AS Character38                         ,
	NULL AS Character39                         ,
	NULL AS Character40                         ,
	NULL AS Character41                         ,
	NULL AS Character42                         ,
	NULL AS Character43                         ,
	NULL AS Character44                         ,
	NULL AS Character45                         ,
	NULL AS Character46                         ,
	NULL AS Character47                         ,
	NULL AS Character48                         ,
	NULL AS Character49                         ,
	NULL AS Character50                         ,
	NULL AS Character51                         ,
	NULL AS Character52                         ,
	NULL AS Character53                         ,
	NULL AS Character54                         ,
	NULL AS Character55                         ,
	NULL AS Character56                         ,
	NULL AS Character57                         ,
	NULL AS Character58                         ,
	NULL AS Character59                         ,
	NULL AS Character60                         ,
	NULL AS Character61                         ,
	NULL AS Character62                         ,
	NULL AS Character63                         ,
	NULL AS Character64                         ,
	NULL AS Character65                         ,
	NULL AS Character66                         ,
	NULL AS Character67                         ,
	NULL AS Character68                         ,
	NULL AS Character69                         ,
	NULL AS Character70                         ,
	NULL AS Character71                         ,
	NULL AS Character72                         ,
	NULL AS Character73                         ,
	NULL AS Character74                         ,
	NULL AS Character75                         ,
	NULL AS Character76                         ,
	NULL AS Character77                         ,
	NULL AS Character78                         ,
	NULL AS Character79                         ,
	NULL AS Character80                         ,
	NULL AS Character81                         ,
	NULL AS Character82                         ,
	NULL AS Character83                         ,
	NULL AS Character84                         ,
	NULL AS Character85                         ,
	NULL AS Character86                         ,
	NULL AS Character87                         ,
	NULL AS Character88                         ,
	NULL AS Character89                         ,
	NULL AS Character90                         ,
	NULL AS Character91                         ,
	NULL AS Character92                         ,
	NULL AS Character93                         ,
	NULL AS Character94                         ,
	NULL AS Character95                         ,
	NULL AS Character96                         ,
	NULL AS Character97                         ,
	NULL AS Character98                         ,
	NULL AS Character99                         ,
	NULL AS Character100                        ,
	NULL AS Number1                             ,
	NULL AS Number2                             ,
	VALOR_MOVIMIENTO AS VALOR_COP_1             ,
	VALOR_MOVIMIENTO AS VALOR_MO_1              ,
	NULL AS Number5                             ,
	NULL AS Number6                             ,
	NULL AS Number7                             ,
	NULL AS Number8                             ,
	NULL AS Number9                             ,
	NULL AS Number10                            ,
	NULL AS Number11                            ,
	NULL AS Number12                            ,
	NULL AS Number13                            ,
	NULL AS Number14                            ,
	NULL AS Number15                            ,
	NULL AS Number16                            ,
	NULL AS Number17                            ,
	NULL AS Number18                            ,
	NULL AS Number19                            ,
	NULL AS Number20                            ,
	NULL AS Number21                            ,
	NULL AS Number22                            ,
	NULL AS Number23                            ,
	NULL AS Number24                            ,
	NULL AS Number25                            ,
	NULL AS Number26                            ,
	NULL AS Number27                            ,
	NULL AS Number28                            ,
	NULL AS Number29                            ,
	NULL AS Number30                            ,
	NULL AS Date1                               ,
	NULL AS Date2                               ,
	NULL AS Date3                               ,
	NULL AS Date4                               ,
	NULL AS Date5                               ,
	NULL AS Date6                               ,
	NULL AS Date7                               ,
	NULL AS Date8                               ,
	NULL AS Date9                               ,
	NULL AS Date10                              ,
	NULL AS RAZON_SOCIAL_TITULAR                ,
	NULL AS `Long Character2`                   ,
	NULL AS `Long Character3`                   ,
	NULL AS `Long Character4`                   ,
	NULL AS `Long Character5` 		    , 
	grupo_salida as grupo
	FROM
		`datalake2-prototipado.cur_administrativa.DAVIBANK_FAH` AS davibank
	WHERE
		davibank.PERIODO = '$YYYYMMDD'
	""",
    NULL,
    'Diaria',
    TRUE,
    FALSE,
    NULL,
    NULL,
    NULL,
    FALSE,
    CURRENT_DATE()
  );

/* ---SQL_DOC--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte`
WHERE
  ID_PRODUCTO_DATOS = 21
  AND NOMBRE_REPORTE = 'ARCHIVO_LINES_DAVIBANK';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte` (
    ID_PRODUCTO_DATOS,
    NOMBRE_PRODUCTO_DATOS,
    ID_REPORTE,
    NOMBRE_REPORTE,
    TIPO_SALIDA,
    TIPO_EJECUCION,
    SCRIPT_PATH,
    FILTRO,
    ENCODING,
    FORMATO_NOMBRE_ARCHIVO,
    EXTENSION,
    DELIMITADOR,
    FLAG_ENCABEZADO,
    RUTA_DESTINO,
    FORMATO_FECHA,
    N_REGISTROS_X_ARCHIVO,
    FLAG_COMPLETITUD_INFORMACION,
    FLAG_CONSECUTIVO,
    CONSECUTIVO_INICIO,
    CONSECUTIVO_FIN,
    FLAG_ACTIVO,
    FECHA_REGISTRO
  )
VALUES
  (
    21,
    'FAH',
    'RPT-DAVIBANK-ARCHIVO-V1',
    'ARCHIVO_LINES_DAVIBANK',
    'Archivo',
    'SQL',
    NULL,
    NULL,
    'UTF-8',
    'XlaTrxL_DAVIBANK_{FORMATO_FECHA}_{CONTADOR}_de_{TOTAL_ARCHIVOS}_{VAR_REPROCESO}.{EXTENCION}',
    'csv',
    ',',
    TRUE,
    'gs://par_dav_proto_configuraciones/8096_tec_planeacion_y_riesgo/Woombat/codigo_fuente/Servicio_de_entrega_de_datos/Generador_Reportes/Salidas/fah/DAVIBANk',
    'YYYYMMDD',
    NULL,
    TRUE,
    FALSE,
    NULL,
    NULL,
    TRUE,
    CURRENT_DATE()
  );


-- CONTROL

/* ---SQL_MASTER--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report`
WHERE
  ID_PRODUCTO_DATOS = 22
  AND NOMBRE_TABLA = 'DAVIBANK_FAH';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report` (
    ID_PRODUCTO_DATOS,
    ID_TIPO_PROCESO,
    NOMBRE_PRODUCTO_DATOS,
    TAG_NEGOCIO,
    DATASET_DESTINO_MST,
    NOMBRE_TABLA,
    CATEGORIA_VOLUMETRIA,
    QUERY,
    AUTOMATIZACION,
    PERIODICIDAD,
    FLAG_ACTIVO,
    FLAG_NOTIFICACION,
    CORREO_NOTIFICACION,
    ASUNTO,
    CUERPO,
    FLG_ADJUNTOS,
    FECHA_REGISTRO
  )
VALUES
  (
    22,
    1,
    'FAH',
    '{"formato": "ARCHIVO_CONTROL_DAVIBANK", "entidad": "ARCHIVO"}',
    'cur_administrativa',
    'DAVIBANK_FAH',
    'BAJA',
    """
	WITH FilteredCommonData AS (
          SELECT
              s.PERIODO,
              s.CODIGO_TRANSACCION,
              s.GRUPO_SALIDA AS GRUPO
          FROM
              datalake2-prototipado.cur_administrativa.DAVIBANK_FAH AS s
             WHERE
              s.PERIODO = '$YYYYMMDD'
      )
      SELECT
          FORMAT_DATE('%Y%m%d', common_data.PERIODO) AS periodo_formateado,
          'LINE' AS tipo_registro,
          COUNT(1) AS recuento,
          common_data.GRUPO
      FROM
          FilteredCommonData AS common_data
      GROUP BY
          periodo_formateado,
          common_data.GRUPO
      UNION
      ALL
      SELECT
          FORMAT_DATE('%Y%m%d', distinct_data.PERIODO) AS periodo_formateado,
          'HEADER' AS tipo_registro,
          COUNT(1) AS recuento,
          distinct_data.GRUPO
      FROM
          (
              SELECT
                  DISTINCT common_data.CODIGO_TRANSACCION,
                  common_data.PERIODO,
                  common_data.GRUPO
              FROM
                  FilteredCommonData AS common_data
          ) AS distinct_data
      GROUP BY
          periodo_formateado,
          distinct_data.GRUPO	
	""",
    NULL,
    'Diaria',
    TRUE,
    FALSE,
    NULL,
    NULL,
    NULL,
    FALSE,
    CURRENT_DATE()
  );

/* ---SQL_DOC--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte`
WHERE
  ID_PRODUCTO_DATOS  = 22
  AND NOMBRE_REPORTE = 'ARCHIVO_CONTROL_DAVIBANK';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte` (
    ID_PRODUCTO_DATOS,
    NOMBRE_PRODUCTO_DATOS,
    ID_REPORTE,
    NOMBRE_REPORTE,
    TIPO_SALIDA,
    TIPO_EJECUCION,
    SCRIPT_PATH,
    FILTRO,
    ENCODING,
    FORMATO_NOMBRE_ARCHIVO,
    EXTENSION,
    DELIMITADOR,
    FLAG_ENCABEZADO,
    RUTA_DESTINO,
    FORMATO_FECHA,
    N_REGISTROS_X_ARCHIVO,
    FLAG_COMPLETITUD_INFORMACION,
    FLAG_CONSECUTIVO,
    CONSECUTIVO_INICIO,
    CONSECUTIVO_FIN,
    FLAG_ACTIVO,
    FECHA_REGISTRO
  )
VALUES
  (
    22,
    'FAH',
    'RPT-DAVIBANK-ARCHIVO-V1',
    'ARCHIVO_CONTROL_DAVIBANK',
    'Archivo',
    'SQL',
    NULL,
    NULL,
    'UTF-8',
    'XlaCtl_DAVIBANK_{FORMATO_FECHA}_{CONTADOR}_de_{TOTAL_ARCHIVOS}_{VAR_REPROCESO}.{EXTENCION}',
    'txt',
    ',',
    FALSE,
    'gs://par_dav_proto_configuraciones/8096_tec_planeacion_y_riesgo/Woombat/codigo_fuente/Servicio_de_entrega_de_datos/Generador_Reportes/Salidas/fah/DAVIBANk',
    'YYYYMMDD',
    NULL,
    TRUE,
    FALSE,
    NULL,
    NULL,
    TRUE,
    CURRENT_DATE()
  );

/

/* ---SQL_MASTER--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report`
WHERE
  ID_PRODUCTO_DATOS = 23
  AND NOMBRE_TABLA = 'GENERAR_FAH_DAVIBANK_COMPLETO_ZIP';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report` (
    ID_PRODUCTO_DATOS,
    ID_TIPO_PROCESO,
    NOMBRE_PRODUCTO_DATOS,
    TAG_NEGOCIO,
    DATASET_DESTINO_MST,
    NOMBRE_TABLA,
    CATEGORIA_VOLUMETRIA,
    QUERY,
    AUTOMATIZACION,
    PERIODICIDAD,
    FLAG_ACTIVO,
    FLAG_NOTIFICACION,
    CORREO_NOTIFICACION,
    ASUNTO,
    CUERPO,
    FLG_ADJUNTOS,
    FECHA_REGISTRO
  )
VALUES
  (
    23,
    1,
    'FAH',
    '{"formato": "GENERAR_FAH_DAVIBANK_COMPLETO_ZIP", "entidad": "ZIP"}',
    '8096_tec_planeacion_y_riesgo',
    'GENERAR_FAH_DAVIBANK_COMPLETO_ZIP',
    'BAJA',
    """SELECT
        d.ID_PRODUCTO_DATOS,
        d.NOMBRE_REPORTE,
        d.ENCODING,
        d.FORMATO_NOMBRE_ARCHIVO,
        d.EXTENSION,
        d.DELIMITADOR,
        d.FLAG_ENCABEZADO,
        d.RUTA_DESTINO,
        d.FLAG_CONSECUTIVO,
        d.CONSECUTIVO_INICIO,
        d.CONSECUTIVO_FIN,
        d.FORMATO_FECHA,
        m.QUERY AS SQL_SUBREPORTE,
        m.TAG_NEGOCIO
    FROM
        `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte` d
        JOIN `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report` m ON d.ID_PRODUCTO_DATOS = m.ID_PRODUCTO_DATOS
        AND d.NOMBRE_PRODUCTO_DATOS = m.NOMBRE_PRODUCTO_DATOS
    WHERE
        d.FLAG_ACTIVO = TRUE
        AND d.NOMBRE_PRODUCTO_DATOS = 'FAH'
        AND UPPER(d.NOMBRE_REPORTE) LIKE '%DAVIBANK%'
        AND d.EXTENSION != 'zip'
    ORDER BY
        d.NOMBRE_REPORTE""",
    NULL,
    'Diaria',
    TRUE,
    FALSE,
    NULL,
    NULL,
    NULL,
    FALSE,
    CURRENT_DATE()
  );

/* ---SQL_DOC--- */
DELETE FROM
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte`
WHERE
  ID_PRODUCTO_DATOS = 23
  AND NOMBRE_REPORTE = 'GENERAR_FAH_DAVIBANK_COMPLETO_ZIP';

INSERT INTO
  `datalake2-prototipado.8096_tec_planeacion_y_riesgo.dav_config_documento_reporte` (
    ID_PRODUCTO_DATOS,
    NOMBRE_PRODUCTO_DATOS,
    ID_REPORTE,
    NOMBRE_REPORTE,
    TIPO_SALIDA,
    TIPO_EJECUCION,
    SCRIPT_PATH,
    FILTRO,
    ENCODING,
    FORMATO_NOMBRE_ARCHIVO,
    EXTENSION,
    DELIMITADOR,
    FLAG_ENCABEZADO,
    RUTA_DESTINO,
    FORMATO_FECHA,
    N_REGISTROS_X_ARCHIVO,
    FLAG_COMPLETITUD_INFORMACION,
    FLAG_CONSECUTIVO,
    CONSECUTIVO_INICIO,
    CONSECUTIVO_FIN,
    FLAG_ACTIVO,
    FECHA_REGISTRO
  )
VALUES
  (
    23,
    'FAH',
    'RPT-DAVIBANK-ZIP-V1',
    'GENERAR_FAH_DAVIBANK_COMPLETO_ZIP',
    'Archivo',
    'PySpark',
    'gs://par_dav_proto_configuraciones/8096_tec_planeacion_y_riesgo/Woombat/codigo_fuente/Servicio_de_entrega_de_datos/Generador_Reportes/Proyectos/reporte_fah.py',
    NULL,
    'UTF-8',
    'XlaTransaction_DAVIBANK_{FORMATO_FECHA}_{CONTADOR}_de_{TOTAL_ARCHIVOS}_{VAR_REPROCESO}.{EXTENCION}',
    'zip',
    null,
    FALSE,
    'gs://par_dav_proto_configuraciones/8096_tec_planeacion_y_riesgo/Woombat/codigo_fuente/Servicio_de_entrega_de_datos/Generador_Reportes/Salidas/fah/DAVIBANk',
    'YYYYMMDD',
    NULL,
    TRUE,
    FALSE,
    NULL,
    NULL,
    TRUE,
    CURRENT_DATE()
  );

-- No aplica: el reporte ZIP no requiere plantilla
