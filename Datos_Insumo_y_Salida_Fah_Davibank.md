# Catálogo de Datos de Insumo y Salida: Integración FAH - Davibank

Este documento describe de manera exhaustiva todos los datos de entrada (insumos) y de salida (entregables/tablas intermedias) que intervienen en el flujo de integración de **DAVIBANK_FAH**.

El flujo está estructurado en dos frentes operacionales: **Producto de Datos** (Procesamiento y Transformación SQL) y **Generador de Reportes** (Extracción PySpark y Empaquetado ZIP).

---

## 1. Frente 1: Producto de Datos (`Agente_Producto_Datos`)

Este frente se encarga de realizar la preparación de los datos en BigQuery. Recibe los datos contables planos del día y genera una tabla consolidada, homologada y balanceada en bloques lógicos.

### Cuadro Descriptivo de Insumos y Salidas

| Categoría | Nombre del Recurso / Componente | Tipo de Recurso | Descripción / Propósito | Campos Clave / Atributos Principales |
| :--- | :--- | :--- | :--- | :--- |
| **Insumo** | `cur_administrativa.trf_davibank_fah` | Tabla BigQuery (Particionada) | Tabla origen que contiene la ingesta diaria de los movimientos contables crudos de Davibank. | `FECHA_EFECTIVA`, `FECHA_POSTEO`, `CTACON`, `MONEDA`, `CTRCTO`, `CONSECUTIVO`, `VLRDEB`, `VLRCRE`, `PERIODO` |
| **Insumo** | Parámetros de Ejecución Airflow | Variables de Control SQL | Parámetros dinámicos pasados por el orquestador al ejecutar el DAG. | `@fecha_proceso` (DATE)<br>`@libro` (STRING, por defecto `'BIFRS_DAV'`) |
| **Insumo** | `cur_activo.STG_LOG_PROCESOS_FAH_PRUEBA` | Tabla BigQuery | Registro histórico utilizado para auditar las ejecuciones del día y determinar el índice de reproceso de la fecha. | `FUENTE` ('DAVIBANK'), `PERIODO_EJECUTADO_FAH`, `NUM_VECES_REPROCESADO` |
| **Insumo** | `cur_administrativa.mnl_homologaciones_fah` | Tabla BigQuery (Estructura de Referencia) | Matriz de homologación manual para traducir las cuentas de origen de Davibank a las cuentas oficiales del Banco Davivienda. | `FUENTE` ('DAVIBANK'), `CAMPO_ORIGEN` ('Cuenta'), `VALOR_ORIGEN`, `VALOR_SALIDA` |
| **Salida Intermedia** | `cur_administrativa.work_davibank_filtrado` | Tabla BigQuery Temporal | Tabla temporal particionada que almacena únicamente los movimientos del día de negocio actual para aislar el volumen de procesamiento. | Copia estructurada de la tabla origen para el periodo correspondiente. |
| **Salida Intermedia** | `cur_administrativa.work_davibank_transformacion_1` | Tabla BigQuery Temporal | Almacena los resultados del desdoblamiento de columnas (unpivot de débito y crédito), el cálculo de llaves únicas transaccionales y la homologación contable. | `codigo_transaccion`, `cuenta`, `naturaleza` ('1'=Débito, '2'=Crédito), `valor_movimiento`, `contador_linea` |
| **Salida Intermedia** | `cur_administrativa.work_davibank_transformacion_2` | Tabla BigQuery Temporal | Almacena el dataset transformado incluyendo la lógica de balanceo físico de registros para asegurar un límite máximo de 150,000 líneas por bloque. | Todos los campos de transformación + `grupo_salida` (identificador del bloque o lote de entrega) |
| **Salida Final** | `cur_administrativa.DAVIBANK_FAH` | Tabla BigQuery Destino (Particionada) | Tabla definitiva persistida que contiene los datos finales del día completamente listos para que el generador de reportes los consuma. | Campos consolidados e indexados por `periodo` y `grupo_salida`. |

---

## 2. Frente 2: Generador de Reportes (`Generador_Reportes_Fah`)

Este frente se encarga de extraer la información de BigQuery a partir del catálogo final de Producto de Datos, aplicar la parametrización de nombres y codificaciones de archivos, empaquetar el entregable en un único archivo ZIP maestro y publicarlo en GCS.

### Cuadro Descriptivo de Insumos y Salidas

| Categoría | Nombre del Recurso / Componente | Tipo de Recurso | Descripción / Propósito | Campos Clave / Atributos Principales |
| :--- | :--- | :--- | :--- | :--- |
| **Insumo** | `cur_administrativa.DAVIBANK_FAH` | Tabla BigQuery Destino | Tabla final generada en el Frente 1 que sirve como la fuente única de verdad transaccional. | `fecha_efectiva`, `cuenta`, `moneda`, `dependencia`, `valor_movimiento`, `naturaleza`, `codigo_transaccion`, `grupo_salida`, `periodo` |
| **Insumo** | `8096_tec_planeacion_y_riesgo.dav_mst_metadatos_master_report` | Tabla BigQuery (Metamodelo) | Almacena las consultas SQL maestras utilizadas para extraer los datos de cada reporte individual. | `ID_PRODUCTO_DATOS`, `NOMBRE_TABLA`, `QUERY`, `TAG_NEGOCIO` |
| **Insumo** | `8096_tec_planeacion_y_riesgo.dav_config_documento_reporte` | Tabla BigQuery (Metamodelo) | Configura el comportamiento físico de los archivos de salida: extensiones, delimitadores, codificación, encabezados y patrones de nombres de archivo. | `NOMBRE_REPORTE`, `ENCODING`, `FORMATO_NOMBRE_ARCHIVO`, `EXTENSION`, `DELIMITADOR`, `FLAG_ENCABEZADO`, `RUTA_DESTINO` |
| **Insumo** | `cur_activo.LOG_PROCESOS_FAH` | Tabla BigQuery (Control de Estado) | Auditoría de control operacional para la lectura y posterior actualización de la secuencia de reproceso diaria. | `FUENTE`, `PERIODO_EJECUTADO_FAH`, `NUM_VECES_REPROCESADO`, `ULT_FECHA_DE_CARGUE` |
| **Insumo** | Argumentos de Ejecución (Paso por CLI) | Diccionario de Parámetros (JSON) | Argumentos entregados al script `reporte_fah.py` que definen el proyecto, las fechas a procesar y el nombre del reporte. | `project_id`, `DATASET_DESTINO_MST`, `PROYECTO_META`, `DATASET_META`, `NOMBRE_REPORTE`, `dict_dates` |
| **Salida Temporal** | Archivo de Metadata (TXT) | Archivo Plano Local (`ISO-8859-1`) | Reporte con información general de la entrega contable. Delimitado por comas, sin encabezado. | Patrón: `Metadata_DAVIBANK_{FORMATO_FECHA}_{GRUPO}_de_{TOTAL_GRUPOS}_{REPROCESO}.txt` |
| **Salida Temporal** | Archivo de Header (Cabecera) (CSV) | Archivo Plano Local (`UTF-8`) | Reporte con los datos de cabecera de las transacciones financieras. Delimitado por comas, incluye encabezado. | Patrón: `XlaTrxH_DAVIBANK_{FORMATO_FECHA}_{GRUPO}_de_{TOTAL_GRUPOS}_{REPROCESO}.csv` |
| **Salida Temporal** | Archivo de Lines (Líneas) (CSV) | Archivo Plano Local (`UTF-8`) | Reporte con los desgloses y distribución contable de movimientos. Delimitado por comas, incluye encabezado. | Patrón: `XlaTrxL_DAVIBANK_{FORMATO_FECHA}_{GRUPO}_de_{TOTAL_GRUPOS}_{REPROCESO}.csv` |
| **Salida Temporal** | Archivo de Control (TXT) | Archivo Plano Local (`UTF-8`) | Reporte de cuadre que resume el recuento de filas de cabecera y líneas por grupo. Delimitado por comas, sin encabezado. | Patrón: `XlaCtl_DAVIBANK_{FORMATO_FECHA}_{GRUPO}_de_{TOTAL_GRUPOS}_{REPROCESO}.txt` |
| **Salida Final** | Archivo Master ZIP (.Zip) | Archivo Comprimido Binario | Entregable final de integración contable que contiene los 4 reportes anteriores agrupados de manera limpia sin anidación. | Patrón: `XlaTransaction_DAVIBANK_{FORMATO_FECHA}_{GRUPO}_de_{TOTAL_GRUPOS}_{REPROCESO}.zip` |
| **Salida Final** | Destino GCS | Almacenamiento en Nube GCS | Carga física automatizada del Master ZIP al bucket de Google Cloud Storage configurado. | Ruta: `gs://par_dav_proto_configuraciones/8096_tec_planeacion_y_riesgo/Woombat/codigo_fuente/Servicio_de_entrega_de_datos/Generador_Reportes/Salidas/fah/DAVIBANk/` |
| **Salida de Auditoría** | `cur_activo.LOG_PROCESOS_FAH` | Actualización de Fila BigQuery | Realiza un merge (UPSERT) para actualizar el campo `NUM_VECES_REPROCESADO` y la fecha de carga del proceso exitoso. | `FUENTE` ('DAVIBANK'), `PERIODO_EJECUTADO_FAH`, `NUM_VECES_REPROCESADO`, `ULT_FECHA_DE_CARGUE` |
