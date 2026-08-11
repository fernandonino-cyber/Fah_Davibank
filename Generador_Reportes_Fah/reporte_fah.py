#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, subprocess
subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "google-cloud-bigquery"])

import json, logging, os, tempfile, zipfile, ast
from datetime import datetime
from collections import defaultdict
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO
)
logging.getLogger("pyspark").setLevel(logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)

logger = logging.getLogger("FAH")

# ==============================================================================
# PARAMETROS POR ARGUMENTO (como reportes_c032.py)
# ==============================================================================
query_cliente          = sys.argv[1]
diccionario_argumentos = sys.argv[2]

# NOTA: sys.argv siempre entrega str (nunca dict), por lo que el chequeo
# isinstance(..., dict) de la version anterior nunca se cumplia realmente.
# El problema real es que quien invoca este script a veces serializa el
# diccionario con str(dict) (comillas simples, True/False/None) en vez de
# json.dumps(dict) (JSON valido, comillas dobles, true/false/null).
# Por eso se intenta primero como JSON y, si falla, como literal de Python.
if isinstance(diccionario_argumentos, dict):
    argumentos = diccionario_argumentos
else:
    try:
        argumentos = json.loads(diccionario_argumentos)
    except json.JSONDecodeError:
        logger.warning(
            "LOGREPORTE: El argumento no es JSON valido, se intenta como literal de Python (dict con comillas simples)"
        )
        argumentos = ast.literal_eval(diccionario_argumentos)

project_id            = argumentos["project_id"]
dataset_mst           = argumentos.get("DATASET_DESTINO_MST", "8096_tec_planeacion_y_riesgo")
PROYECTO_META         = argumentos.get("PROYECTO_META", "datalake2-prototipado")
DATASET_META          = argumentos.get("DATASET_META", "8096_tec_planeacion_y_riesgo")
NOMBRE_REPORTE_MAESTRO = argumentos.get("NOMBRE_REPORTE", "GENERAR_FAH_SIGLEASE_COMPLETO_ZIP")
dict_dates            = argumentos.get("dict_dates", {})
fecha_inicial         = dict_dates.get("fecha_inicio", argumentos.get("FECHA_INICIAL", datetime.now().strftime("%Y%m%d")))
fecha_final           = dict_dates.get("fecha_fin", argumentos.get("FECHA_FINAL", fecha_inicial))
fecha_ejecucion       = fecha_inicial
# Version sin guiones de la fecha, usada unicamente para armar nombres de archivo.
# El header/control y la consulta de reproceso siguen usando fecha_ejecucion tal cual.
fecha_archivo         = fecha_ejecucion.replace("-", "")

logger.info("LOGREPORTE: project_id=%s", project_id)
logger.info("LOGREPORTE: metamodelo=%s.%s", PROYECTO_META, DATASET_META)
logger.info("LOGREPORTE: reporte_maestro=%s", NOMBRE_REPORTE_MAESTRO)
logger.info("LOGREPORTE: fecha_ejecucion=%s", fecha_ejecucion)

TABLA_MASTER  = "dav_mst_metadatos_master_report"
TABLA_DOC     = "dav_config_documento_reporte"
# V2: TABLA_CAMPOS (dav_config_campos_fuentes_report) eliminada.
# Las columnas de salida ahora se toman directamente del DataFrame.


def extraer_fuente_desde_maestro(nombre_maestro):
    """
    Extrae la fuente (SIGLEASE, COBIS, DAVIBANK, etc.) desde el nombre del
    reporte maestro, asumiendo el patron GENERAR_FAH_<FUENTE>_... 
    Ej: GENERAR_FAH_SIGLEASE_COMPLETO_ZIP -> SIGLEASE
    """
    match = re.match(r"^GENERAR_FAH_([A-Z0-9]+)_", nombre_maestro)
    if match:
        return match.group(1)
    return None

# ------------------------------------------------------------------------------
# CONTROL DE REPROCESO (equivalente a VAR_VALIDA_REPROCESO en ODI)
# ------------------------------------------------------------------------------
# Mismo proyecto/dataset que STG_SIGLEASE_FAH ("cur_activo" en datalake2-prototipado),
# pero se deja parametrizable por si cambia el ambiente.
PROYECTO_LOG        = argumentos.get("PROYECTO_LOG", project_id)
DATASET_LOG         = argumentos.get("DATASET_LOG", "cur_activo")
TABLA_LOG_PROCESOS  = argumentos.get("TABLA_LOG_PROCESOS", "LOG_PROCESOS_FAH")
FUENTE_FAH          = (
    argumentos.get("FUENTE_FAH")
    or extraer_fuente_desde_maestro(NOMBRE_REPORTE_MAESTRO)
)

# ------------------------------------------------------------------------------
# V2: columnas tecnicas de control que las queries de sub-reportes pueden traer
# (p.ej. para agrupar o para ordenar) pero que NO deben terminar en el archivo
# de salida. Configurable por argumento; por defecto GRUPO y ORDEN.
# ------------------------------------------------------------------------------
COLUMNAS_EXCLUIR = set(
    c.upper() for c in argumentos.get("COLUMNAS_EXCLUIR", ["GRUPO", "ORDEN"])
)

spark = SparkSession.builder \
    .appName(f"FAH-{NOMBRE_REPORTE_MAESTRO}") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("viewsEnabled", "true") \
    .config("materializationDataset", dataset_mst) \
    .config("materializationExpirationTimeInMinutes", 300) \
    .getOrCreate()

def bq(query):
    return spark.read.format("bigquery").option("query", query).load()

def row_to_dict(row):
    return row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)

# ==============================================================================
# 1. LEER MAESTRO DEL METAMODELO
# ==============================================================================
def obtener_maestro(nombre_maestro):
    sql = f"""
        SELECT
            d.ID_PRODUCTO_DATOS,
            d.NOMBRE_PRODUCTO_DATOS,
            d.ID_REPORTE,
            d.NOMBRE_REPORTE,
            d.TIPO_SALIDA,
            d.TIPO_EJECUCION,
            d.SCRIPT_PATH,
            d.FILTRO,
            d.ENCODING,
            d.FORMATO_NOMBRE_ARCHIVO,
            d.EXTENSION,
            d.DELIMITADOR,
            d.FLAG_ENCABEZADO,
            d.RUTA_DESTINO,
            d.FORMATO_FECHA,
            d.N_REGISTROS_X_ARCHIVO,
            d.FLAG_COMPLETITUD_INFORMACION,
            d.FLAG_CONSECUTIVO,
            d.CONSECUTIVO_INICIO,
            d.CONSECUTIVO_FIN,
            m.QUERY,
            m.DATASET_DESTINO_MST,
            m.TAG_NEGOCIO,
            m.NOMBRE_TABLA,
            m.ID_TIPO_PROCESO,
            m.CATEGORIA_VOLUMETRIA,
            m.AUTOMATIZACION,
            m.PERIODICIDAD,
            m.FLAG_NOTIFICACION,
            m.CORREO_NOTIFICACION,
            m.ASUNTO,
            m.CUERPO,
            m.FLG_ADJUNTOS
        FROM `{PROYECTO_META}.{DATASET_META}.{TABLA_DOC}` d
        JOIN `{PROYECTO_META}.{DATASET_META}.{TABLA_MASTER}` m
            ON d.ID_PRODUCTO_DATOS = m.ID_PRODUCTO_DATOS
            AND d.NOMBRE_PRODUCTO_DATOS = m.NOMBRE_PRODUCTO_DATOS
        WHERE d.FLAG_ACTIVO = TRUE
          AND d.NOMBRE_REPORTE = '{nombre_maestro}'
        LIMIT 1
    """

    logger.info("LOGREPORTE: Leyendo reporte maestro: %s", nombre_maestro)
    df = bq(sql)
    rows = df.collect()
    if not rows:
        raise Exception(f"No se encontro el reporte maestro '{nombre_maestro}' en el metamodelo")
    r = row_to_dict(rows[0])

    tag = {}
    if r.get("TAG_NEGOCIO"):
        try:
            tag = json.loads(r["TAG_NEGOCIO"])
        except:
            pass

    maestro = {
            "id_producto":      r["ID_PRODUCTO_DATOS"],
            "nombre_producto":  r["NOMBRE_PRODUCTO_DATOS"],
            "id_reporte":       r["ID_REPORTE"],
            "nombre_reporte":   r["NOMBRE_REPORTE"],
            "tipo_salida":      r["TIPO_SALIDA"],
            "encoding":         r["ENCODING"],
            "formato_nombre":   r["FORMATO_NOMBRE_ARCHIVO"],
            "extension":        r["EXTENSION"],
            "delimitador":      r["DELIMITADOR"],
            "flag_encabezado":  r["FLAG_ENCABEZADO"],
            "flag_completitud": r.get("FLAG_COMPLETITUD_INFORMACION", True),
            "ruta_destino":     r["RUTA_DESTINO"],
            "formato_fecha":    r["FORMATO_FECHA"],
            "query_subreportes": r["QUERY"],
            "tag_negocio":      tag
        }
    return maestro

# ==============================================================================
# 2. OBTENER SUB-REPORTES DESDE EL QUERY DEL MAESTRO
# ==============================================================================
def obtener_subreportes(query_maestro):
    query = query_maestro \
        .replace("{PROYECTO_META}", PROYECTO_META) \
        .replace("{DATASET_META}", DATASET_META) \
        .replace("{NOMBRE_PRODUCTO}", argumentos.get("NOMBRE_PRODUCTO_DATOS", "FAH"))

    logger.info("LOGREPORTE: Ejecutando query del maestro para obtener sub-reportes...")
    logger.info("LOGREPORTE: Query sub-reportes: %s", query[:300])
    df = bq(query)
    rows = df.collect()
    logger.info("LOGREPORTE: Sub-reportes encontrados: %d", len(rows))

    subreportes = []
    for r in rows:
        id_producto = r["ID_PRODUCTO_DATOS"]
        nombre_reporte = r["NOMBRE_REPORTE"]

        # V2: ya no se consulta dav_config_campos_fuentes_report. Las columnas
        # de salida (nombre y orden) se toman directamente del resultado de
        # la query del sub-reporte al momento de generar el archivo.

        tag = {}
        r = row_to_dict(r)
        if r.get("TAG_NEGOCIO"):
            try:
                tag = json.loads(r["TAG_NEGOCIO"])
            except:
                pass

        subreportes.append({
            "id_producto":      id_producto,
            "nombre_reporte":   nombre_reporte,
            "encoding":         r["ENCODING"],
            "formato_nombre":   r["FORMATO_NOMBRE_ARCHIVO"],
            "extension":        r["EXTENSION"],
            "delimitador":      r["DELIMITADOR"],
            "flag_encabezado":  r["FLAG_ENCABEZADO"],
            "flag_completitud": r.get("FLAG_COMPLETITUD_INFORMACION", True),
            "ruta_destino":     r["RUTA_DESTINO"],
            "formato_fecha":    r.get("FORMATO_FECHA"),
            "flag_consecutivo": r["FLAG_CONSECUTIVO"],
            "consecutivo_inicio": r["CONSECUTIVO_INICIO"],
            "consecutivo_fin":  r["CONSECUTIVO_FIN"],
            "query":            r.get("SQL_SUBREPORTE") or r.get("QUERY"),
            "tag_negocio":      tag
        })

    return subreportes

# ==============================================================================
# 2.5 CONTROL DE REPROCESO (VAR_VALIDA_REPROCESO)
# ==============================================================================
def obtener_valor_reproceso(periodo, fuente):
    """
    Replica la logica de ODI: consulta LOG_PROCESOS_FAH para el periodo/fuente.
    - Si no hay registros previos -> 1 (primera ejecucion del periodo).
    - Si existen -> NUM_VECES_REPROCESADO + 1.
    """
    sql = f"""
        SELECT COALESCE(MAX(NUM_VECES_REPROCESADO), 0) AS ULTIMO_REPROCESO
        FROM `{PROYECTO_LOG}.{DATASET_LOG}.{TABLA_LOG_PROCESOS}`
        WHERE FUENTE = '{fuente}'
          AND PERIODO_EJECUTADO_FAH = '{periodo}'
    """
    try:
        row = bq(sql).collect()[0]
        ultimo = row["ULTIMO_REPROCESO"] or 0
    except Exception as e:
        logger.warning("LOGREPORTE: No se pudo leer LOG_PROCESOS_FAH (%s). Se asume primer proceso del periodo.", e)
        ultimo = 0

    valor = int(ultimo) + 1
    logger.info("LOGREPORTE: VAR_VALIDA_REPROCESO calculado = %s (periodo=%s, fuente=%s)", valor, periodo, fuente)
    return valor


def actualizar_log_proceso(periodo, fuente, valor_reproceso):
    """
    Equivalente al paso 'Actualiza Log de Proceso' de ODI: hace UPSERT del contador
    NUM_VECES_REPROCESADO para que la proxima ejecucion del mismo periodo tome el
    siguiente numero de la secuencia.
    """
    sql_merge = f"""
        MERGE `{PROYECTO_LOG}.{DATASET_LOG}.{TABLA_LOG_PROCESOS}` T
        USING (SELECT '{fuente}' AS FUENTE, DATE '{periodo}' AS PERIODO_EJECUTADO_FAH) S
        ON T.FUENTE = S.FUENTE AND T.PERIODO_EJECUTADO_FAH = S.PERIODO_EJECUTADO_FAH
        WHEN MATCHED THEN
          UPDATE SET NUM_VECES_REPROCESADO = {valor_reproceso},
                     ULT_FECHA_DE_CARGUE = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (FUENTE, PERIODO_EJECUTADO_FAH, NUM_VECES_REPROCESADO, ULT_FECHA_DE_CARGUE)
          VALUES ('{fuente}', DATE '{periodo}', {valor_reproceso}, CURRENT_TIMESTAMP())
    """
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROYECTO_LOG)
        client.query(sql_merge).result()
        logger.info(
            "LOGREPORTE: Log de proceso actualizado: fuente=%s periodo=%s num_veces_reprocesado=%s",
            fuente, periodo, valor_reproceso
        )
    except Exception as e:
        logger.error("LOGREPORTE: Error actualizando LOG_PROCESOS_FAH: %s", e)

# ==============================================================================
# 3. GENERAR HEADER
# ==============================================================================
def columnas_salida(df):
    """
    V2: define el orden y nombre de las columnas de salida directamente desde
    el DataFrame (tal como las devuelve la query del sub-reporte), excluyendo
    las columnas tecnicas de control definidas en COLUMNAS_EXCLUIR.
    """
    return [c for c in df.columns if c.upper() not in COLUMNAS_EXCLUIR]


def generar_header(df, reporte, grupo, total_registros, fecha):
    delim = reporte["delimitador"] if reporte["delimitador"] and reporte["delimitador"] != "{}" else ";"
    cols = columnas_salida(df)
    return delim.join(cols)

# ==============================================================================
# 4. GENERAR LINEAS
# ==============================================================================
def generar_lineas(df, reporte):
    delim = reporte["delimitador"] if reporte["delimitador"] and reporte["delimitador"] != "{}" else ";"
    if "ORDEN" in df.columns:
        df = df.orderBy("ORDEN")
    cols = columnas_salida(df)
    lineas = []
    for row in df.toLocalIterator():
        row_dict = row_to_dict(row)
        valores = ["" if row_dict.get(col) is None else str(row_dict.get(col)) for col in cols]
        lineas.append(delim.join(valores))
    return lineas

# ==============================================================================
# 5. GENERAR CONTROL
# ==============================================================================
def generar_control(reporte, grupo, total_registros, fecha):
    delim = reporte["delimitador"] if reporte["delimitador"] and reporte["delimitador"] != "{}" else ";"
    parts = [
        f"TOTAL_REGISTROS:{total_registros}",
        f"GRUPO:{grupo}",
        f"FECHA:{fecha}",
        f"REPORTE:{reporte['nombre_reporte']}"
    ]
    return delim.join(parts)

# ==============================================================================
# 6. ESCRIBIR ARCHIVO
# ==============================================================================
def escribir_archivo(reporte, grupo, header, lineas, control, fecha, output_dir, contador=1, total_archivos=1, valor_reproceso=1):
    fecha_fmt = fecha
    extension = reporte["extension"] or "txt"
    nombre_archivo = reporte["formato_nombre"] \
        .replace("{FECHA_INICIAL}", fecha_fmt) \
        .replace("{FECHA_FINAL}", fecha_fmt) \
        .replace("{{FORMATO_FECHA}}", fecha_fmt) \
        .replace("{FORMATO_FECHA}", fecha_fmt) \
        .replace("{CANTIDAD_REGISTROS}", str(len(lineas))) \
        .replace("{CONTADOR}", str(contador)) \
        .replace("{TOTAL_ARCHIVOS}", str(total_archivos)) \
        .replace("{VAR_REPROCESO}", str(valor_reproceso)) \
        .replace("{EXTENCION}", extension)

    # Compatibilidad: si FORMATO_NOMBRE_ARCHIVO todavia trae el "_1" fijo heredado
    # de ODI (en vez del placeholder {VAR_REPROCESO}), se reemplaza igual para que
    # el archivo quede unico por reproceso, tal como exige el flujo original.
    if "{VAR_REPROCESO}" not in reporte["formato_nombre"]:
        sufijo_legado = f"_1.{extension}"
        if nombre_archivo.endswith(sufijo_legado):
            nombre_archivo = nombre_archivo[: -len(sufijo_legado)] + f"_{valor_reproceso}.{extension}"

    # V2: ya no se antepone el grupo al nombre del archivo individual.
    # El agrupamiento se mantiene unicamente a nivel del ZIP (ver generar_zip()).
    nombre_archivo = nombre_archivo.replace(" ", "_").replace("/", "_")

    ruta = f"{output_dir}/{nombre_archivo}"
    os.makedirs(output_dir, exist_ok=True)

    encoding = reporte["encoding"] or "UTF-8"
    with open(ruta, "w", encoding=encoding) as f:
        if reporte["flag_encabezado"] and header:
            f.write(header + "\r\n")
        for linea in lineas:
            f.write(linea + "\r\n")
        # if reporte.get("flag_completitud", True) and control:
        #     f.write(control + "\n")

    logger.info("LOGREPORTE: Archivo local: %s (%d regs, %s)", ruta, len(lineas), encoding)

    # NOTA: los archivos individuales NO se suben a GCS. Solo se generan
    # localmente para poder empaquetarlos dentro del ZIP por grupo y del
    # ZIP maestro (generar_master_zip), que es lo unico que se sube a GCS.

    return ruta

# ==============================================================================
# 7. SUBIR A GCS
# ==============================================================================
def subir_gcs(origen, destino):
    if not destino:
        return None
    ruta_completa = f"{destino.rstrip('/')}/{os.path.basename(origen)}"
    cmd = f"gsutil -q cp {origen} {ruta_completa}"
    resultado = subprocess.run(cmd, shell=True, capture_output=True)
    if resultado.returncode == 0:
        logger.info("LOGREPORTE: GCS OK: %s", ruta_completa)
    else:
        logger.error("LOGREPORTE: Error GCS: %s", resultado.stderr.decode())
    return ruta_completa if resultado.returncode == 0 else None

# ==============================================================================
# 8. ZIP POR GRUPO
# ==============================================================================
def generar_zip(grupo, archivos, output_dir, ruta_destino_gcs=None):
    nombre = str(grupo).replace(" ", "_").replace("/", "_") if grupo else "SIN_GRUPO"
    zip_path = f"{output_dir}/{nombre}.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for a in archivos:
            zf.write(a, os.path.basename(a))
    logger.info("LOGREPORTE: ZIP grupo: %s (%d archivos)", zip_path, len(archivos))

    if ruta_destino_gcs:
        subir_gcs(zip_path, ruta_destino_gcs)
    return zip_path

# ==============================================================================
# 9. MASTER ZIP (usa config del maestro)
# ==============================================================================
def generar_master_zip(maestro, todos_archivos, fecha, output_dir, valor_reproceso=1):
    nombre_archivo = maestro["formato_nombre"] \
        .replace("{FECHA_INICIAL}", fecha) \
        .replace("{FECHA_FINAL}", fecha) \
        .replace("{{FORMATO_FECHA}}", fecha) \
        .replace("{FORMATO_FECHA}", fecha) \
        .replace("{CANTIDAD_REGISTROS}", str(len(todos_archivos))) \
        .replace("{CONTADOR}", "1") \
        .replace("{TOTAL_ARCHIVOS}", "1") \
        .replace("{VAR_REPROCESO}", str(valor_reproceso)) \
        .replace("{EXTENCION}", "zip")

    if "{VAR_REPROCESO}" not in maestro["formato_nombre"] and nombre_archivo.endswith("_1.zip"):
        nombre_archivo = nombre_archivo[: -len("_1.zip")] + f"_{valor_reproceso}.zip"

    nombre_archivo = nombre_archivo.replace(" ", "_").replace("/", "_")
    zip_path = f"{output_dir}/{nombre_archivo}"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for a in todos_archivos:
            zf.write(a, os.path.basename(a))
    logger.info("LOGREPORTE: Master ZIP: %s (%d archivos)", zip_path, len(todos_archivos))
    if maestro.get("ruta_destino"):
        subir_gcs(zip_path, maestro["ruta_destino"])
    return zip_path

# ==============================================================================
# MAIN
# ==============================================================================
def main():
    logger.info("=" * 70)
    logger.info("LOGREPORTE: INICIANDO ORQUESTADOR")
    logger.info("LOGREPORTE: Maestro: %s", NOMBRE_REPORTE_MAESTRO)
    logger.info("=" * 70)

    directorio = tempfile.mkdtemp(prefix="reportes_fah_")

    # 1. Leer maestro
    try:
        maestro = obtener_maestro(NOMBRE_REPORTE_MAESTRO)
    except Exception as e:
        logger.error("LOGREPORTE: Error leyendo maestro: %s", e)
        spark.stop()
        sys.exit(1)

    logger.info("LOGREPORTE: Maestro encontrado: %s (%s)", maestro["nombre_reporte"], maestro["id_producto"])

    # 2. Obtener sub-reportes desde el QUERY del maestro
    query_maestro = maestro.get("query_subreportes")
    if not query_maestro:
        logger.error("LOGREPORTE: El maestro no tiene QUERY definido")
        spark.stop()
        sys.exit(1)

    subreportes = obtener_subreportes(query_maestro)
    if not subreportes:
        logger.warning("LOGREPORTE: No se encontraron sub-reportes para procesar")
        spark.stop()
        sys.exit(0)

    logger.info("LOGREPORTE: Sub-reportes a procesar: %d", len(subreportes))

    # 2.5 Calcular VAR_VALIDA_REPROCESO para el periodo/fuente actual
    periodo_reproceso = f"{fecha_ejecucion[:4]}-{fecha_ejecucion[4:6]}-{fecha_ejecucion[6:8]}" if "-" not in fecha_ejecucion else fecha_ejecucion
    valor_reproceso = obtener_valor_reproceso(periodo_reproceso, FUENTE_FAH)

    total_registros_leidos = 0
    total_registros_escritos = 0
    rutas_salida = []
    todos_archivos = []

    # 3. Procesar cada sub-reporte
    grupos = defaultdict(list)
    for rpt in subreportes:
        logger.info("LOGREPORTE: Ejecutando sub-reporte: %s", rpt["nombre_reporte"])
        try:
            fecha_plana = fecha_ejecucion.replace("-", "")
            fecha_iso = f"'{fecha_plana[:4]}-{fecha_plana[4:6]}-{fecha_plana[6:8]}'"
            query = rpt["query"].replace("'$YYYYMMDD'", fecha_iso).replace("$YYYYMMDD", fecha_iso)
            query = query.replace("VAR_VALIDA_REPROCESO_VALUE", str(valor_reproceso))
            df = bq(query)
            grupos_val = [r["GRUPO"] for r in df.select("GRUPO").distinct().collect() if r["GRUPO"]]
            logger.info("LOGREPORTE:   GRUPOS: %s", grupos_val)

            if not grupos_val:
                grupos["SIN_GRUPO"].append({"config": rpt, "df": df})
            else:
                for g in grupos_val:
                    grupos[g].append({"config": rpt, "df": df.filter(F.col("GRUPO") == g)})

        except Exception as e:
            logger.error("LOGREPORTE: Error en sub-reporte %s: %s", rpt["nombre_reporte"], e)

    # 4. Generar archivos por grupo
    total_grupos = len(grupos)
    for grupo_id, items in grupos.items():
        logger.info("LOGREPORTE: --- GRUPO: %s (%d items) ---", grupo_id, len(items))

        for idx, item in enumerate(items, 1):
            rpt = item["config"]
            df_grupo = item["df"]
            total = df_grupo.count()

            if total == 0:
                logger.warning("LOGREPORTE:   %s: sin registros, omitido", rpt["nombre_reporte"])
                continue

            header = generar_header(df_grupo, rpt, grupo_id, total, fecha_ejecucion)
            lineas = generar_lineas(df_grupo, rpt)
            control = generar_control(rpt, grupo_id, total, fecha_ejecucion)

            # {CONTADOR} = valor real de GRUPO al que pertenece este archivo.
            # {TOTAL_ARCHIVOS} = cantidad total de grupos que existen en esta corrida.
            ruta = escribir_archivo(rpt, grupo_id, header, lineas, control,
                                    fecha_archivo, directorio,
                                    contador=grupo_id, total_archivos=total_grupos,
                                    valor_reproceso=valor_reproceso)

            total_registros_leidos += total
            total_registros_escritos += len(lineas)
            rutas_salida.append(ruta)
            todos_archivos.append(ruta)

    # NOTA: ya no se genera un .zip intermedio por grupo (antes se anidaba
    # dentro del ZIP maestro, dejando p.ej. 1.zip/2.zip adentro). Los
    # archivos individuales de cada grupo van directo a todos_archivos y
    # terminan sueltos (no anidados) dentro del ZIP maestro.

    # 5. Master ZIP (con nombre del maestro) - unico artefacto que se sube a GCS
    if todos_archivos:
        master_zip = generar_master_zip(maestro, todos_archivos, fecha_archivo, directorio,
                                         valor_reproceso=valor_reproceso)
        # rutas_salida ya contiene los 4 archivos individuales (para el RESULTADO
        # informativo/log), pero solo master_zip se sube a GCS (ver generar_master_zip).
        rutas_salida.append(master_zip)

        # 5.1 Actualizar log de procesos (solo si el proceso genero archivos exitosamente,
        # equivalente al paso "Actualiza Log de Proceso" de ODI)
        actualizar_log_proceso(periodo_reproceso, FUENTE_FAH, valor_reproceso)
    else:
        logger.warning("LOGREPORTE: No se generaron archivos, omitiendo Master ZIP")
        logger.warning("LOGREPORTE: No se actualiza log de procesos por no haber archivos generados")

    # ==========================================================================
    # RESULTADO FINAL
    # ==========================================================================
    RESULTADO = {
        "FECHA_INICIO": fecha_ejecucion,
        "FECHA_FIN": fecha_final,
        "RUTA_SALIDA": rutas_salida,
        "REGISTROS_LEIDOS": total_registros_leidos,
        "REGISTROS_ESCRITOS": total_registros_escritos
    }

    if len(json.dumps(RESULTADO, ensure_ascii=False)) >= 200000:
        logger.warning("LOGREPORTE: RESULTADO supera 200000 bytes - usando version reducida")
        RESULTADO["RUTA_SALIDA"] = RESULTADO["RUTA_SALIDA"][:1]

    logger.info("LOGREPORTE: RESULTADO = %s", json.dumps(RESULTADO, ensure_ascii=False))

    logger.info("=" * 70)
    logger.info("LOGREPORTE: PROCESO COMPLETADO")
    logger.info("LOGREPORTE:   Maestro: %s", NOMBRE_REPORTE_MAESTRO)
    logger.info("LOGREPORTE:   Sub-reportes: %d", len(subreportes))
    logger.info("LOGREPORTE:   Grupos: %d", len(grupos))
    logger.info("LOGREPORTE:   Registros Leidos: %s", total_registros_leidos)
    logger.info("LOGREPORTE:   Registros Escritos: %s", total_registros_escritos)
    logger.info("LOGREPORTE:   Archivos generados: %d", len(rutas_salida))
    logger.info("=" * 70)

    spark.stop()
    logger.info("LOGREPORTE: Sesion Spark cerrada correctamente.")


if __name__ == "__main__":
    main()