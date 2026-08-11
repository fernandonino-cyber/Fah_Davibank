
import cdo.construcciones.commons.estandar as estandar

nombre_proyecto = "DAVIBANK_FAH"
key_sesion = nombre_proyecto

ruta_principal = "/home/airflow/gcs/data/cdo"
ruta_queries = f"construcciones/{nombre_proyecto}"

correo_notificacion = "test@woombat.com"

parametros_escritura = {
    "nombre_dataset": "cur_administrativa",
    "nombre_tabla": "davibank_fah",
    "tipo_escritura": estandar.TipoTabla.PARTICIONADA_POR_DATE,
    "tipo_particion": estandar.TipoParticionamiento.DIARIA,
    "nombre_columna_particion": "periodo",
    "valor_particion": "{{ params.fecha_proceso }}",
}

queries = {
    "definir_parametros": "1_variables.sql",
    "filtrado": "2_filtrado.sql",
    "transformacion": [
        "3_transformacion_1.sql",
        "4_transformacion_2.sql",
    ],
    "escritura": "5_select_final.sql",
}

etiquetas_generales = {
    "departamento": "cdo",
    "construccion": nombre_proyecto,
    "tipo_de_proyecto": "hub",
    "responsable": "Agente Woombat",
    "periodicidad": "diario",
    "escritura": parametros_escritura["tipo_escritura"].name.lower(),
}

(etiquetas_filtrado, etiquetas_transformacion, etiquetas_escritura) = estandar.crear_etiquetas(etiquetas_generales)

definir_parametros_ejecucion = None

controles_etapa = {
    # Los controles se mantienen como en la plantilla original
}

etiquetas_etapa = {
    "definir_parametros": None,
    "filtrado": etiquetas_filtrado,
    "transformacion": etiquetas_transformacion,
    "escritura": etiquetas_escritura,
}

parametros_dag_ejecutor = {
    "parametros_generales": {
        "key_sesion": key_sesion,
        "ruta_principal": ruta_principal,
        "ruta_queries": ruta_queries,
    },
    "parametros_escritura": parametros_escritura,
    "queries": queries,
    "controles_etapa": controles_etapa,
    "etiquetas_generales": etiquetas_generales,
    "definir_parametros_ejecucion": definir_parametros_ejecucion,
}

parametros_dag_orquestador = {
    "nombre_dataset": parametros_escritura["nombre_dataset"],
    "nombre_tabla": parametros_escritura["nombre_tabla"],
    "dependencias_requeridas": [
        {
            "nombre_dataset": "datalake2-produccion.par_temporal_estandarizacion",
            "nombre_tabla": "DAVIBANK_FAH",
            "tipo_escritura": estandar.TipoTabla.PARTICIONADA_POR_DATE,
            "tipo_fuente": "USAR_INFO_TABLA",
            "periodicidad": "diario",
            "dependencia_requerida": "SI",
        }
    ],
    "tipo_orquestamiento": "REVISAR_ACTUALIZACION_DEPENDENCIAS",
    "parametros_orquestamiento": {
        "nombre_construccion": nombre_proyecto,
        "tipo_escritura": parametros_escritura["tipo_escritura"],
        "regla": estandar.Regla.TODOS_CUMPLEN,
        "orden": "default",
        "periodo_inicio": "None",
        "corrimiento": 0,
        "unidad": "",
        "periodicidad": "diario",
        "inicio_periodicidad": "",
        "periodos_procesar": "TODOS",
        "esperar_fin_periodo": True,
        "dias_espera": 0,
        "formato_crontab": "@daily",
        "libro": "BIFRS_DAV"  # AJUSTE REALIZADO AQUÃ
    },
}