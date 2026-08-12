import cdo.construcciones.commons.estandar as estandar
from datetime import datetime

# -------------------------------
# -------------------------------
# DEFINICION DE LA CONSTRUCCION
# -------------------------------
# -------------------------------

# 2. Configuracion
## Parametros generales
nombre_proyecto = "STG_CAI_GL_FAH" # Debe ser el mismo nombre de la tabla a crear
key_sesion = nombre_proyecto # usaremos una sesion con el mismo nombre del dag 
ruta_principal = '/home/airflow/gcs/data/cdo'
ruta_queries = f'construcciones/{nombre_proyecto}'
# feat 2025-09-25: CreaciÃ³n de la funcionalidad de envÃ­o de correos.
correo_notificacion = "test@woombat.com"

## Parametros de Ejecucion
# Por favor configure los siguientes parametros:

# Definir parametros de escritura
parametros_escritura = {
    "nombre_dataset": 'par_temporal_estandarizacion',
    "nombre_tabla": 'CAI_GL_FAH',
    "tipo_escritura": estandar.TipoTabla.PARTICIONADA_POR_DATE,
    "tipo_particion": estandar.TipoParticionamiento.DIARIA,
    "nombre_columna_particion": "periodo",
    "valor_particion": "periodito"
}

# Agregue en esta lista la queries de acuerdo a su funcion
queries = {
    'definir_parametros': '1_variables',
    'filtrado': '2_filtrado',
    'transformacion': ['3_transformacion_1','3_transformacion_2','3_transformacion_3'],
    'escritura': '4_select_final'
}

# Definir etiquetas para las distintas fases del proceso. Por favor
# solo utilizar caracteres minusculas, numericos y los caracteres especiales "_" y  "-".
etiquetas_generales = {
    "departamento" : "cdo",
    "construccion" : nombre_proyecto,
    "tipo_de_proyecto" : "hub", # hub / caso de uso / proyecto
    "responsable" : "Agente Woombat",
    # Agregar valor de periodicidad de ejecuciÃ³n: "diaria", "semanal", "mensual", "anual", "otro".  
    "periodicidad" : "diario",
    # NO MODIFICAR LOS SIGUIENTES VALORES.
    "escritura" : parametros_escritura["tipo_escritura"].name.lower(),
}

etiquetas_filtrado, etiquetas_transformacion, etiquetas_escritura = estandar.crear_etiquetas(etiquetas_generales)

# UNICAMENTE VALIDO PARA EJECUCIONES MANUALES O EJECUCIONES DE REPROCESOS.
def definir_parametros_ejecucion(**context):
    # INCLUIR AQUI LA LOGICA DE LA FUNCION
    periodo = "CURRENT_DATE()"
    parametros_ejecucion = {
        "periodo" : periodo
    }
    return parametros_ejecucion

# DEFINICION DE CONTROLES POR ETAPA 
controles_etapa = {
    'definir_parametros': {'pos': [], 'pre': []},
    'filtrado': {'pos': [], 'pre': []},
    'transformacion': {'pos': [], 'pre': []},
    'escritura': {
        'pos': [
            {
                'accion': estandar.TipoControl.CONTEO_REGISTROS,
                'parametros': {
                    'key_num_rows': estandar.constants.KEY_POS_NUM_ROWS_FINAL_TABLE,
                    'callback_nombre_tabla': estandar.obtener_nombre_tabla_final_desde_contexto
                }
            },
            {
                'accion': estandar.TipoControl.CONTEO_REGISTROS,
                'parametros': {
                    'key_num_rows': estandar.constants.KEY_NUM_ROWS_TEMP_FINAL_TABLE_NAMES,
                    'callback_nombre_tabla': estandar.obtener_nombre_tabla_temp_select_final_desde_contexto
                }
            }
        ],
        'pre': [
            {
                'accion': estandar.TipoControl.CONTEO_REGISTROS,
                'parametros': {
                    'key_num_rows': estandar.constants.KEY_PRE_NUM_ROWS_FINAL_TABLE,
                    'callback_nombre_tabla': estandar.obtener_nombre_tabla_final_desde_contexto
                }
            }
        ]
    }
}
etiquetas_etapa = {
    "definir_parametros" : None,
    "filtrado" : etiquetas_filtrado,
    "transformacion" : etiquetas_transformacion,
    "escritura" : etiquetas_escritura,
}
# EXPONER PARAMETROS DE EJECUCION
parametros_dag_ejecutor = {
    "parametros_generales" : {
        "key_sesion" : key_sesion,
        "ruta_principal" : ruta_principal,
        "ruta_queries" : ruta_queries,
    }, 
    "parametros_escritura" : parametros_escritura, 
    "queries" : queries, 
    "controles_etapa" : controles_etapa,
    "etiquetas_generales" : etiquetas_generales,
    "definir_parametros_ejecucion" : definir_parametros_ejecucion,
}

# -------------------------------
# -------------------------------
# DEFINICION DEL ORQUESTADOR
# -------------------------------
# -------------------------------
# EXPONER PARAMETROS DE EJECUCION

parametros_dag_orquestador = {
    "nombre_dataset" : parametros_escritura['nombre_dataset'],
    "nombre_tabla" : parametros_escritura['nombre_tabla'],
    "dependencias_requeridas" : [],
    "tipo_orquestamiento" : estandar.TipoOrquestamiento.REVISAR_SIGUIENTE_EJECUCION,
    "parametros_orquestamiento" : {
        "nombre_construccion": nombre_proyecto,
        "tipo_escritura" : parametros_escritura["tipo_escritura"],
        "regla" : estandar.Regla.TODOS_CUMPLEN,
        "orden" : 'asc',
        "periodo_inicio": '2023-01-01',
        "corrimiento": 1,
        "unidad": 'DIAS',
        "periodicidad" : "diario",
        "inicio_periodicidad" : "",
        "periodos_procesar" : "TODOS",
        "esperar_fin_periodo" : True,
        "dias_espera" : 0,
        "formato_crontab" : '@daily',
    }
}