# Informe de Revisión y Comparativa Estructural de DAGs FAH

Este documento presenta el informe técnico comparativo entre el archivo de configuración del DAG de Davibank (`config_davibank_fah.py`) y el archivo de configuración del DAG certificado de CAI GL (`config_cai_gl_fah.py`). El objetivo es evaluar la homogeneidad estructural de ambos DAGs bajo el framework común de desarrollo en Google Cloud Platform (GCP).

---

## 1. Cuadro Comparativo Estructural

A continuación, se detalla la comparativa sección por sección de ambos archivos para analizar la nomenclatura, tipos de variables y estructuras declaradas:

| Componente / Sección | DAG Davibank (`config_davibank_fah.py`) | DAG Certificado CAI GL (`config_cai_gl_fah.py`) | Homogeneidad / Observaciones |
| :--- | :--- | :--- | :--- |
| **Imports** | `import cdo.construcciones.commons.estandar as estandar` | `import cdo.construcciones.commons.estandar as estandar`<br>`from datetime import datetime` | **Homólogo.** CAI GL importa adicionalmente `datetime` pero no altera la estructura del framework. |
| **Parámetros Generales** | `nombre_proyecto = "DAVIBANK_FAH"`<br>`key_sesion = nombre_proyecto`<br>`ruta_principal = "/home/airflow/gcs/data/cdo"`<br>`ruta_queries = f"construcciones/{nombre_proyecto}"`<br>`correo_notificacion = "test@woombat.com"` | `nombre_proyecto = "STG_CAI_GL_FAH"`<br>`key_sesion = nombre_proyecto`<br>`ruta_principal = '/home/airflow/gcs/data/cdo'`<br>`ruta_queries = f'construcciones/{nombre_proyecto}'`<br>`correo_notificacion = "test@woombat.com"` | **Homólogo.** Ambos definen exactamente las mismas variables con tipos idénticos de strings. |
| **Parámetros de Escritura (`parametros_escritura`)** | Dataset: `"cur_administrativa"`<br>Tabla: `"davibank_fah"`<br>Escritura: `estandar.TipoTabla.PARTICIONADA_POR_DATE`<br>Partición: `estandar.TipoParticionamiento.DIARIA`<br>Columna: `"periodo"`<br>Valor Partición: `"{{ params.fecha_proceso }}"` | Dataset: `'par_temporal_estandarizacion'`<br>Tabla: `'CAI_GL_FAH'`<br>Escritura: `estandar.TipoTabla.PARTICIONADA_POR_DATE`<br>Partición: `estandar.TipoParticionamiento.DIARIA`<br>Columna: `"periodo"`<br>Valor Partición: `"periodito"` | **Homólogo.** Comparten la estructura de claves de diccionario del framework. El valor de partición difiere según el orquestador asociado (Jinja en Davibank vs variable en CAI). |
| **Queries (`queries`)** | Mapea sufijos `.sql` en una estructura de lista para transformaciones: `'definir_parametros'`, `'filtrado'`, `'transformacion'` (lista), `'escritura'`. | Mapea nombres de archivos sin sufijo `.sql` para transformaciones: `'definir_parametros'`, `'filtrado'`, `'transformacion'` (lista), `'escritura'`. | **Homólogo.** Ambas estructuras de diccionario son equivalentes y válidas para el ejecutor Airflow. El llamado de scripts propios se mantiene independiente. |
| **Etiquetas Generales** | `departamento`, `construccion`, `tipo_de_proyecto`, `responsable`, `periodicidad`, `escritura`. Generación de etiquetas mediante `estandar.crear_etiquetas`. | `departamento`, `construccion`, `tipo_de_proyecto`, `responsable`, `periodicidad`, `escritura`. Generación de etiquetas mediante `estandar.crear_etiquetas`. | **Homólogo.** Idéntica estructura y lógica de llamado al framework estándar de CDO. |
| **Parámetros de Ejecución (`definir_parametros_ejecucion`)** | Declarado como `None`. | Declarada función con lógica interna para retornar diccionario con `"periodo" : "CURRENT_DATE()"`. | **Compatible.** Ambos métodos de inyección de parámetros son válidos en el framework. Davibank inyecta mediante Jinja del orquestador, por lo que heredar la función de CAI no es necesario. |
| **Controles de Etapa (`controles_etapa`)** | Diccionario vacío: `# Los controles se mantienen como en la plantilla original` | Estructura con validaciones pre/pos de `estandar.TipoControl.CONTEO_REGISTROS` para la fase de escritura. | **Compatible.** Davibank realiza sus cuadres de control y completitud a nivel del reporteador final (`reporte_fah.py`), por lo que no requiere bloqueos por conteo en el DAG ejecutor. |
| **Parámetros Ejecutor (`parametros_dag_ejecutor`)** | Diccionario que encapsula `parametros_generales`, `parametros_escritura`, `queries`, `controles_etapa`, `etiquetas_generales` y `definir_parametros_ejecucion`. | Diccionario que encapsula `parametros_generales`, `parametros_escritura`, `queries`, `controles_etapa`, `etiquetas_generales` y `definir_parametros_ejecucion`. | **Homólogo.** Idéntica estructura requerida por el operador ejecutor de Airflow de Woombat. |
| **Parámetros Orquestador (`parametros_dag_orquestador`)** | Dataset/Tabla, dependencias requeridas (tabla de estandarización), tipo de orquestamiento string plano `"REVISAR_ACTUALIZACION_DEPENDENCIAS"`, parámetros con campo de negocio `"libro": "BIFRS_DAV"`. | Dataset/Tabla, sin dependencias, tipo de orquestamiento `estandar.TipoOrquestamiento.REVISAR_SIGUIENTE_EJECUCION`, parámetros de ordenamiento asc y corrimiento de días. | **Homólogo / Diferencias de Negocio.** Comparten la misma estructura de claves. Las diferencias de valores (como el campo `libro` o la regla de dependencias) son particularidades de negocio de Davibank que deben conservarse. |

---

## 2. Diferencias Clave Identificadas

Aunque la estructura de variables y la arquitectura de configuración es homóloga, existen tres diferencias funcionales derivadas del diseño específico de cada proceso:

1.  **Mapeo de Queries:**
    *   **CAI GL:** Mapea nombres de scripts sin extensión (ej: `'1_variables'`).
    *   **Davibank:** Mapea nombres de scripts incluyendo la extensión `.sql` (ej: `'1_variables.sql'`).
    *   *Justificación:* Cada proceso busca los archivos tal como están almacenados en GCS. Se deben mantener como están para no romper la ejecución de los scripts de cada proceso.

2.  **Tipo de Orquestamiento y Dependencias:**
    *   **CAI GL:** Utiliza `estandar.TipoOrquestamiento.REVISAR_SIGUIENTE_EJECUCION` y no tiene dependencias requeridas.
    *   **Davibank:** Utiliza un string plano `"REVISAR_ACTUALIZACION_DEPENDENCIAS"` y tiene una dependencia crítica hacia `datalake2-produccion.par_temporal_estandarizacion.DAVIBANK_FAH`.
    *   *Justificación:* El proceso contable de Davibank requiere esperar obligatoriamente que la tabla de estandarización externa esté actualizada antes de iniciar, por lo que esta regla de negocio es indispensable.

3.  **Controles de Etapa (`controles_etapa`):**
    *   **CAI GL:** Implementa controles pre y pos-escritura para contar registros en BigQuery.
    *   **Davibank:** No implementa controles directos en el ejecutor de Airflow ya que delega la cuadratura del balanceo y la generación al PySpark `reporte_fah.py` (el cual genera el reporte de control físico `.txt` del ZIP).

---

## 3. Conclusiones y Recomendaciones

*   **No se requieren ajustes de estructura:** La configuración del DAG ejecutor de Davibank (`config_davibank_fah.py`) ya es completamente compatible y homóloga con el estándar del framework de GCP certificado en `config_cai_gl_fah.py`.
*   **Aislamiento de Lógica:** Las diferencias encontradas (dependencias, variables Jinja y campos como `libro`) son requerimientos esenciales de negocio para Davibank. Modificarlas para asemejarse a CAI GL causaría fallas operacionales y de cuadre contable.
*   **Decisión Final:** Conforme a la directriz ("*Si en el análisis no ves diferencias relevantes, no sería necesario ajustar el Dag*"), **se determina mantener `config_davibank_fah.py` en su estado actual**, garantizando así la estabilidad y correcto funcionamiento del proceso de integración contable de Davibank.
