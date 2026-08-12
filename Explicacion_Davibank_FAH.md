# Interpretación de la Configuración y Pipeline SQL de Davibank (FAH)

Este documento contiene la interpretación detallada del archivo `config_davibank_fah.py` (el archivo de configuración del DAG ejecutor) y la secuencia de scripts SQL que componen el flujo de datos para **DAVIBANK_FAH**.

---

## 1. Interpretación de `config_davibank_fah.py`

Este archivo es la configuración para el orquestador y ejecutor de **Airflow** que define cómo se ejecuta el proceso ETL para Davibank FAH en Google Cloud Platform (GCP), utilizando BigQuery y Google Cloud Storage (GCS).

### Componentes Clave:
*   **Identificación del Proyecto (`nombre_proyecto` / `key_sesion`):**
    Establece el nombre como `"DAVIBANK_FAH"`, el cual se utiliza para nombrar la sesión y estructurar las rutas de almacenamiento.
*   **Rutas de Trabajo (`ruta_principal` / `ruta_queries`):**
    *   La ruta base del almacenamiento es `/home/airflow/gcs/data/cdo` (directorio de datos montado en GCS).
    *   Las consultas SQL se cargan de la ruta `construcciones/DAVIBANK_FAH`.
*   **Correo de Notificación (`correo_notificacion`):**
    Establece el contacto de notificaciones para las tareas del pipeline como `test@woombat.com`.
*   **Parámetros de Escritura (`parametros_escritura`):**
    Configura que el destino final sea un dataset de BigQuery llamado `cur_administrativa` en la tabla `davibank_fah`. Esta tabla es de tipo **particionada diariamente por la columna `periodo`**, utilizando la variable `@fecha_proceso` (o parámetro de ejecución de Airflow `{{ params.fecha_proceso }}`).
*   **Mapeo de Queries (`queries`):**
    Asocia las fases lógicas del DAG con los respectivos scripts SQL alojados en la carpeta `Scripts/`:
    *   **Definir Parámetros:** `1_variables.sql`
    *   **Filtrado:** `2_filtrado.sql`
    *   **Transformación:** Ejecuta dos scripts secuencialmente: `3_transformacion_1.sql` y `4_transformacion_2.sql`.
    *   **Escritura:** `5_select_final.sql` (Inserta en la tabla de BigQuery).
*   **Metadatos y Etiquetas (`etiquetas_generales` y `etiquetas_etapa`):**
    Define tags de gobierno de datos para BigQuery y Airflow (departamento, periodicidad diaria, responsable "Agente Woombat", etc.).
*   **Parámetros de Orquestación (`parametros_dag_orquestador`):**
    Indica que el orquestador tiene un tipo de ejecución `REVISAR_ACTUALIZACION_DEPENDENCIAS`.
    *   **Dependencia:** Espera que la tabla externa `datalake2-produccion.par_temporal_estandarizacion.DAVIBANK_FAH` esté actualizada para el día correspondiente.
    *   **Periodicidad:** Diaria (`@daily`).
    *   **Libro Contable:** Está configurado para el libro `"BIFRS_DAV"`.

---

## 2. Flujo y Explicación de los Scripts SQL (`Scripts/`)

Los scripts SQL transforman los datos contables de origen desde la ingesta cruda en BigQuery hasta un formato estandarizado adecuado para la generación de reportes regulatorios/maestros.

### 1️⃣ `1_variables.sql` (Fase de Variables)
*   **Función:** Declara las variables globales en BigQuery que serán inyectadas por Airflow:
    *   `fecha_proceso DATE`: El periodo o día de negocio que se está procesando.
    *   `libro STRING`: El libro contable de destino (por ejemplo, `'BIFRS_DAV'`).

### 2️⃣ `2_filtrado.sql` (Fase de Filtrado)
*   **Función:** Crea o reemplaza una tabla de trabajo física particionada por el periodo llamada `work_davibank_filtrado`.
*   **Lógica:** Extrae únicamente los registros de la tabla origen `trf_davibank_fah` cuyo `PERIODO` coincide con `@fecha_proceso`. Esto aísla la carga diaria y mejora el rendimiento de BigQuery al evitar procesar históricos innecesarios.

### 3️⃣ `3_transformacion_1.sql` (Cálculos y Homologación)
Este script realiza las transformaciones pesadas de lógica contable, calculando consecuenciales y estructurando las transacciones:
*   **Cálculo de Código de Reproceso (`LogInfo`):**
    Consulta la tabla `STG_LOG_PROCESOS_FAH_PRUEBA` para ver si la fecha actual ya se había ejecutado. Genera una variable secuencial (`CodigoReproceso`) que incrementa por cada reproceso, asegurando que los IDs de transacciones sean únicos entre ejecuciones.
*   **Unpivot de Débitos y Créditos (`UnpivotedData`):**
    La contabilidad origen usualmente tiene columnas para débito (`vlrdeb`) y crédito (`vlrcre`). El script las convierte en filas individuales ("unpivot").
    *   Si hay un valor de débito (`vlrdeb > 0`), crea una fila con `naturaleza = '1'`.
    *   Si hay un valor de crédito (`vlrcre > 0`), crea otra fila con `naturaleza = '2'`.
*   **Estructuración de Transacciones (`transformacion_base`):**
    *   Genera un `contador_linea` único para cada movimiento dentro de una misma transacción contable.
    *   Genera el `codigo_transaccion` concatenando: `tipo_de_comprobante ('AK')` + `fecha_efectiva` + `fecha_posteo` + `consecutivo` (rellenado con ceros a la izquierda) + `CodigoReproceso`. Esto produce una clave única por grupo transaccional.
*   **Homologación de Cuentas Contables:**
    Realiza un `LEFT JOIN` con la tabla `mnl_homologaciones_fah`. Si la cuenta original de Davibank tiene una equivalencia homologada en BigQuery, se reemplaza con `VALOR_SALIDA`, de lo contrario, conserva la cuenta original.
*   **Salida:** Los resultados se guardan en la tabla particionada temporal `work_davibank_transformacion_1`.

### 4️⃣ `4_transformacion_2.sql` (Fase de Balanceo de Archivos)
Para evitar archivos de salida excesivamente gigantescos o desbalanceados que dificulten su descarga o procesamiento en sistemas externos:
*   **Lógica de Umbral (150,000 registros):**
    *   Cuenta cuántos registros tiene cada `codigo_transaccion`.
    *   **Grupos Grandes:** Si una sola transacción supera los 150,000 registros, se le asigna su propio número de grupo exclusivo (`grupo_salida`).
    *   **Grupos Pequeños:** Si las transacciones son menores a 150,000 registros, se suman secuencialmente y se agrupan en lotes balanceados de máximo 150,000 registros mediante operaciones matemáticas (`FLOOR` / `SUM OVER`).
*   **Salida:** Guarda el set de datos transformados original pero ahora anexando la columna `grupo_salida` en `work_davibank_transformacion_2`.

### 5️⃣ `5_select_final.sql` (Fase de Escritura)
*   **Función:** Inserta de forma definitiva y limpia todos los campos calculados, homologados y balanceados en la tabla de BigQuery `cur_administrativa.DAVIBANK_FAH` para el periodo actual. Esta tabla queda disponible para que el generador de reportes (ej. `reporte_fah.py`) lea los grupos de salida y empaquete los archivos txt/csv correspondientes.
