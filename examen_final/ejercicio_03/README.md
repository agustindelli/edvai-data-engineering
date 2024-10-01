# Google Skills Boost - Examen Final

## LAB:

Realizar el siguiente LAB, al finalizar pegar un print screen donde se ve su perfil y el progreso final verificado

Título: Creating a Data Transformation Pipeline with Cloud Dataprep

Schedule: 1 hour 15 minutes

Cost: 5 Credits

Link: https://www.cloudskillsboost.google/focuses/4415?catalog_rank=%7B%22rank%22%3A1%2C%22num_filters%22%3A0%2C%22has_search%22%3Atrue%7D&parent=catalog&search_id=32278924

### Print screen:

![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_03/captures/capture.png)

## Contestar las siguientes preguntas:

###  1. ¿Para que se utiliza data prep?
DataPrep es un servicio inteligente de datos que facilita la exploración, limpieza, transformación y preparación de datos para su visualización o aprendizaje automático, sin necesidad de tener conocimientos de programación, lo que significa que no es necesario escribir código. Esto permite a los usuarios realizar estos procesos de manera más sencilla.

### 2. ¿Qué cosas se pueden realizar con DataPrep?
Con DataPrep, se puede explorar y analizar, limpiar datos, transformar datos, integrar fuentes de datos, automatizar flujos de trabajo, entre otras.

### 3. ¿Por qué otra/s herramientas lo podrías reemplazar? Por qué?
Se podria reemplazar por Databricks, por ejemplo combina procesamiento de datos, análisis, y aprendizaje automático en una única plataforma. 

### 4. ¿Cuáles son los casos de uso comunes de Data Prep de GCP?
Algunos casos comunes de uso de DataPrep son: preparación de datos para análisis, ETL (Extracción, Transformación, Carga), integración y unión de datos de múltiples fuentes, preparación de datos para modelos de aprendizaje automático, entre otros.

### 5. ¿Cómo se cargan los datos en Data Prep de GCP?
Se pueden cargar datos en DataPrep conectándose a varias fuentes de manera muy sencilla, como por ejemplo: Google Cloud Storage y BigQuery.

### 6. ¿Qué tipos de datos se pueden preparar en Data Prep de GCP?
Se puede preparar datos estructurados, como archivos CSV, Parquet, Avro y tablas de bases de datos, así como también datos no estructurados.

### 7. ¿Qué pasos se pueden seguir para limpiar y transformar datos en Data Prep de GCP?
En primer lugar cargar los datos desde diversas fuentes, como Google Cloud Storage o BigQuery. Una vez que los datos están cargados, se debe explorar el conjunto utilizando herramientas de perfilado de datos para identificar problemas de calidad, como valores faltantes o duplicados y asi aplicar las transformaciones necesarias.

### 8. ¿Cómo se pueden automatizar tareas de preparación de datos en Data Prep de GCP?
Una opción es exportar flujos de trabajo como pipelines que se ejecutan en Google Cloud Dataflow, lo que permite programar estas tareas de preparación.

### 9. ¿Qué tipos de visualizaciones se pueden crear en Data Prep de GCP?
DataPrep permite crear diversas visualizaciones, como gráficos de barras, pie charts, y otras representaciones visuales que ayudan a comprender los datos y sus patrones.

### 10. ¿Cómo se puede garantizar la calidad de los datos en Data Prep de GCP?
Se puede garantizar mediante la aplicación de reglas de calidad para validar los datos y el monitoreo.

## Arquitectura:

El gerente de Analitca te pide realizar una arquitectura hecha en GCP que contemple el uso de esta herramienta ya que le parece muy fácil de usar y una interfaz visual que ayuda a sus
desarrolladores ya que no necesitan conocer ningún lenguaje de desarrollo.
Esta arquitectura debería contemplar las siguiente etapas:

- Ingesta: datos parquet almacenados en un bucket de S3 y datos de una aplicación que guarda sus datos en Cloud SQL.

- Procesamiento: filtrar, limpiar y procesar datos provenientes de estas fuentes
Almacenar: almacenar los datos procesados en BigQuery.

- BI: herramientas para visualizar la información almacenada en el Data Warehouse.

- ML: Herramienta para construir un modelo de regresión lineal con la información almacenada
en el Data Warehouse.

![alt text](https://github.com/agustindelli/edvai-data-engineering/blob/main/examen_final/ejercicio_03/captures/architecture.png)
