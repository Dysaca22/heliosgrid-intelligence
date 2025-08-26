# HeliosGrid Intelligence

## Proyecto

### Descripción
HeliosGrid Intelligence es una plataforma de monitoreo y predicción de la producción energética en tiempo real, diseñada para operadores de parques solares. El proyecto se enfoca en el concepto de **"Data as a Product" (DaaP)**, entregando datos limpios, agregados y predictivos a través de una API para optimizar las operaciones de la red.

### Problema
La naturaleza intermitente de la energía solar presenta un desafío significativo para los operadores de redes eléctricas, quienes deben equilibrar la oferta y la demanda en tiempo real para garantizar la estabilidad de la red y evitar apagones. Adicionalmente, la incapacidad de predecir fallos en los paneles solares conduce a costosas estrategias de mantenimiento reactivo.

### Solución
La plataforma ingiere datos de sensores de parques solares (irradiancia, temperatura de paneles, producción energética) en tiempo real. Este flujo de datos es procesado para alimentar un dashboard de monitoreo en vivo y un modelo de predicción a corto plazo. Esto permite a los operadores anticipar la producción energética y programar el mantenimiento de forma proactiva, transformando datos brutos en inteligencia accionable.

### Propuesta de Valor
- **Reducción de costos operativos** a través del mantenimiento predictivo y la optimización de recursos.
- **Aumento de la estabilidad de la red eléctrica** al proporcionar pronósticos de producción fiables.
- **Maximización de la rentabilidad** de los activos de energía renovable.

## Objetivos de Aprendizaje

Este proyecto es una oportunidad para profundizar y demostrar habilidades en **Ingeniería de Datos** y **MLOps**, cubriendo las siguientes áreas clave:

- **Ingeniería de Datos en Tiempo Real:** Gestión y procesamiento de flujos de datos continuos con baja latencia.
- **Procesamiento de Big Data:** Aplicación de herramientas y técnicas para manejar grandes volúmenes de datos de manera eficiente y escalable.
- **Orquestación de Pipelines de Datos:** Automatización y monitoreo de flujos de trabajo complejos con herramientas estándar de la industria como Apache Airflow.
- **Arquitectura Cloud-Native:** Diseño y construcción de una infraestructura escalable y desacoplada, aplicando principios de la nube en un entorno local.
- **Modelado Predictivo con Series Temporales:** Desarrollo de modelos para pronosticar la producción energética basándose en datos históricos.

## Propuesta Técnica

Se construirá un stack de datos completo de forma local, utilizando Docker Compose para orquestar todos los servicios de manera cohesionada.

### Stack Tecnológico

- **Containerización: Docker y Docker Compose**
    - **Justificación:** Permite definir y ejecutar un entorno de desarrollo multi-contenedor completo con un solo comando (`docker-compose up`). Replica un entorno de producción, asegurando consistencia y portabilidad.
- **Ingesta de Datos: Apache Kafka**
    - **Justificación:** Estándar de la industria para la ingesta de datos en streaming de alto rendimiento. Desacopla los productores de datos de los consumidores, proporcionando resiliencia y escalabilidad.
- **Procesamiento en Tiempo Real: Servicio en Python**
    - **Justificación:** Para mantener la simplicidad y el control, se desarrollará un servicio ligero en Python con `kafka-python`. Este enfoque, en lugar de usar un framework pesado como Spark, permite un manejo explícito de la lógica de procesamiento de streams, la gestión de estado y la tolerancia a fallos.
- **Almacenamiento de Datos: PostgreSQL**
    - **Justificación:** Una base de datos relacional de código abierto, robusta y potente, ideal para almacenar los datos procesados y servir como backend para análisis y visualización. Su soporte para consultas analíticas complejas es clave para el proyecto.
- **Orquestación: Apache Airflow**
    - **Justificación:** Se utilizará la configuración oficial de Docker Compose para desplegar Airflow localmente. Servirá para orquestar, programar y monitorear todos los pipelines de datos, desde la ingesta hasta el entrenamiento del modelo.
- **Visualización: Power BI**
    - **Justificación:** Herramienta líder para la visualización de datos. Se usará su versión de escritorio (gratuita) para conectarse a PostgreSQL y construir un dashboard interactivo. El informe final se podrá publicar en la web.

### Flujo de Información

El sistema funcionará como un pipeline de datos continuo, desde la generación hasta la visualización y predicción. A continuación se detalla el flujo paso a paso:

1.  **Generación de Datos (Productor):** Un script de Python (`producer.py`) simula los sensores de un parque solar. Genera datos de irradiancia, temperatura y producción energética a intervalos regulares y los envía como mensajes a un topic específico en Apache Kafka.

2.  **Ingesta (Kafka):** Kafka actúa como el sistema nervioso central del pipeline. Recibe los mensajes del productor y los almacena de forma duradera y ordenada en un topic. Esto desacopla la generación de datos del procesamiento, permitiendo que múltiples consumidores lean los datos de forma independiente y a su propio ritmo.

3.  **Procesamiento (Consumidor):** Un servicio de Python (`consumer.py`) se suscribe al topic de Kafka. Lee los mensajes en micro-lotes, realiza las transformaciones necesarias (limpieza, validación de tipos, agregaciones simples) y prepara los datos para su almacenamiento.

4.  **Almacenamiento (PostgreSQL):** El consumidor inserta los datos procesados y limpios en una tabla dentro de la base de datos PostgreSQL. Esta base de datos se convierte en la fuente de verdad (`source of truth`) para todos los datos históricos y en tiempo real.

5.  **Orquestación (Airflow):** Apache Airflow supervisa y gestiona todo el proceso. Un DAG (Grafo Acíclico Dirigido) define las dependencias y la programación de las tareas. Por ejemplo, puede iniciar el productor y el consumidor, realizar verificaciones de calidad de datos periódicas y, más adelante, orquestar el reentrenamiento del modelo predictivo.

6.  **Visualización (Power BI):** Power BI se conecta directamente a la base de datos PostgreSQL. A través de consultas, extrae los datos para alimentar un dashboard interactivo. Los operadores pueden visualizar métricas clave en tiempo real, como la producción actual, la eficiencia de los paneles y las tendencias históricas.

7.  **Predicción (ML Pipeline en Airflow):** En la fase final, Airflow orquestará una tarea adicional que:
    -   Lee los datos históricos desde PostgreSQL.
    -   Entrena un modelo de series temporales (ej. Prophet).
    -   Genera pronósticos de producción energética para las próximas horas/días.
    -   Guarda estas predicciones en una nueva tabla en PostgreSQL, que también será visualizada en el dashboard de Power BI.

### Plan de Acción

- **Fase 1: Configuración del Entorno e Ingesta de Datos**
    - Definir y configurar todos los servicios (Kafka, Zookeeper, PostgreSQL, Airflow) en un archivo `docker-compose.yml`.
    - Desarrollar un script productor en Python que simule datos de sensores y los publique en un topic de Kafka.
- **Fase 2: Procesamiento en Tiempo Real y Almacenamiento**
    - Desarrollar un script consumidor en Python que lea los datos del topic de Kafka, realice transformaciones y agregaciones en micro-lotes, y persista los resultados en la base de datos PostgreSQL.
- **Fase 3: Orquestación del Pipeline con Airflow**
    - Crear un DAG de Airflow para automatizar el flujo de trabajo completo. Este DAG orquestará la ejecución de los scripts de productor y consumidor, e incluirá tareas para la validación de la calidad de los datos.
- **Fase 4: Visualización de Datos**
    - Conectar Power BI Desktop a la base de datos PostgreSQL.
    - Diseñar y construir un dashboard de monitoreo en tiempo real.
    - Publicar el informe en el servicio de Power BI para compartirlo.
- **Fase 5: Modelado Predictivo**
    - Integrar el entrenamiento de un modelo de series temporales (ej. ARIMA, Prophet) como una tarea en el DAG de Airflow.
    - El script leerá datos históricos de PostgreSQL, generará predicciones y las guardará en una tabla dedicada. El dashboard se actualizará para incluir estas predicciones.
