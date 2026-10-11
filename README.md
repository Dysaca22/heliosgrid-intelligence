# HeliosGrid Intelligence

Monitoreo de un parque solar en streaming: sensores simulados con datos reales de irradiancia, Kafka, PostgreSQL y un tablero de Power BI.

## Problema

La producción solar sube y baja con las nubes. El operador de red necesita verla en tiempo real para equilibrar oferta y demanda. Sin esa visibilidad, el mantenimiento de los paneles llega tarde.

## Datos

National Solar Radiation Database (NSRDB) del NREL: un punto, el año 2022 completo, 8.760 registros horarios. Incluye irradiancia (GHI, DNI, DHI), temperatura, viento, presión, humedad, tipo de nube y ángulo cenital.

El CSV no viene en el repositorio. Se descarga del [visor del NSRDB](https://nsrdb.nrel.gov/data-viewer) y va en `producer/data/`.

## Método

1. `producer/`: limpia el CSV y publica un registro por segundo en el topic `solar-power-data` de Kafka. La limpieza descarta columnas con más de la mitad vacía y corrige los −9999 del ángulo cenital.
2. `consumer/`: lee el topic y escribe cada registro en la tabla `solar_data` de PostgreSQL. Reintenta la conexión si la base aún no está lista.
3. `dashboard.pbix`: tablero de Power BI conectado a PostgreSQL.
4. Docker Compose levanta Kafka, Zookeeper, PostgreSQL, Airflow, el productor y el consumidor.

El análisis exploratorio previo está en `producer/EDA.ipynb`.

## Resultado

Está hecho el tramo del CSV al tablero: Kafka, PostgreSQL y Power BI, todo levantado con Docker Compose.

Falta lo que [el plan](docs/plan.md) dejó para después: los DAG de Airflow (la carpeta `dags/` está vacía) y el pronóstico de producción.

## Cómo correrlo

Requiere Docker y el CSV del NSRDB en `producer/data/`.

```sh
cp .env.example .env    # ajustar puertos, credenciales y DATA_FILE_PATH
docker compose up --build
```
