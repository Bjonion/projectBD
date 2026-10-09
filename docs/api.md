# Consultas geoespaciales y API Flask

La API escucha en `http://127.0.0.1:5000`. Consulta `projectbd.accidents`, con
GeoJSON Point y el índice `location_2dsphere`, y las cuatro colecciones Spark.
No recibe ni lee credenciales Kaggle o GitHub.

| Endpoint | Parámetros | Consulta y respuesta |
| --- | --- | --- |
| GET /health | Ninguno | Ping real MongoDB, 200 o 503 |
| GET /accidents/nearby | latitude, longitude, radius, limit opcional | `$near`: documentos del más cercano al más lejano |
| POST /accidents/within | JSON con polygon y limit opcional | `$geoWithin`: documentos dentro del polígono, sin orden de distancia |
| GET /accidents/nearby-summary | latitude, longitude, radius | `$geoNear` seguido de `$group`: cantidad y distancias mínima, máxima y media |
| GET /aggregates/&lt;kind&gt; | kind: grid/hours/months/hotspots; limit y offset opcionales | Resultados publicados por Spark |

`latitude` está entre -90 y 90; `longitude`, entre -180 y 180. `radius` se
expresa en metros y admite valores de 0,001 a 20.040.000. La API construye
los puntos como `[longitud, latitud]`. `limit` es entero de 1 a 500, por defecto
100. `offset` permite recorrer los agregados y va de 0 a 1.000.000.

El resumen calcula todos los registros dentro del radio; no aplica el límite
del listado. Si no hay coincidencias, devuelve count=0 y distancias null.
Los listados incluyen `returned`, que cuenta la respuesta, no el total de
coincidencias. Cada consulta tiene un máximo de cinco segundos de ejecución
en MongoDB; el timeout y otros fallos del servicio devuelven 503 sin detalles
internos. Los parámetros inválidos devuelven 400; un cuerpo mayor de 256 KiB,
413. Solo se aceptan colecciones Spark enumeradas en el código.

El polígono debe ser una geometría GeoJSON `Polygon`, con posiciones numéricas
de dos coordenadas, anillos cerrados y al menos tres vértices distintos.
Se admiten anillos interiores. MongoDB valida su geometría esférica. Solo se
trasladan tipo y coordenadas a la consulta, evitando aceptar operadores o un
CRS arbitrario. Las regiones que cruzan el antimeridiano o abarcan más de un
hemisferio requieren cuidado: esta API conserva la interpretación esférica
estándar de MongoDB, adecuada para las consultas locales del dataset.

## Ejemplos

```sh
curl 'http://127.0.0.1:5000/accidents/nearby?latitude=40&longitude=-73&radius=1000&limit=10'
curl 'http://127.0.0.1:5000/accidents/nearby-summary?latitude=40&longitude=-73&radius=1000'
curl 'http://127.0.0.1:5000/aggregates/hotspots?limit=20'
curl -X POST http://127.0.0.1:5000/accidents/within \
  -H 'Content-Type: application/json' \
  -d '{"polygon":{"type":"Polygon","coordinates":[[[-74,39],[-72,39],[-72,41],[-74,41],[-74,39]]]},"limit":10}'
```

Para construir, iniciar y verificar esta etapa:

```sh
docker compose build api
docker compose up -d --wait api
docker compose exec -T api python -m pytest -q -p no:cacheprovider /app/tests/test_api.py
docker compose exec -T api python /app/scripts/smoke_api.py
```

Las pruebas pytest crean una base temporal, con índice 2dsphere y puntos a
distancias conocidas. Verifican radio en metros, orden de cercanía, cambio de
radio, resumen de distancias, resultado vacío, inclusión y exclusión mediante
polígono con hueco, parámetros inválidos y paginación de agregados. La base
temporal se elimina al terminar. Las pruebas HTTP usan el servicio Gunicorn y
las colecciones reales: buscan una ubicación existente y prueban todos los
endpoints. El pipeline ejecuta ambas y archiva `api-smoke-report.json`.

Referencias oficiales: [$near](https://www.mongodb.com/docs/v7.0/reference/operator/query/near/),
[$geoWithin](https://www.mongodb.com/docs/v7.0/reference/operator/query/geoWithin/)
y [$geoNear](https://www.mongodb.com/docs/v7.0/reference/operator/aggregation/geoNear/).

## Validación realizada

La ejecución 5 del job Jenkins `projectbd-ingestion` terminó con **SUCCESS**:
tres pruebas de ingesta, dos Spark y tres API aprobadas. También terminaron
correctamente las pruebas HTTP contra Gunicorn y MongoDB con la muestra real.
Reporte archivado: [api-result.json](api-result.json).

La ubicación elegida dinámicamente devolvió tres accidentes en un radio de
200 metros, un resumen con la misma cantidad y dos accidentes dentro del
polígono de prueba. Se recuperaron diez grupos de cada colección Spark; todos
compartían el identificador de la nueva ejecución de procesamiento. Este job
repitió la ingesta y las agregaciones; el reporte histórico de la etapa Spark
corresponde a la ejecución 4.

Pytest avisó que no podía escribir su caché en `/app`, que pertenece a root en
la imagen Flask. Se desactivó esa caché en el comando de prueba y se verificó
de nuevo: las tres pruebas API pasaron sin avisos. El cambio afecta únicamente
la ejecución de pytest, no las consultas ni el servicio.

El token Kaggle y la contraseña Jenkins no aparecen en el log, el reporte ni
los archivos publicables. El archivo del enunciado sigue excluido de Git.
Este commit de API permanece pendiente de revisión y aprobación de Julian.
