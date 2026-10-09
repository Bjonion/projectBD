# Procesamiento distribuido con Spark

La entrada es `projectbd.accidents`, la muestra de un millón de documentos
publicada por Dask. Spark 3.5.8 lee y escribe con MongoDB Spark Connector 10.7.0.
El maestro es `spark://spark-master:7077`; las tareas se ejecutan en el worker.

## Definición de las agregaciones

- `spark_grid`: conteo por celda de 0,01 grados, con
  `cell_x = floor(longitud / 0.01)` y `cell_y = floor(latitud / 0.01)`.
  `floor` conserva correctamente el lado negativo de los ejes. El identificador
  es `cell_x:cell_y`; cada registro incluye el tamaño angular de celda.
- `spark_hours`: conteo por hora de inicio, de 0 a 23, acumulando todos los años.
- `spark_months`: conteo por año y mes, con identificador `AAAA-MM`.
- `spark_hotspots`: las 20 celdas con más accidentes. Los empates se resuelven
  por `cell_x` y `cell_y` ascendentes.

La grilla tiene tamaño angular constante, no superficie constante ni radio en
metros. La concentración depende también de la cobertura del dataset: los
conteos no estiman riesgo por vehículo o población. Las horas corresponden a
la hora local del registro; no se mezclan con una conversión ficticia a UTC.
Estas mismas fórmulas servirán para la comparación Dask/Spark.

## Lectura, memoria y publicación

Se declara un esquema limitado a coordenadas, hora, año y mes. El particionador
`PaginateIntoPartitionsPartitioner` divide la lectura usando el `_id` único con
un máximo de ocho particiones. La caché usa `DISK_ONLY` para respetar los límites
del Mac. Se verifica el conteo de entrada y se rechazan documentos inválidos;
Spark no realiza una segunda limpieza que reduzca silenciosamente la muestra.

Cada resultado se escribe primero en una colección temporal mediante el
conector. Se verifican el número de grupos y la suma de conteos de grilla,
horas y meses: cada suma debe coincidir con la entrada. Después se renombra
cada colección para reemplazar su versión publicada. Los temporales se limpian
al terminar. Una entrada inválida conserva las colecciones anteriores.

Cada renombrado es atómico individualmente; los cuatro renombrados no forman
una transacción conjunta. Un fallo durante esa secuencia podría dejar versiones
distintas. El campo `run_id` permite reconocer qué resultados pertenecen a la
misma ejecución. El pipeline serializa ejecuciones para evitar escrituras
simultáneas.

## Reproducción

Desde la raíz del repositorio, con la ingesta ya finalizada:

```sh
docker compose build spark-master
docker compose up -d --wait spark-master spark-worker
docker compose exec -T spark-master spark-submit /app/tests/run_spark_tests.py
docker compose exec -T spark-master spark-submit /app/processing/aggregate.py
```

El último comando imprime `SPARK_RESULT` seguido del reporte JSON. El pipeline
ejecuta las pruebas Spark antes de la descarga e ingesta y luego procesa la
muestra; archiva el reporte como `spark-report.json`. Sigue pendiente completar
las etapas de API, despliegue y webhook de la guía.

Las pruebas incluyen coordenadas negativas y orientación de ejes, agregación
temporal que separa años, lectura/escritura real del conector, repetición sin
duplicados y conservación de resultados ante documentos inválidos. Las pruebas
de integración crean una base temporal y la eliminan, sin modificar la muestra.

Opciones del conector contrastadas con la documentación oficial:
[lectura](https://www.mongodb.com/docs/spark-connector/v10.x/batch-mode/batch-read-config/)
y [escritura](https://www.mongodb.com/docs/spark-connector/v10.x/batch-mode/batch-write-config/).

## Validación realizada

Jenkins `projectbd-ingestion`, ejecución 4, finalizó con **SUCCESS**. Pasaron
las tres pruebas de ingesta y las dos pruebas Spark. El dataset completo quedó
en el volumen y la ingesta volvió a publicar un millón de documentos únicos.

Reporte real: [spark-result.json](spark-result.json).

| Colección | Grupos publicados | Suma de accidentes |
| --- | ---: | ---: |
| spark_grid | 175.583 | 1.000.000 |
| spark_hours | 24 | 1.000.000 |
| spark_months | 86 | 1.000.000 |
| spark_hotspots | 20 | Subconjunto de las celdas de mayor concentración |

Spark leyó ocho particiones. El maestro registró la aplicación
`app-20261009005412-0001` como `projectbd-aggregations`; durante su ejecución
el worker estaba ALIVE con un núcleo ocupado y después la aplicación quedó
FINISHED. La duración total del trabajo fue 21,857 segundos, incluyendo lectura,
validaciones, cálculo y escritura. Esta medición no sustituye el benchmark de
dos configuraciones por motor, que sigue pendiente.

Una consulta independiente a MongoDB confirmó las tres sumas, que los hotspots
coinciden exactamente con las primeras 20 celdas ordenadas, que las cuatro
colecciones comparten `run_id` y que no quedan colecciones temporales.
Se comprobó que el token Kaggle y la contraseña Jenkins no aparecen en el log,
el reporte ni los archivos publicables. No se ha creado el commit de esta etapa.
