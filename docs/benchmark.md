# Comparación Dask y Spark: protocolo y mediciones

## Entrada y operación común

La muestra `projectbd.accidents` se exporta, ordenada por `_id`, a diez archivos
Parquet de 100.000 filas con las columnas id, longitud y latitud. Ambos motores
leen exactamente esos archivos desde el volumen dataset. La exportación no
forma parte del tiempo medido. `input.json` identifica las filas y los archivos
con SHA-256; cada ejecución valida los archivos antes de comenzar.

La operación es una agregación por grilla de 0,01 grados:
`cell_x = floor(longitud / 0.01)`, `cell_y = floor(latitud / 0.01)`, seguida de
conteo por pareja de índices. El tiempo incluye lectura Parquet, creación y
ejecución de la agregación y materialización del resultado en el driver.
Excluye arranque de contenedores, conexión inicial al clúster, validación de
hashes, escritura de resultados y comparación con la grilla publicada.

Las mediciones Dask usan dos y tres workers, con un hilo y una CPU por worker,
límite Dask de 512 MiB y límite de contenedor de 768 MiB. Las configuraciones
Spark usan uno y dos workers, con una CPU y un core de ejecutor por worker,
heap de ejecutor de 512 MiB, daemon de 256 MiB y límite de contenedor de
1536 MiB. El driver Spark tiene heap de 512 MiB y contenedor de 1 GiB.
El límite Dask de 512 MiB controla el proceso Python; el heap Spark de
512 MiB controla la JVM y no toda su memoria. Por ello no representan
límites idénticos. Se comparan máximos reales de contenedor y se indican
los recursos completos. Se esperan todos los ejecutores antes del cronómetro.
No se ejecutan ambos motores simultáneamente. Cada runner comprueba que Jenkins
esté inactivo, pausa los workers del otro motor y los restaura al terminar.
Spark también lee diez particiones; ambos motores configuran ocho particiones
de salida/shuffle (Spark puede coalescerlas con su optimización adaptativa).

## Memoria

Se lee `memory.peak` del cgroup de cada contenedor worker, recreado para cada
configuración, y del driver efímero. Este máximo incluye procesos, arranque y
memoria contabilizada por el kernel, incluida caché de archivos. No es solo el
heap de Python o Java. El máximo del driver se captura al terminar el cálculo,
antes de comparar con MongoDB.

La suma de máximos individuales de workers es una cota superior del máximo
conjunto: los máximos no tienen por qué ocurrir al mismo tiempo. Se reporta
separada del driver. Se excluyen scheduler/master, MongoDB, Flask y Jenkins.
Spark usa el mismo criterio de cgroup. Sus workers incluyen el daemon y el
ejecutor Java; Dask incluye sus procesos de worker y supervisión. Los máximos
Spark se leen desde el host al finalizar el driver; el cgroup conserva su
máximo aunque el ejecutor ya haya terminado. El máximo del driver Spark se
captura inmediatamente después de materializar, antes de validar y escribir.

## Dask: resultados reales

Reporte: [benchmark-dask.json](benchmark-dask.json).

| Workers | Tiempo de lectura y agregación | Suma de máximos de workers | Máximo del driver |
| --- | ---: | ---: | ---: |
| 2 | 1,011786 s | 457,11 MiB | 109,08 MiB |
| 3 | 0,617470 s | 634,65 MiB | 108,73 MiB |

Ambas configuraciones procesaron un millón de filas y produjeron 175.583 celdas.
Se compararon todos sus conteos con `spark_grid`, publicada previamente desde
la misma muestra MongoDB: igualdad completa. Además, las dos configuraciones
Dask tienen el mismo SHA-256 de entrada y de resultados ordenados.

Estos son registros únicos por configuración, ejecutados en orden 2 → 3.
No se vació la caché del sistema operativo. En esta ejecución tres workers
tardaron menos y acumularon más memoria máxima; el dato no permite separar
el efecto del paralelismo del calentamiento de cachés ni generalizar a otras
cargas. Las mediciones Spark usan
estos mismos Parquet. No debe compararse este tiempo con el reporte de ingesta
o `spark-result.json`, que incluyen otras fuentes y operaciones.

## Reproducción de Dask

Desde la raíz del repositorio, con los servicios iniciados, la muestra y las
agregaciones publicadas, y sin pipeline activo:

```sh
python3 scripts/benchmark_dask.py
```

El programa construye la imagen, exporta la entrada común, mide las dos
configuraciones, comprueba todos los conteos y restaura Spark. Conserva los
Parquet y reportes en `/data/benchmark` del volumen y escribe el resumen en
`docs/benchmark-dask.json`. La tercera instancia está en
`docker-compose.benchmark.yml`; no forma parte del arranque normal.

Los datos de entrada deben conservarse para las mediciones Spark. La medición
Dask fue aprobada en Andres; Spark quedó aprobado en Julian (`5d88a8c`).

## Spark: resultados reales y comparación

Reporte: [benchmark-spark.json](benchmark-spark.json).

| Motor | Workers | Tiempo | Suma de máximos de workers | Máximo del driver |
| --- | ---: | ---: | ---: | ---: |
| Dask | 2 | 1,011786 s | 457,11 MiB | 109,08 MiB |
| Dask | 3 | 0,617470 s | 634,65 MiB | 108,73 MiB |
| Spark | 1 | 4,966773 s | 415,14 MiB | 454,21 MiB |
| Spark | 2 | 5,230302 s | 797,18 MiB | 442,31 MiB |

Las cuatro ejecuciones procesaron exactamente el mismo millón de filas y
produjeron las mismas 175.583 celdas, verificadas mediante SHA-256 de todos
los resultados ordenados. Se confirmaron uno y dos ejecutores Spark activos,
respectivamente. Los workers se recrearon para reiniciar sus máximos de memoria.

En este Mac, para esta grilla sobre Parquet, Dask obtuvo los tiempos menores
con un driver de menor memoria. Spark con un worker acumuló menos memoria
máxima en workers que Dask con dos, pero su driver consumió bastante más.
Añadir un segundo worker Spark no redujo el tiempo en esta ejecución y aumentó
la suma de máximos de workers. Para esta operación y este tamaño elegiríamos
Dask si priorizamos latencia y memoria total; Spark sigue siendo el motor de
procesamiento requerido y ya valida la lectura/escritura mediante MongoDB
Spark Connector y las agregaciones espaciales y temporales del proyecto.

No se demuestra que Spark sea menos conveniente en datasets mayores ni que
más workers lo vuelvan siempre más lento. Hay una sola medición por configuración,
en orden Dask 2 → 3 y luego Spark 1 → 2, sin vaciar cachés. Inicialización de
JVM, conexión y arranque de ejecutores quedan fuera del cronómetro; compilación
y planificación necesarias para la consulta quedan dentro. Los costes de
coordinación y la variabilidad pueden pesar en una carga que dura pocos segundos.
La suma de máximos de workers más driver tampoco es un máximo simultáneo total.

## Reproducción de Spark

Después de ejecutar el benchmark Dask, conservar `/data/benchmark/input` y sus
metadatos; no exportar otra muestra entre motores:

```sh
python3 scripts/benchmark_spark.py
```

El runner construye Spark, comprueba que Jenkins esté inactivo, pausa los workers
Dask, recrea workers Spark, verifica recursos y registro en el master, ejecuta
ambas configuraciones y restaura Dask. El driver temporal usa el alias de red
`spark-benchmark`, accesible a los ejecutores con `--use-aliases`.
La segunda instancia Spark solo existe en el perfil `benchmark` y se detiene
al terminar. Los resultados completos quedan en `docs/benchmark-spark.json`
y `/data/benchmark/spark-1.json` y `spark-2.json`; las grillas ordenadas también
se conservan como CSV en el volumen. No se cambian las colecciones publicadas.

La medición Dask quedó aprobada en Andres (`6db785c`). La implementación,
medición y análisis Spark quedaron aprobados en Julian (`5d88a8c`).
