# Comparación Dask y Spark: protocolo y mediciones

## Entrada y operación común

La muestra `projectbd.accidents` se exporta, ordenada por `_id`, a diez archivos
Parquet de 100.000 filas con las columnas id, longitud y latitud. Ambos motores
leerán exactamente esos archivos desde el volumen dataset. La exportación no
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
Spark pendientes usarán uno y dos workers, conservando recursos por worker.
No se ejecutan ambos motores simultáneamente. El runner comprueba que Jenkins
esté inactivo, pausa Spark durante Dask y lo restaura al terminar.

## Memoria

Se lee `memory.peak` del cgroup de cada contenedor worker, recreado para cada
configuración, y del driver efímero. Este máximo incluye procesos, arranque y
memoria contabilizada por el kernel, incluida caché de archivos. No es solo el
heap de Python o Java. El máximo del driver se captura al terminar el cálculo,
antes de comparar con MongoDB.

La suma de máximos individuales de workers es una cota superior del máximo
conjunto: los máximos no tienen por qué ocurrir al mismo tiempo. Se reporta
separada del driver. Se excluyen scheduler/master, MongoDB, Flask y Jenkins.
Se usará el mismo criterio en Spark para evitar comparar RSS con heap o métricas
de alcances distintos.

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
cargas. El análisis Dask/Spark se completará con las mediciones Spark sobre
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
Dask se prepara en Andres; la implementación y medición Spark se prepararán
en Julian, con revisión y aprobación de cada integrante antes de su commit.
