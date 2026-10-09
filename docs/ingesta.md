# Descarga y limpieza geoespacial con Dask

## Flujo implementado

El Jenkinsfile actual construye la imagen Dask, levanta los servicios, ejecuta
pytest, descarga el dataset completo mediante la API de Kaggle y carga una
muestra de un millón de registros. Si pytest falla, no ejecuta la descarga ni
la ingesta. El pipeline completo también ejecuta Spark, pruebas API y despliegue
condicionado; su configuración actual está en README y docs/cicd.md.

La credencial Secret text `kaggle-api-token` se obtiene del almacén Jenkins.
El shell desactiva la impresión de comandos antes de enviarla por stdin al
contenedor de descarga. El token no se escribe en el volumen de datos,
parámetros de línea de comandos, imagen, Compose o configuración Flask.
Las redirecciones HTTPS hacia otro host no conservan el header Authorization;
los mensajes de error de descarga tampoco imprimen URLs firmadas ni headers.

El ZIP completo y el CSV extraído se guardan en el volumen `dataset`, bajo
`/data/raw`. Se verifica el miembro exacto del ZIP y se registran SHA-256,
versión, tamaño y fecha de actualización en `/data/raw/source.json`.
Una reejecución reutiliza la descarga solo si los metadatos y ambos hashes
coinciden. No se descarga únicamente una muestra.

## Limpieza y esquema

Dask lee el CSV por bloques de 8 MiB usando sus dos workers. Se conservan ocho
columnas de las 46 originales: ID, Start_Time, Start_Lat, Start_Lng, Severity,
City, State y Timezone. Los atributos meteorológicos y otros campos no
necesarios para las consultas y agregaciones de la guía permanecen en el CSV
completo, pero no se cargan en MongoDB para limitar memoria y almacenamiento.

Las reglas de descarte se cuentan sin duplicar razones:

1. Coordenadas nulas o no numéricas.
2. Latitud fuera de [-90, 90] o longitud fuera de [-180, 180].
3. ID nulo o vacío, necesario para identificación y reejecución sin duplicados.
4. Start_Time inválido, necesario para las agregaciones temporales.

No se descartan coordenadas iguales a cero ni se inventan valores para campos
opcionales. GeoJSON usa `[longitud, latitud]`. El ID original se convierte en
`_id` y los campos opcionales ausentes se conservan como null.

La fuente expresa Start_Time en hora local. Se guarda como cadena ISO en
`start_time_local`, junto a Timezone y campos year, month, date y hour derivados
de esa hora local. No se afirma que estos valores representen UTC ni se mezclan
offsets supuestos. Las agregaciones temporales utilizarán esta semántica local.

## Muestreo y publicación

La primera pasada distribuida cuenta filas válidas en cada partición. Se
asignan cuotas proporcionales por el método de mayores restos para sumar
exactamente 1.000.000. Dentro de cada partición se seleccionan los IDs con
menores hashes deterministas, con clave fija y desempate por ID.

Esto distribuye la muestra sobre el archivo completo y evita tomar solo sus
primeras filas. La selección depende del archivo, tamaño de bloque y versión
pandas, registrados o fijados en el proyecto. No es una estratificación espacial
ni garantiza cubrir con igual peso todas las ciudades.

Cada worker carga lotes de hasta 1000 operaciones ReplaceOne con upsert en una
colección temporal exclusiva de la ejecución. Se exige el millón de IDs únicos,
se crea `location_2dsphere` y solo entonces se renombra a `accidents`.
Si una carga falla o los IDs seleccionados no son únicos, se elimina la
colección temporal y se conserva la colección publicada anteriormente.
Una ejecución exitosa reemplaza la muestra anterior de `accidents`; nunca
acumula otra copia del millón de registros.

## Ejecución reproducible

Con infraestructura y administrador Jenkins configurados según README:

```sh
python3 scripts/setup_ingestion_job.py
```

El job de producción `projectbd` carga Jenkinsfile desde GitHub y realiza
checkout de la rama main. `projectbd-ingestion` conserva el histórico de
validación local y solo se utiliza con `--local-source`. Para validar cambios
locales antes de un commit aprobado, sin publicar nada en GitHub:

```sh
python3 scripts/setup_ingestion_job.py --local-source --run
```

Este modo copia exclusivamente código conocido al workspace Jenkins, excluye
secretos y el enunciado, y omite el checkout en esa ejecución. Fue el modo
utilizado para la validación de esta etapa porque el código aún requiere
aprobación del usuario antes de hacer commit/publicarlo. El webhook y la
lectura automática del Jenkinsfile desde GitHub ya se validaron en CI/CD.

Pruebas independientes, con los servicios iniciados y la imagen actualizada:

```sh
docker compose run --rm -T --no-deps ingestion python -m pytest -q /app/tests/test_ingestion.py
```

Las pruebas comprueban limpieza, orden GeoJSON, tamaño y determinismo de muestra,
carga distribuida real, consulta espacial, índice, reejecución sin duplicados
y conservación de datos anteriores frente a IDs repetidos. Usan colecciones
temporales propias que se eliminan al terminar.

## Resultados de esta etapa

- Dataset Kaggle `sobhanmoosavi/us-accidents`, versión 13.
- CSV completo: 3.058.183.727 bytes; ZIP: 684.855.912 bytes.
- Filas leídas: 7.728.394, distribuidas en 364 particiones.
- Coordenadas nulas/no numéricas o fuera de rango: 0 en este archivo.
- IDs o fechas inválidos: 0 en este archivo.
- Muestra publicada: 1.000.000 documentos únicos, mediante ambos workers.
- Lotes ejecutados: 1093; índice `location_2dsphere` creado.
- Jenkins: ejecuciones 2 y 3 exitosas. La segunda reutilizó el ZIP y CSV
  completos después de verificar ambos hashes y volvió a publicar exactamente
  un millón de registros. Tres casos pytest pasaron, incluyendo la prueba de
  conservación de la colección anterior ante IDs duplicados.
- Ingesta de la ejecución 3: 59,263 segundos. Se comprobaron consultas $near
  y $geoNear sobre un punto real de la muestra; su exposición en Flask se validó
  posteriormente en la etapa de API.

El [reporte JSON generado por Jenkins](ingestion-result.json) conserva hashes,
conteos y tiempos medidos. Estas duraciones describen la ingesta y todavía no
constituyen la comparación obligatoria entre Dask y Spark.
