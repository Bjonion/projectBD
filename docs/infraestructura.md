# Validación de infraestructura

Validación realizada el 8 de octubre de 2026, hora de Colombia, en este Mac
ARM64. Estas pruebas usan datos sintéticos mínimos y no sustituyen la ingesta
del dataset ni el benchmark obligatorio.

## Resultados

| Verificación | Resultado |
| --- | --- |
| Construcción | Imágenes Dask, Spark, Flask y Jenkins construidas para ARM64; MongoDB oficial descargado |
| Compose | Configuración válida; ocho servicios iniciados |
| MongoDB | Ping correcto; un documento temporal sobrevivió a la recreación del contenedor |
| Flask | GET /health devuelve HTTP 200 y estado MongoDB ok |
| Dask | Dos workers registrados; tareas asignadas a cada worker devolvieron 1 y 4 |
| Volumen Dask | Ambos workers leyeron el marcador escrito por el scheduler en /data |
| Spark | Un worker activo; ejecución standalone con un executor en ese worker |
| Conector MongoDB/Spark | Lectura de un documento temporal y escritura del resultado de una agregación |
| Resultado Spark | Suma de enteros 0–99 = 4950, comprobada posteriormente desde MongoDB |
| Jenkins | Administrador Andres autenticado; la cuenta persistió al recrear el contenedor |
| Docker desde Jenkins | Docker Engine 29.8.0 accesible y Compose v5.6.0 disponible |
| Credencial Kaggle | Secret text con ID kaggle-api-token guardada en Jenkins; no se imprimió su valor |

Las colecciones temporales y el marcador del volumen usados en las pruebas se
eliminaron al finalizar. No se descargó todavía el dataset completo.

## Ajuste de compatibilidad

MongoDB 8.0 rechazó iniciar por la incompatibilidad con el kernel de Docker
`7.0.12-linuxkit`. Se seleccionó MongoDB 7.0, cuya compatibilidad en este rango
está documentada en la [matriz oficial SERVER-125742](https://jira.mongodb.org/browse/SERVER-125742).
La guía no impone una versión específica. Se conservan GeoJSON, índice
2dsphere y compatibilidad con el MongoDB Spark Connector.

Se amplió el límite del master Spark a 1 GiB para alojar el daemon y el driver
de 512 MiB durante las ejecuciones. La suma de límites por contenedor es
7,125 GiB frente a aproximadamente 7,75 GiB disponibles para Docker.

Una lectura puntual, después de las pruebas, mostró aproximadamente 1,1 GiB
de uso conjunto de los servicios. Esta cifra representa ese instante, no el
pico ni el consumo durante la ingesta; los límites deberán comprobarse con
el dataset real.

## Alcance pendiente

- Ingesta, reglas de limpieza, muestreo e índice geoespacial.
- Agregaciones reales, consultas y los tres endpoints de la guía.
- Pytest, Jenkinsfile, job, webhook y despliegue continuo.
- Benchmark con dos configuraciones por motor e informe técnico.
- Publicación GitHub y commits siguientes, sujetos a aprobación.

La credencial local y la contraseña Jenkins están excluidas de Git y Docker.
El usuario decidió conservar el token actual durante el desarrollo y cambiarlo
al cierre. El almacén Jenkins reside en su volumen persistente; todavía no se
ha realizado un respaldo externo.
