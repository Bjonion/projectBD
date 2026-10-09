# Plan de desarrollo y validación

Cada etapa produce cambios revisables antes de solicitar autorización para su
commit. Las pruebas y mediciones se documentarán cuando se ejecuten; este plan
no acredita resultados todavía.

| Etapa | Trabajo | Evidencia necesaria para completarla |
| --- | --- | --- |
| 1. Preparación | Repositorio, ramas, responsables, dataset y accesos | Dataset confirmado con el docente, acceso Kaggle, identidad Git y primer commit aprobado |
| 2. Infraestructura y Jenkins temprano | Compose, MongoDB, Spark master/worker, Dask scheduler/2 workers, Flask y Jenkins | Configuración válida, arranque, salud, persistencia e imágenes compatibles con ARM64 |
| 3. Ingesta Dask | Descarga Kaggle automatizada, lectura particionada, limpieza y carga por lotes | Conteos antes/después, al menos un millón de registros válidos si se usa muestra, GeoJSON e índice 2dsphere; reejecución sin duplicados |
| 4. Spark | Lectura mediante MongoDB Spark Connector y agregaciones espaciales/temporales | Colecciones de resultados verificadas y trabajo ejecutado en workers |
| 5. Consultas y API | $near, $geoWithin, $geoNear; endpoints de cercanía, polígono y agregados | Parámetros validados y pruebas con MongoDB real, incluyendo coordenadas [longitud, latitud] y radio en metros |
| 6. CI/CD completo | Jenkinsfile, construcción, pytest, arranque, smoke tests, despliegue y webhook | PR integrado dispara el pipeline; un fallo de prueba impide desplegar; credenciales fuera de Git |
| 7. Comparación | Misma agregación por grilla en Dask y Spark, dos configuraciones de workers por motor | Mismo conjunto de entrada, resultados equivalentes, tiempos y memoria medidos, condiciones y repeticiones registradas |
| 8. Entrega | README reproducible e informe técnico de máximo diez páginas | Arranque siguiendo la documentación, diagrama, decisiones, consultas y análisis basado en resultados reales |

## Decisiones pendientes

1. Dataset confirmado y validado con el docente: `sobhanmoosavi/us-accidents`. Descarga completa verificada desde Jenkins.
2. Este Mac corresponde a Andres; ramas solicitadas: `Andres` y `Julian`.
3. Identidades confirmadas: Andres <agomezp@correo.iue.edu.co> y Julian <julilc324@gmail.com>. Cada integrante conserva la autoría de sus propios cambios.
4. Credencial Kaggle almacenada localmente con acceso restringido y cifrada en Jenkins. El usuario decidió usarla durante el desarrollo y reemplazarla al finalizar.
5. Autenticación GitHub verificada como Bjonion: permisos admin/push y acceso de lectura a la API de webhooks. Todavía no se publicaron cambios ni se creó un webhook.
6. Aprobar las instalaciones o descargas de software antes de ejecutarlas.

El usuario aprobó el primer commit, condicionado a verificar los accesos de
Kaggle y GitHub antes de ejecutarlo. `Actividad_bigdata.md` debe permanecer
exclusivamente local y no debe incluirse en ningún commit.

## Validación de accesos antes del primer commit

- API de metadatos Kaggle: HTTP 200 para `sobhanmoosavi/us-accidents`;
  tamaño informado: 3.058.183.727 bytes.
- Inicialmente no se encontraron credenciales Kaggle en las ubicaciones
  convencionales ni un token de API en el entorno. Posteriormente el usuario
  proporcionó un token, almacenado en un archivo local excluido de Git.
- La petición HEAD al endpoint de descarga respondió HTTP 404; no confirma
  ni descarta que la descarga mediante la API oficial con credenciales funcione.
- API GitHub: HTTP 200 al consultar el usuario autenticado y el repositorio;
  permisos admin, maintain, push, triage y pull disponibles.
- API de webhooks GitHub: lectura HTTP 200. No se realizaron escrituras remotas.
- No se mostraron ni guardaron en archivos del proyecto las credenciales GitHub.
- Posteriormente, una petición GET de descarga con token respondió HTTP 206
  y se verificó la firma ZIP con solo cuatro bytes leídos. El endpoint es
  público: esto verifica acceso al archivo, no la identidad del token.
- Los archivos secretos tienen permisos 600 y sus directorios 700.
- El usuario decidió posponer la rotación del token hasta el final de la
  actividad. La integración en Jenkins se completó; el almacén cifrado persiste
  en su volumen local. No se ha creado un respaldo externo.
- El usuario indicó ignorar el avance anterior y desarrollar desde cero.
- Validación de acceso al dataset y a GitHub completada para el primer commit.

## Etapas completadas y pendientes

- Etapa 1: commit local `ed5e16e`, ramas main/Andres/Julian creadas, identidad
  de Andres configurada. No se ha realizado push a GitHub.
- Etapa 2 validada: ocho servicios ejecutándose, límites por contenedor y
  puertos de host restringidos a localhost. Flask dispone de /health con ping
  real a MongoDB; no tiene todavía consultas. Jenkins tiene administrador y
  credencial Kaggle cifrada configurados.
- Imágenes base verificadas mediante metadatos del registro: soporte ARM64.
- Descarga/construcción autorizadas y ejecutadas. Salud, registro de workers,
  persistencia MongoDB/Jenkins, volumen Dask y conector Spark/MongoDB verificados.
- MongoDB se ajustó a 7.0 debido a la incompatibilidad de 8.x con el kernel
  7.0.12-linuxkit del Docker de este equipo.
- Etapa 2 registrada en el commit aprobado `7ea6319`, bajo Andres.
- Etapa 3: Jenkins descargó el dataset completo versión 13; Dask leyó
  7.728.394 filas y publicó un millón de documentos únicos GeoJSON con índice
  2dsphere. Código y reporte registrados en el commit aprobado `16185bb` de Andres.
- Etapa 4 validada en Julian: Jenkins build 4 exitoso, cinco pruebas aprobadas
  (tres de ingesta y dos Spark), ocho particiones leídas y agregaciones
  espaciales/temporales publicadas sobre un millón de documentos.
  Commit aprobado `05be1c8`, autor Julian <julilc324@gmail.com>, cuenta GitHub
  confirmada `Julianlc324`.
- Etapa 5 validada en Julian: Jenkins build 5 exitoso, ocho pruebas aprobadas
  y consultas HTTP reales sobre la muestra y resultados Spark. El siguiente
  commit requiere revisión y aprobación propias.
- Ambas ramas comparten la base aprobada de ingesta; no hay cambios publicados
  en GitHub. La etapa posterior prevista es completar CI/CD y el webhook.

## Distribución del trabajo por integrante

- El usuario exige participación de ambos integrantes, tanto en infraestructura
  como en el código del proyecto. Cada uno trabajará en su rama y conservará
  la autoría real de los cambios que realice y revise.
- Propuesta para Andres: MongoDB, Dask, volúmenes de ingesta y configuración
  Compose correspondiente; descarga, limpieza, muestreo e instrumentación Dask.
- Propuesta para Julian: configuración Spark y Flask, infraestructura Jenkins
  para CI/CD; procesamiento Spark, consultas/API, Jenkinsfile e instrumentación Spark.
- Integración y documentación: cada integrante documenta y valida su parte;
  los cambios se revisan mediante pull requests hacia main.

La distribución concreta sigue siendo una propuesta. La base de infraestructura
desarrollada en esta sesión corresponde a Andres. No se cambiará la identidad
Git a Julian para simular aportes: sus commits deberán reflejar su participación
real. El usuario confirmó que Julian trabajará y revisará los cambios en este
Mac; se confirmará la autoría antes de cada commit, además de su aprobación.

## Condiciones de la comparación

La propuesta inicial es comparar 2 y 3 workers de Dask y 1 y 2 workers de Spark,
manteniendo documentados los recursos por worker. Se ajustará según la memoria
disponible. Se evitará ejecutar ambos benchmarks simultáneamente. La comparación
deberá distinguir el tiempo de lectura del tiempo de cálculo y explicar cuándo
se incluye la escritura. Se conservará la misma definición de grilla y el mismo
tratamiento de coordenadas y fechas en ambos motores.

## Estado inicial del equipo

Verificación realizada el 8 de octubre de 2026:

- macOS ARM64, 16 GiB de RAM; Docker con aproximadamente 7,75 GiB asignados.
- Docker Engine 29.8.0 y Docker Compose v5.5.1 operativos.
- Git 2.50.1 y Python 3.9.6 disponibles.
- Aproximadamente 713 GiB de espacio libre al comprobarlo.
- No se encontró runtime Java local; se prevé usar Java en contenedores.
- No se encontró el archivo convencional ~/.kaggle/kaggle.json.
- El repositorio remoto no publicó referencias de ramas en la consulta inicial.
- La actividad indica entrega y sustentación el 9 de octubre de 2026.
