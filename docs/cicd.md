# Integración y despliegue con Jenkins

El Jenkinsfile prepara y prueba una API candidata antes de reemplazar la API
publicada en el puerto local 5000. La descarga Kaggle permanece dentro del
pipeline y usa la credencial cifrada `kaggle-api-token` mediante stdin.

## Etapas y criterio de despliegue

1. Checkout de la rama configurada; en producción será `main`. Se limpia el
   workspace antes del checkout para evitar código local residual.
2. Construcción de imágenes Dask, Spark y Flask. La candidata Flask recibe
   una etiqueta `projectbd-api:ci-N`, donde N es el número de build.
3. Arranque de MongoDB, Dask, Spark y `api-validation`, sin reemplazar `api`
   ni reiniciar el controlador Jenkins.
4. Pytest de ingesta, Spark y API con MongoDB real.
5. Descarga completa Kaggle, ingesta Dask y procesamiento Spark.
6. Pruebas HTTP contra la API candidata y los datos publicados.
7. Despliegue: solo después de aprobar todo, se etiqueta la candidata como
   `projectbd-api:local`, se reemplaza `api` y se comprueba su respuesta HTTP.
8. Limpieza de `api-validation` al terminar, tanto con éxito como con fallo.

Las etapas fallidas detienen las siguientes. La API candidata no publica puertos
del host; solo es accesible dentro de la red Docker. La API publicada conserva
su contenedor e imagen si pytest o las pruebas HTTP previas fallan. La ingesta
y el procesamiento son trabajos de datos: pueden actualizar MongoDB antes de
la validación HTTP final. Este flujo no promete una transacción conjunta de
todos los datos y servicios ni rollback automático posterior al despliegue.

Los reportes de ingesta, Spark, HTTP y el identificador del contenedor desplegado
se archivan en Jenkins. `disableConcurrentBuilds()` evita concurrencia dentro
del job; se utiliza un solo job para este entorno Docker.

## Configuración del job

Mientras un commit está pendiente de revisión, puede validarse sin publicarlo:

```sh
python3 scripts/setup_ingestion_job.py --local-source --run
```

Esta opción copia únicamente código permitido y usa temporalmente la definición
local del Jenkinsfile. No incluye el enunciado, datasets, secretos o la guía
personal. No acredita checkout ni ejecución automática desde GitHub.

Una vez integrado el Jenkinsfile completo en `main`:

```sh
python3 scripts/setup_ingestion_job.py --branch main
python3 scripts/manage_webhook.py start
```

Sin `--local-source`, el job usa **Pipeline script from SCM** y carga
`Jenkinsfile` desde GitHub. El plugin GitHub tiene su trigger habilitado y el
Jenkinsfile declara `githubPush()`. No se instala polling periódico como
sustituto del webhook.

## Webhook y acceso público

El perfil Compose `webhook` incluye la imagen oficial cloudflared fijada por
digest y un receptor Python. No añade una cuenta Cloudflare ni un dominio.
El receptor reenvía a Jenkins exclusivamente POST a `/github-webhook/`, con
firma HMAC-SHA256 válida y repositorio `Bjonion/projectBD`. Solo procesa eventos
`push` para `refs/heads/main`, además de `ping` para comprobar conectividad.

La administración Jenkins, MongoDB y las interfaces de Spark/Dask no se publican
por el túnel. Su secreto se guarda en `secrets/github/webhook_secret` con
permisos 600; su directorio usa 700. Se monta como secreto en el receptor, no se
incluye en imágenes, variables persistentes ni archivos versionados. El token
GitHub se obtiene de la autenticación del Mac y se usa únicamente en memoria
para configurar el webhook; no se traslada a Jenkins ni al túnel.

`scripts/manage_webhook.py start` inicia el túnel, detecta su URL y crea o actualiza
el webhook. `restart` vuelve a crearlo y actualiza GitHub; `status` muestra la
dirección actual y la registrada; `stop` cierra el acceso externo. El ID del
webhook se conserva localmente para reutilizarlo tras reinicios.

El host debe estar encendido y conectado. Los túneles temporales no garantizan
disponibilidad; al recrearlos cambia su dirección. La guía personal de reinicio
se conserva fuera del repositorio por solicitud del usuario.

## Trabajo en ramas y revisión

Los commits aprobados ya se publicaron conservando sus autores:

- [PR 1: Andres → main](https://github.com/Bjonion/projectBD/pull/1): infraestructura e ingesta.
- [PR 2: Julian → Andres](https://github.com/Bjonion/projectBD/pull/2): Spark y Flask.

Los PR están en borrador. La base inicial del PR de Julian permite revisar su
aporte separado; después de integrar Andres se cambia su base a `main`.
Se usará merge que conserve los commits originales. La ampliación CI/CD se
prepara en Julian y requiere su aprobación antes del commit y publicación.

Para validar completamente el webhook falta integrar el Jenkinsfile final en
`main`, configurar el job desde SCM y observar que una integración real inicia
Jenkins. El ping exitoso confirma transporte, no sustituye esa evidencia.

Referencias: [Jenkins Pipeline](https://www.jenkins.io/doc/book/pipeline/syntax/),
[plugin GitHub](https://plugins.jenkins.io/github/),
[Cloudflare Quick Tunnels](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/).

## Evidencias de esta etapa

Reporte: [cicd-result.json](cicd-result.json).

- Build 6: se dividió artificialmente el radio entre mil solo en el código
  candidato del workspace Jenkins. Pytest detectó que faltaba un accidente
  cercano, falló y omitió despliegue. Los identificadores del contenedor y de
  la imagen publicados fueron idénticos antes y después.
- Se restauró el código correcto desde el repositorio local. Build 7 terminó
  SUCCESS, con ocho pruebas aprobadas y pruebas HTTP sobre la candidata. Luego
  promovió la imagen y verificó la API publicada; archivó deployment.json.
  La imagen candidata coincidía con la ya publicada porque Flask no había
  cambiado; Compose conservó el contenedor existente. Se comprobó que la imagen
  publicada corresponde exactamente a la candidata aprobada por las pruebas.
- Ping real GitHub → Cloudflare → receptor → Jenkins: 200. Acceso público a
  `/login` y `/manage`: 404. POST sin firma al webhook: 403.
- Se recreó el túnel y se verificó una URL nueva, actualización del mismo ID de
  webhook y un nuevo ping 200. `status` confirmó la sincronización de URLs.
- Los archivos publicables, el log y los reportes no contienen token Kaggle,
  contraseña Jenkins ni secreto del webhook. La guía personal y el enunciado
  no están en el repositorio.

Estas ejecuciones usan el código local revisable. La prueba de checkout desde
SCM y disparo por integración a `main` sigue pendiente de aprobar el commit
final de esta etapa y de integrar las contribuciones.
