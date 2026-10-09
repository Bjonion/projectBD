// La API publicada se reemplaza únicamente después de pytest y smoke tests.
pipeline {
    agent any
    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
        timeout(time: 60, unit: 'MINUTES')
    }
    triggers { githubPush() }
    environment {
        API_IMAGE = "projectbd-api:ci-${BUILD_NUMBER}"
    }
    parameters {
        booleanParam(name: 'CHECKOUT_REPOSITORY', defaultValue: true,
                     description: 'Desactivar solo para validar código local antes del commit aprobado.')
        string(name: 'GIT_BRANCH', defaultValue: 'main', description: 'Rama a validar; webhook configurado para main.')
    }
    stages {
        stage('Checkout') {
            when { expression { params.CHECKOUT_REPOSITORY } }
            steps {
                deleteDir()
                git branch: params.GIT_BRANCH, url: 'https://github.com/Bjonion/projectBD.git'
            }
        }
        stage('Construcción Dask, Spark y API') {
            steps { sh 'docker compose build dask-scheduler spark-master api' }
        }
        stage('Servicios') {
            steps {
                sh 'docker compose up -d --wait --wait-timeout 300 mongodb dask-scheduler dask-worker-1 dask-worker-2 spark-master spark-worker api-validation'
            }
        }
        stage('Pytest ingesta') {
            steps {
                sh 'docker compose run --rm -T --no-deps ingestion python -m pytest -q /app/tests/test_ingestion.py'
            }
        }
        stage('Pytest Spark') {
            steps {
                sh 'docker compose exec -T spark-master spark-submit /app/tests/run_spark_tests.py'
            }
        }
        stage('Pytest API con MongoDB') {
            steps {
                sh 'docker compose exec -T api-validation python -m pytest -q -p no:cacheprovider /app/tests/test_api.py'
            }
        }
        stage('Descarga completa Kaggle') {
            steps {
                withCredentials([string(credentialsId: 'kaggle-api-token', variable: 'KAGGLE_TOKEN')]) {
                    sh '''
                        set +x
                        printf '%s' "$KAGGLE_TOKEN" | docker compose run --rm -T --no-deps ingestion python -m ingestion.download --token-stdin
                    '''
                }
            }
        }
        stage('Ingesta Dask y GeoJSON') {
            steps {
                sh 'docker compose run --rm -T --no-deps ingestion python -m ingestion.ingest --rows 1000000'
            }
        }
        stage('Reporte') {
            steps {
                sh 'docker compose run --rm -T --no-deps ingestion cat /data/reports/ingestion.json > ingestion-report.json'
                archiveArtifacts artifacts: 'ingestion-report.json', fingerprint: true
            }
        }
        stage('Agregaciones Spark') {
            steps {
                sh 'docker compose exec -T spark-master spark-submit /app/processing/aggregate.py > spark-processing.log'
                sh "sed -n 's/^SPARK_RESULT //p' spark-processing.log > spark-report.json"
                archiveArtifacts artifacts: 'spark-report.json', fingerprint: true
            }
        }
        stage('Pruebas HTTP API') {
            steps {
                sh 'docker compose exec -T api-validation python /app/scripts/smoke_api.py > api-smoke.log'
                sh "sed -n 's/^SMOKE_RESULT //p' api-smoke.log > api-smoke-report.json"
                archiveArtifacts artifacts: 'api-smoke-report.json', fingerprint: true
            }
        }
        stage('Despliegue') {
            steps {
                sh '''
                    docker image tag "$API_IMAGE" projectbd-api:local
                    API_IMAGE=projectbd-api:local docker compose up -d --no-deps --wait --wait-timeout 120 api
                    API_IMAGE=projectbd-api:local docker compose exec -T api python /app/scripts/smoke_api.py
                    docker inspect "$(docker compose ps -q api)" --format '{"container_id":"{{.Id}}","image_id":"{{.Image}}"}' > deployment.json
                '''
                archiveArtifacts artifacts: 'deployment.json', fingerprint: true
            }
        }
    }
    post {
        always {
            sh 'docker compose rm -sf api-validation'
        }
    }
}
