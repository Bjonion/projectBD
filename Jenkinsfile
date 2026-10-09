// Etapa actual: descarga e ingesta. Pruebas/API/despliegue completos se añaden en CI/CD.
pipeline {
    agent any
    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
        timeout(time: 60, unit: 'MINUTES')
    }
    parameters {
        booleanParam(name: 'CHECKOUT_REPOSITORY', defaultValue: true,
                     description: 'Desactivar solo para validar código local antes del commit aprobado.')
        string(name: 'GIT_BRANCH', defaultValue: 'Andres', description: 'Rama a validar.')
    }
    stages {
        stage('Checkout') {
            when { expression { params.CHECKOUT_REPOSITORY } }
            steps {
                git branch: params.GIT_BRANCH, url: 'https://github.com/Bjonion/projectBD.git'
            }
        }
        stage('Construcción Dask') {
            steps { sh 'docker compose build dask-scheduler' }
        }
        stage('Servicios') {
            steps { sh 'docker compose up -d --wait --wait-timeout 300' }
        }
        stage('Pytest ingesta') {
            steps {
                sh 'docker compose run --rm -T --no-deps ingestion python -m pytest -q /app/tests/test_ingestion.py'
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
    }
}
