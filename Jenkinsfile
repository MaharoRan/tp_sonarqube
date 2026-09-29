pipeline {
    agent any

    environment {
        PYTHON = 'python3'
        RUN_INTEGRATION_TESTS = 'true'
        RUN_E2E_TESTS = 'true'
        API_BASE_URL = 'http://sales-api:8000'
        KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
        KAFKA_TOPIC = 'sales.orders'
        COMPOSE_COMMAND = 'docker-compose'
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Environment') {
            steps {
                sh 'python3 --version'
                sh 'docker --version'
            }
        }

        stage('Install') {
            steps {
                sh '''
                    python3 -m pip install --break-system-packages -r requirements.txt
                '''
            }
        }

        stage('Unit Tests') {
            steps {
                sh '''
                                        python3 -m pytest tests/unit \
                      --cov=app \
                      --cov-report=xml:coverage.xml \
                      --cov-report=term-missing
                '''
            }
        }

        stage('Start Platform') {
            steps {
                sh '''
                                        docker-compose up -d --build \
                      sales-api kafka postgres spark-master spark-worker spark-streaming
                '''
            }
        }

        stage('Integration Tests') {
            steps {
                sh '''
                    python3 -m pytest tests/integration
                '''
            }
        }

        stage('E2E Tests') {
            steps {
                sh '''
                    echo "TODO: students must activate the complete E2E scenario."
                    python3 -m pytest tests/e2e
                '''
            }
        }

        stage('SonarQube') {
            steps {
                echo 'TODO: configure SonarQube Scanner / server credentials.'
            }
        }

        stage('Quality Gate') {
            steps {
                echo 'TODO: waitForQualityGate() after SonarQube integration.'
            }
        }
    }

    post {
        always {
            archiveArtifacts allowEmptyArchive: true, artifacts: 'coverage.xml'
        }
    }
}
