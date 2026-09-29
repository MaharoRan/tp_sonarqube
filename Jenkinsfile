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
        COMPOSE_PROJECT_NAME = 'real-time-sales-devops-tp-dev'
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
                    python3 -m venv .venv
                    .venv/bin/python -m pip install --upgrade pip
                    .venv/bin/python -m pip install -r requirements.txt
                '''
            }
        }

        stage('Unit Tests') {
            steps {
                sh '''
                    .venv/bin/python -m pytest tests/unit \
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
                    .venv/bin/python -m pytest tests/integration
                '''
            }
        }

        stage('E2E Tests') {
            steps {
                sh '''
                    echo "TODO: students must activate the complete E2E scenario."
                    .venv/bin/python -m pytest tests/e2e
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
