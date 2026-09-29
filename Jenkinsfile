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

        stage('Wait for API') {
            steps {
                sh '''
                    for attempt in $(seq 1 60); do
                        if curl --fail --silent "$API_BASE_URL/api/health" > /dev/null; then
                            exit 0
                        fi
                        sleep 2
                    done
                    echo "API did not become ready within 120 seconds"
                    exit 1
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
                script {
                    def scannerHome = tool 'sonar-scanner'
                    withSonarQubeEnv('sonarqube') {
                        sh "${scannerHome}/bin/sonar-scanner"
                    }
                }
            }
        }

        stage('Quality Gate') {
            steps {
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }
    }

    post {
        always {
            archiveArtifacts allowEmptyArchive: true, artifacts: 'coverage.xml'
        }
    }
}
