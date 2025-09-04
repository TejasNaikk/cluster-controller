# Coverage Integration Setup

## Coverage Badges

Add these badges to your main README.md:

```markdown
[![codecov](https://codecov.io/gh/YOUR_USERNAME/cluster-controller/branch/main/graph/badge.svg)](https://codecov.io/gh/YOUR_USERNAME/cluster-controller)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=cluster-controller&metric=coverage)](https://sonarcloud.io/summary/new_code?id=cluster-controller)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=cluster-controller&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=cluster-controller)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=cluster-controller&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=cluster-controller)
```

## Setup Instructions

### 1. Codecov Integration
1. Go to [codecov.io](https://codecov.io) and sign in with GitHub
2. Add your repository
3. Copy the upload token and add it to GitHub Secrets as `CODECOV_TOKEN`

### 2. SonarCloud Integration  
1. Go to [sonarcloud.io](https://sonarcloud.io) and sign in with GitHub
2. Import your repository
3. Update the organization name in `pom.xml` (line 21)
4. Add `SONAR_TOKEN` to GitHub Secrets

### 3. GitHub Actions Secrets
Add these secrets in your GitHub repository settings:
- `CODECOV_TOKEN`: From Codecov dashboard
- `SONAR_TOKEN`: From SonarCloud dashboard  
- `CODACY_PROJECT_TOKEN`: (Optional) From Codacy dashboard

## Features Enabled

### ✅ Automatic Coverage Reports
- **HTML Reports**: Generated in `target/site/jacoco/index.html`
- **XML Reports**: For CI integration at `target/site/jacoco/jacoco.xml`
- **CSV Reports**: For data analysis at `target/site/jacoco/jacoco.csv`

### ✅ Pull Request Integration
- **Coverage Comments**: Automatic PR comments showing coverage changes
- **Diff Coverage**: Shows coverage for only changed files
- **Quality Gates**: Configurable minimum coverage thresholds

### ✅ Multiple Coverage Services
- **Codecov**: Beautiful coverage visualization and PR integration
- **SonarCloud**: Code quality + coverage analysis
- **Codacy**: Additional code quality insights
- **GitHub Actions**: Built-in coverage summaries

### ✅ CI/CD Integration
- **Automated Testing**: Runs on every push and PR
- **Coverage Reporting**: Uploads to multiple services
- **Test Results**: Published in GitHub Actions
- **Artifacts**: Test reports stored for 30 days

## Local Development

Run tests with coverage locally:
```bash
mvn clean test                    # Run tests with coverage
mvn jacoco:report                # Generate coverage report
open target/site/jacoco/index.html  # View coverage report
```

## Coverage Thresholds

Current thresholds (configurable in `pom.xml`):
- **Overall Coverage**: 50% minimum
- **Branch Coverage**: 50% minimum  
- **PR Changed Files**: 60% minimum (GitHub Actions)
- **PR Overall**: 40% minimum (GitHub Actions)
