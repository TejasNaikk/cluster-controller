#!/bin/bash

# Coverage reporting script for cluster-controller

set -e

echo "🧪 Running tests with coverage..."
mvn clean test

echo "📊 Generating coverage reports..."
mvn jacoco:report

echo "📈 Coverage Summary:"
echo "==================="

# Check if jacoco.csv exists and show summary
if [ -f "target/site/jacoco/jacoco.csv" ]; then
    # Extract coverage data from CSV (last line contains totals)
    COVERAGE_DATA=$(tail -n 1 target/site/jacoco/jacoco.csv)
    
    # Parse coverage data
    INSTRUCTION_COVERED=$(echo $COVERAGE_DATA | cut -d',' -f4)
    INSTRUCTION_MISSED=$(echo $COVERAGE_DATA | cut -d',' -f5)
    BRANCH_COVERED=$(echo $COVERAGE_DATA | cut -d',' -f6)
    BRANCH_MISSED=$(echo $COVERAGE_DATA | cut -d',' -f7)
    
    # Calculate percentages
    TOTAL_INSTRUCTIONS=$((INSTRUCTION_COVERED + INSTRUCTION_MISSED))
    TOTAL_BRANCHES=$((BRANCH_COVERED + BRANCH_MISSED))
    
    if [ $TOTAL_INSTRUCTIONS -gt 0 ]; then
        INSTRUCTION_PERCENTAGE=$((INSTRUCTION_COVERED * 100 / TOTAL_INSTRUCTIONS))
        echo "📋 Instruction Coverage: ${INSTRUCTION_PERCENTAGE}% (${INSTRUCTION_COVERED}/${TOTAL_INSTRUCTIONS})"
    fi
    
    if [ $TOTAL_BRANCHES -gt 0 ]; then
        BRANCH_PERCENTAGE=$((BRANCH_COVERED * 100 / TOTAL_BRANCHES))
        echo "🌿 Branch Coverage: ${BRANCH_PERCENTAGE}% (${BRANCH_COVERED}/${TOTAL_BRANCHES})"
    fi
else
    echo "❌ Coverage data not found. Make sure tests ran successfully."
fi

echo ""
echo "📁 Reports generated:"
echo "  📊 HTML: target/site/jacoco/index.html"
echo "  📄 XML:  target/site/jacoco/jacoco.xml"
echo "  📊 CSV:  target/site/jacoco/jacoco.csv"
echo ""

# Check if we can open the HTML report
if command -v open &> /dev/null; then
    echo "🌐 Opening HTML coverage report..."
    open target/site/jacoco/index.html
elif command -v xdg-open &> /dev/null; then
    echo "🌐 Opening HTML coverage report..."
    xdg-open target/site/jacoco/index.html
else
    echo "💡 To view the report, open: target/site/jacoco/index.html"
fi
