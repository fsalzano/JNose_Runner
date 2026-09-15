#!/usr/bin/env bash

set -u

# ============================================================
# JNose batch runner
#
# Usage:
#   ./run_jnose_all.sh <projects_directory> [results_directory]
#
# Example:
#   ./run_jnose_all.sh ~/Desktop/java-projects
#
#   ./run_jnose_all.sh \
#       ~/Desktop/java-projects \
#       ~/Desktop/emse-analysis/results
# ============================================================

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <projects_directory> [results_directory]"
    exit 1
fi

INPUT_ROOT="$(realpath "$1")"

if [ "$#" -ge 2 ]; then
    OUTPUT_ROOT="$(realpath -m "$2")"
else
    OUTPUT_ROOT="$HOME/Desktop/emse-analysis/results"
fi

TOOLS_ROOT="$HOME/Desktop/emse-analysis/tools"

JNose_JAR="$TOOLS_ROOT/jnose-core.jar"
RUNNER_BIN="$TOOLS_ROOT/bin"

# ------------------------------------------------------------
# Checks
# ------------------------------------------------------------

if [ ! -d "$INPUT_ROOT" ]; then
    echo "ERROR: input directory does not exist:"
    echo "$INPUT_ROOT"
    exit 1
fi

if [ ! -f "$JNose_JAR" ]; then
    echo "ERROR: JNose jar not found:"
    echo "$JNose_JAR"
    exit 1
fi

if [ ! -f "$RUNNER_BIN/JNoseBatchRunner.class" ]; then
    echo "ERROR: JNoseBatchRunner.class not found:"
    echo "$RUNNER_BIN"
    exit 1
fi

mkdir -p "$OUTPUT_ROOT"
mkdir -p "$OUTPUT_ROOT/logs"

STATUS_FILE="$OUTPUT_ROOT/status.csv"

echo "project,status" > "$STATUS_FILE"

SUCCESS=0
FAILED=0
TOTAL=0

echo
echo "============================================================"
echo "JNose batch analysis"
echo "============================================================"
echo "Projects: $INPUT_ROOT"
echo "Results:  $OUTPUT_ROOT"
echo "============================================================"
echo

# ------------------------------------------------------------
# Analyze every immediate subdirectory
# ------------------------------------------------------------

while IFS= read -r -d '' PROJECT_PATH; do

    PROJECT_NAME="$(basename "$PROJECT_PATH")"

    TOTAL=$((TOTAL + 1))

    PROJECT_OUTPUT="$OUTPUT_ROOT/$PROJECT_NAME"
    LOG_FILE="$OUTPUT_ROOT/logs/$PROJECT_NAME.log"

    mkdir -p "$PROJECT_OUTPUT"

    echo
    echo "------------------------------------------------------------"
    echo "[$TOTAL] Analyzing: $PROJECT_NAME"
    echo "Path: $PROJECT_PATH"
    echo "------------------------------------------------------------"

    docker run --rm \
        -v "$INPUT_ROOT:/projects:ro" \
        -v "$TOOLS_ROOT:/tools:ro" \
        -v "$OUTPUT_ROOT:/results" \
        eclipse-temurin:25-jdk \
        java \
        -cp "/tools/bin:/tools/jnose-core.jar" \
        JNoseBatchRunner \
        "/projects/$PROJECT_NAME" \
        "/results/$PROJECT_NAME" \
        > "$LOG_FILE" 2>&1

    EXIT_CODE=$?

    if [ "$EXIT_CODE" -eq 0 ]; then

        echo "OK: $PROJECT_NAME"

        echo "\"$PROJECT_NAME\",OK" >> "$STATUS_FILE"

        SUCCESS=$((SUCCESS + 1))

    else

        echo "FAILED: $PROJECT_NAME"
        echo "Log: $LOG_FILE"

        echo "\"$PROJECT_NAME\",FAILED" >> "$STATUS_FILE"

        FAILED=$((FAILED + 1))

    fi

done < <(
    find "$INPUT_ROOT" \
        -mindepth 1 \
        -maxdepth 1 \
        -type d \
        -print0 \
        | sort -z
)

# ------------------------------------------------------------
# Final summary
# ------------------------------------------------------------

echo
echo "============================================================"
echo "Analysis completed"
echo "============================================================"
echo "Total:      $TOTAL"
echo "Successful: $SUCCESS"
echo "Failed:     $FAILED"
echo
echo "Status file:"
echo "$STATUS_FILE"
echo
echo "Logs:"
echo "$OUTPUT_ROOT/logs"
echo "============================================================"