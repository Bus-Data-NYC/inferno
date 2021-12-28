#!/bin/bash
# Runner for aws batch jobs
# Useful when doing:
# aws batch submit-job --job-name foo --job-queue bar --job-definition caller \
# --array-properties size=31 \
# --container-overrides "environment=[{name=YEAR,value=2021},{name=MONTH,value=10}]"
INFERNOFLAGS=${INFERNOFLAGS---quiet --incomplete}
arrayday=$(date -d "${YEAR}-${MONTH}-01 + ${AWS_BATCH_JOB_ARRAY_INDEX} days" +"%d")
day=${DAY:-$arrayday}
DAY=$(echo ${day:-1} | xargs printf "%02.0f")
./src/inferno.py ${INFERNOFLAGS} ${YEAR}-${MONTH}-${DAY}
