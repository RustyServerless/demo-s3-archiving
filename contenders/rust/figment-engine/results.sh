#!/usr/bin/env bash
set -euo pipefail

echo "grabbing ARN"
SM=$(aws cloudformation describe-stacks --stack-name demo-s3-archiving-root \
  --query 'Stacks[0].Outputs[?OutputKey==`BenchingStateMachineArn`].OutputValue' --output text)
ARN=$(aws stepfunctions list-executions --state-machine-arn "$SM" \
  --max-results 1 --query 'executions[0].executionArn' --output text)

STATUS=$(aws stepfunctions describe-execution --execution-arn "$ARN" --query 'status' --output text)
echo "status: $STATUS"
if [ "$STATUS" != "SUCCEEDED" ]; then
  echo "not finished — re-run when SUCCEEDED"
  exit 0
fi

OUT=$(aws stepfunctions describe-execution --execution-arn "$ARN" --query 'output' --output text)

echo "grabbing data"
echo "$OUT" \
| jq -r '.success[] | [(.arn|split(":")|last), .memory_mb, (.duration_ms/1000|floor), (.duration_ms_stddev/1000*100|floor/100), .run_price_usd] | @tsv' \
| sort -k5 -n \
| column -t

echo "any failures?"
echo "$OUT" | jq '.failure'

echo "check for OOM and peak memory"
for m in figment-engine ; do
  echo "=== $m ==="
  aws logs filter-log-events \
    --log-group-name "/aws/lambda/demo-s3-archiving-rust-$m" \
    --filter-pattern "Max Memory Used" \
    --query 'events[].message' --output text \
  | grep -o 'Max Memory Used: [0-9]* MB' | sort -t: -k2 -n | uniq -c
done
