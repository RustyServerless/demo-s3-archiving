#!/usr/bin/env bash
set -euo pipefail

echo "fetching contenders + state machine"
CONTENDERS=$(aws cloudformation describe-stacks --stack-name demo-s3-archiving-root \
  --query 'Stacks[0].Outputs[?OutputKey==`ContenderArns`].OutputValue' --output text)
SM=$(aws cloudformation describe-stacks --stack-name demo-s3-archiving-root \
  --query 'Stacks[0].Outputs[?OutputKey==`BenchingStateMachineArn`].OutputValue' --output text)

echo "starting execution"
ARN=$(aws stepfunctions start-execution --state-machine-arn "$SM" --input "$CONTENDERS" \
  --query 'executionArn' --output text)
echo "execution: $ARN"

echo "polling until done (this takes a few minutes)..."
while true; do
  STATUS=$(aws stepfunctions describe-execution --execution-arn "$ARN" --query 'status' --output text)
  if [ "$STATUS" != "RUNNING" ]; then
    echo "status: $STATUS"
    break
  fi
  printf '.'
  sleep 15
done
