#!/usr/bin/env bash
set -euo pipefail

echo "forcing pipeline"
aws codepipeline start-pipeline-execution --name release-demo-s3-archiving
