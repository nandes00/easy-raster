#!/bin/bash
set -e

awslocal sqs create-queue \
  --queue-name tracking-raw.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true

awslocal s3 mb s3://${S3_BUCKET:-easyraster} 2>/dev/null || true

echo "LocalStack resources ready"