#!/usr/bin/env bash
set -euo pipefail

echo "Initializing LocalStack SQS FIFO queues for Distributed Wagering Processor..."

# 1. Create Dead-Letter Queue (FIFO)
DLQ_URL=$(awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes "FifoQueue=true,ContentBasedDeduplication=false" \
  --query 'QueueUrl' --output text)

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)

# 2. Create Main Wagering Queue (FIFO) with RedrivePolicy pointing to DLQ
awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "{
    \"FifoQueue\": \"true\",
    \"ContentBasedDeduplication\": \"false\",
    \"VisibilityTimeout\": \"30\",
    \"ReceiveMessageWaitTimeSeconds\": \"1\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"
  }"

# 3. Create Integration Events Outbox Queue (FIFO) for published domain events
awslocal sqs create-queue \
  --queue-name wager-integration-events.fifo \
  --attributes "FifoQueue=true,ContentBasedDeduplication=false"

echo "SQS FIFO queues ready!"
awslocal sqs list-queues
