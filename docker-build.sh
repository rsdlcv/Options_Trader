#!/bin/bash

set -e # Stop on any error

IMAGE_NAME="$1"
DOCKERFILE="$2"

echo "Start docker build..."

docker build --platform=linux/amd64 -t "$IMAGE_NAME" -f "$DOCKERFILE" .
