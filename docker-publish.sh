#!/bin/bash

set -e # Stop on any error

IMAGE_NAME="$1"
echo "Got parameters correctly: IMAGE_NAME=$IMAGE_NAME"

VERSION=$(poetry version | grep -oP "[0-9]+\.[0-9]{3}")
if [[ -z "$VERSION" ]]; then
  echo "Failed to extract version from poetry."
  exit 1
fi
echo "Got Version: $VERSION"

# TAGGING AND PUSHING
echo "Tagging docker image..."
docker tag "$IMAGE_NAME:latest" "$IMAGE_NAME:latest"
docker tag "$IMAGE_NAME:latest" "$IMAGE_NAME:$VERSION"

echo "Pushing image"
docker push "$IMAGE_NAME:latest"
docker push "$IMAGE_NAME:$VERSION"

