#!/bin/bash

GIT_BRANCH=$1
IMAGE_NAME="ubcsolarstrategy/sunbeam:${GIT_BRANCH}"

# Clone the repo and checkout the branch
echo -e "Cloning at Sunbeam from branch ${GIT_BRANCH}..."
git clone -b $GIT_BRANCH https://github.com/UBC-Solar/sunbean.git repo-$GIT_BRANCH
cd repo-$GIT_BRANCH
echo -e "Successfully cloned."

# Build Docker image
echo -e "Building Docker image ${IMAGE_NAME}..."
docker build -t $IMAGE_NAME ./pipeline
echo -e "Image has been successfully built..."

# Push to Docker Hub
echo -e "Pushing image to Docker Hub."
docker push $IMAGE_NAME
echo -e "Image has been successfully pushed."

# Cleanup
echo -e "Cleaning up..."
cd ..
rm -rf repo-$GIT_BRANCH
echo -e "Successfully built and pushed a Sunbeam deployment for ${GIT_BRANCH}"