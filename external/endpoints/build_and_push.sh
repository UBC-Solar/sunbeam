#!/bin/bash

GIT_BRANCH=$1
IMAGE_NAME="ubcsolarstrategy/sunbeam:${GIT_BRANCH}"

cleanup() {
    # Cleanup
    echo -e "Cleaning up..."
    cd ..
    rm -rf repo-$GIT_BRANCH

    exit 1
}

trap cleanup 1  # Run cleanup function on any error

# Clone the repo and checkout the branch
echo -e "Cloning at Sunbeam from branch ${GIT_BRANCH}..."
git clone -b $GIT_BRANCH https://github.com/UBC-Solar/sunbeam.git repo-$GIT_BRANCH
if [ $? -eq 0 ]; then
    echo -e "Successfully cloned."
else
    echo -e "Failed to clone... Aborting!"
    cleanup
    exit 1
fi
cd repo-$GIT_BRANCH

# Build Docker image
echo -e "Building Docker image ${IMAGE_NAME}..."
docker build -t $IMAGE_NAME ./pipeline
if [ $? -eq 0 ]; then
echo -e "Image has been successfully built..."
else
    echo -e "Failed to build Docker image... Aborting!"
    cleanup
    exit 1
fi

# Push to Docker Hub
echo -e "Pushing image to Docker Hub."
docker push $IMAGE_NAME
echo -e "Image has been successfully pushed."
if [ $? -eq 0 ]; then
    echo -e "Image has been successfully pushed."
else
    echo -e "Failed to push Docker image to Docker Hub."
fi

cleanup
echo -e "Successfully built and pushed a Sunbeam deployment for ${GIT_BRANCH}"