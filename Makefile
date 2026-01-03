.PHONY: test diff deploy-only
POETRY_NAME=$(shell poetry version | awk '{print $$1}')
POETRY_VERSION=$(shell poetry version | awk '{print $$2}')


build-and-publish:
    # Get the project version using poetry
	$(info Building options trader)
	poetry lock --no-update
	poetry export -f requirements.txt --without-hashes --output requirements.txt
	poetry env info --path | rm -fr

	echo "Building & deploying docker image..."
	./docker-build.sh prpa1984/options-trader Dockerfile
	./docker-publish.sh prpa1984/options-trader
