SHELL:=/bin/bash

build_hub:
	pushd cli/hub; go build -o ../../0hub; popd

hub: build_hub
	./0hub
