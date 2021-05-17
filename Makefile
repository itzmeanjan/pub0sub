SHELL:=/bin/bash

build_hub:
	pushd cli/hub; go build -o ../../0hub; popd

hub: build_hub
	./0hub

build_pub:
	pushd cli/publisher; go build -o ../../0pub; popd

pub: build_pub
	./0pub
