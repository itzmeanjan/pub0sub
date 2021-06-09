SHELL:=/bin/bash

build_hub:
	pushd cli/hub; go build -o ../../0hub; popd

hub: build_hub
	./0hub

docker_hub:
	docker build -t 0hub .

run_hub:
	docker run --name hub --env-file 0hub.env -d 0hub

stop_hub:
	docker stop hub

remove_hub:
	docker rm hub

build_pub:
	pushd cli/publisher; go build -o ../../0pub; popd

pub: build_pub
	./0pub

build_sub:
	pushd cli/subscriber; go build -o ../../0sub; popd

sub: build_sub
	./0sub
