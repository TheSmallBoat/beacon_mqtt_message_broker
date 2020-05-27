
build for linux in mac osx
~~~~~~~~~~~~~~~~~~~~~~~~~~~
go clean
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "./docker/2020.01.10/mbs_linux" -ldflags "-w"

build docker image
~~~~~~~~~~~~~~~~~~~~~~~~
docker build -m 4g -t beacon/mbs:01.10 -t beacon/mbs:01.10.alpine -t beacon/mbs:01.10.alpine.3.8 ./


docker usage
~~~~~~~~~~~~~~~~~~~~~~~~
docker run -d -it --name beacon_mbs -p 1883:1883 beacon/mbs:01.10.alpine.3.8

Mount Points:
/beacon/config

User/Group
The image runs mbs under the beacon user and group, which are created with a uid and gid of 1883.

Configuration:
When creating a container from the image, the default configuration values are used.
To use a custom configuration file, mount a local configuration file to /beacon/config/mbs.conf

docker run -d -it -p 1883:1883 -v <absolute-path-to-configuration-file>:/beacon/config/mbs.conf beacon/mbs:<version>
