############################################################ 
# Dockerfile to build golang Installed Containers 

# Based on alpine

############################################################

FROM golang:1.17 AS builder

COPY . /src
WORKDIR /src

RUN GOPROXY=https://goproxy.cn make build

FROM alpine:3.13

RUN mkdir /keel
COPY --from=builder /src/bin/linux/rulex /keel
COPY --from=builder /src/rulex-dev.toml /keel/


EXPOSE 10631
WORKDIR /keel
CMD ["/keel/rulex", "serve", "--conf", "/keel/rulex-dev.toml"]
