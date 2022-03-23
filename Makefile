# MDMP Makefile

GOCMD = GO111MODULE=on go

VERSION := $(shell grep "const Version " pkg/version/version.go | sed -E 's/.*"(.+)"$$/\1/')
GIT_COMMIT=$(shell git rev-parse HEAD)
GIT_DIRTY=$(shell test -n "`git status --porcelain`" && echo "+CHANGES" || true)
BUILD_DATE=$(shell date '+%Y-%m-%d-%H:%M:%S')
GOBUILD = $(GOCMD) build -ldflags "-X github.com/tkeel-io/rule-$(BINNAME)/pkg/version.GitCommit=${GIT_COMMIT}${GIT_DIRTY} -X github.com/tkeel-io/rule-$(BINNAME)/pkg/version.BuildDate=${BUILD_DATE}"

DOCKERTAG?=tkeelio/rulex:dev

BINNAME = rulex
BINNAMES = rulex

run:
	@echo "---------------------------"
	@echo "-         Run             -"
	@echo "---------------------------"
	@$(GOCMD) run main.go serve --conf conf/rulex-example.toml

build:
	@rm -rf bin/
	@mkdir bin/
	@echo "---------------------------"
	@echo "-        build...         -"
	@$(GOBUILD)    -o bin/$(BINNAME)
	@echo "-     build(linux)...     -"
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64  $(GOBUILD) -o bin/linux/$(BINNAME)
	@echo "-    builds completed!    -"
	@echo "---------------------------"
	@bin/rulex version

docker-build:
	docker build -t $(DOCKERTAG) .
docker-push:
	docker push $(DOCKERTAG)
docker-auto:
	docker build -t $(DOCKERTAG) .
	docker push $(DOCKERTAG)
.PHONY: install generate

-include .dev/*.makefile