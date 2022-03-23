# MDMP router


## Development

To regenerate the proto files, ensure you have installed the generate dependencies:

```bash
$ make install
go install \
        ./vendor/github.com/gogo/protobuf/protoc-gen-gogo
go mod vendor      
```

It also requires you to have the Google Protobuf compiler `protoc` installed.
Please follow instructions for your platform on the
[official protoc repo](https://github.com/google/protobuf#protocol-compiler-installation).

Regenerate the files by running `make generate`:

```bash 
$ make generate
```



