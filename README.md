### Sprawl Hub

A Golang implementation of the PPSSPP relay server protocol, as referenced
from https://github.com/Kethen/aemu_postoffice.

This project also implements the aemu `pspnet_adhocctl_server` server, 
providing a single binary with both relay & adhocctl capabilities.

#### Deprecation warning

With PPSSPP 1.21.0, the reference relay server implementation also supports using the
[ENnet](http://enet.bespin.org/) protocol for relayed packets. This project does NOT support it
and will likely break when users try to use it.

#### Running the server

To run the server, first you need GoLang 1.27 installed, after that you can run:

```bash
go run cmd/sprawl/main.go
```

A `Dockerfile` is also available to build a docker image.

##### AI Disclosure

Some parts of this project have been written with the assistance of LLMs, although the majority is handwritten.