# go-sql

## Getting Started:
**Installing**
```
go get -u github.com/digital-dream-labs/go-sql@latest
```

**Supported CLI flags**

The cli flags are found in the `flags/` folder.
TLS is not supported yet.

**Building**

Build flags are required; `mysql,postgres`
```
go build -tag=postgres main.go
```

## Notes:
------------------
### Migration
When a DBX() interface is called it will pull from a list of connections. This list of connections is create for testing purposes. It helps speed up testing by not create unnecessary db connections. So when using it from a testing perspective don't ever send "different" migrations paths because the subsequent New() calls will return an already existing connection.