# chapter 1

## A simple http log service


from book: distributed services with GO

```bash
go run cmd/server/main.go
```


```bash
curl -X POST localhost:8080 -d \
'{"record": {"value": "TGV0J3MgR28gIzEK"}}'

curl -X GET localhost:8080 -d '{"offset": 0}'
```


# chapter 2

## log structure

![./log_structure.png]

```bash
cd internal/log/
go test -v
```
