#/bin/sh

rm out*

go clean -testcache
go test -run 2A
go test -run 2B
go test -run 2C
go test -run 2D

echo Good!!!
