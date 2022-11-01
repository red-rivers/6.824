#/bin/sh

rm out*

for i in {1..10}
do
go test -run 2B
done
