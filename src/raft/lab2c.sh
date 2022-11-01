#/bin/sh

rm out*

for i in {1..10}
do
go clean -testcache
go test -run 2C > out$i
echo out$i result
cat out$i | grep -i fail
done

cat out* | grep -i fail
