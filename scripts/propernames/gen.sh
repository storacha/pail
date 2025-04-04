while read p; do
  pail put $p $(pail-randomcid) --max-shard-size=10000
done </usr/share/dict/propernames
