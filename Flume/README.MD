launch:
./bin/flume-ng agent --conf conf --conf-file conf/file_to_kafka.conf  -Dflume.root.logger=DEBUG,console --name a1 -Xmx512m -Xms256m