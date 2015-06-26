#!/bin/sh

ETCDBIN=$GOPATH/etcd-v2.1.0-alpha.1-linux-amd64

# clear etcd
$ETCDBIN/etcdctl rm --recursive reducer+test/

echo $azureAccountKey
./process_sentence_reducer -job="reducer test" -type=c -azureAccountKey=$azureAccountKey > www.txt&

# need to wait for controller to setup
sleep 2
./process_sentence_reducer -job="reducer test" -type=t -azureAccountKey=$azureAccountKey > www1.txt &
./process_sentence_reducer -job="reducer test" -type=t -azureAccountKey=$azureAccountKey > www2.txt &
./process_sentence_reducer -job="reducer test" -type=t -azureAccountKey=$azureAccountKey > www3.txt &
./process_sentence_reducer -job="reducer test" -type=t -azureAccountKey=$azureAccountKey > www4.txt &
./process_sentence_reducer -job="reducer test" -type=t -azureAccountKey=$azureAccountKey > www5.txt &
# ./ex_mapper -job="mapper test" -type=t -azureAccountKey=$azureAccountKey > www3.txt &
# ./ex_mapper -job="mapper test" -type=t -azureAccountKey=$azureAccountKey > www4.txt &
# ./ex_mapper -job="mapper test" -type=t -azureAccountKey=$azureAccountKey > www5.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www3.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www4.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www5.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www6.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www7.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www8.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www9.txt &
# ./ex -job="mapreduce test" -type=t -azureAccntKey=$azureAccountKey > www10.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www11.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www12.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www13.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www14.txt &
 


