#!/bin/sh

ETCDBIN=$GOPATH/etcd-v2.0.5-darwin-amd64

# clear etcd
$ETCDBIN/etcdctl rm --recursive mapper+test/

echo $azureAccountKey
./ex_mapper -job="mapper test" -type=c -azureAccountKey=$azureAccountKey > www.txt&

# need to wait for controller to setup
sleep 2
./ex_mapper -job="mapper test" -type=t -azureAccountKey=$azureAccountKey > www1.txt &
./ex_mapper -job="mapper test" -type=t -azureAccountKey=$azureAccountKey > www2.txt &
./ex_mapper -job="mapper test" -type=t -azureAccountKey=$azureAccountKey > www3.txt &
./ex_mapper -job="mapper test" -type=t -azureAccountKey=$azureAccountKey > www4.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www3.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www4.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www5.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www6.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www7.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www8.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www9.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www10.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www11.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www12.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www13.txt &
# ./ex -job="mapreduce test" -type=t -azureAccountKey=$azureAccountKey > www14.txt &
 


