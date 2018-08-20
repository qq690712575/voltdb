#!/bin/bash
for P in `kubectl get pods | egrep "^$1" | cut -d\  -f1`
do
    kubectl delete pods $P --grace-period=0 --force
done
kubectl delete statefulset $1 --grace-period=0 --force
kubectl delete service $1
if [ $# -gt 1 -a .$2 = .PVC ]; then
    for P in `kubectl get pvc | egrep "$1" | cut -d\  -f1`
    do
        kubectl delete pvc $P
    done
fi
