# Try it

Copy-paste commands to stand up the operator on a local `kind` cluster, create a
`ValkeyCluster`, and exercise every feature. Run from the repo root.

> All `kubectl` here targets the local kind cluster. Double-check you're on it:
> `kubectl config current-context` should print `kind-valkeycluster-dev`
> (switch with `kubectl config use-context kind-valkeycluster-dev`).

## 0. One-time: cluster + operator

```bash
make kind-create                                  # 3-node kind cluster (skip if it exists)
make kind-deploy IMG=valkeycluster-operator:dev   # build, load, install CRD, deploy operator
kubectl -n valkeycluster-system get deploy        # operator should be Available
```

## 1. Create the cluster

```bash
kubectl apply -f try/valkeycluster.yaml

# watch it form (Pending -> Provisioning -> Forming -> Ready)
kubectl get valkeycluster try -w
# Ctrl-C once PHASE=Ready, READY=3
```

Inspect it:

```bash
kubectl get valkeycluster try                      # SHARDS/REPLICAS/PHASE/READY columns
kubectl describe valkeycluster try                 # conditions + per-shard primary/slots/replicas
kubectl get pods -l app.kubernetes.io/instance=try -o wide   # 6 pods, spread across nodes
```

Confirm the real Valkey cluster is healthy (3 primaries, all slots covered):

```bash
kubectl exec try-shard-0-0 -- valkey-cli --cluster check 127.0.0.1:6379
```

Check the operator-derived maxmemory (from the 256Mi limit, ~70%):

```bash
kubectl exec try-shard-0-0 -- valkey-cli -p 6379 config get maxmemory maxmemory-policy
```

## 2. Use it — write/read across shards (cluster-aware client)

`-c` puts `valkey-cli` in cluster mode so it follows `MOVED`/`ASK` redirects.

```bash
# write 10 keys; they hash to different shards
kubectl exec try-shard-0-0 -- sh -c \
  'for i in $(seq 1 10); do valkey-cli -c -p 6379 set key:$i "value-$i"; done'

# read them back
kubectl exec try-shard-0-0 -- sh -c \
  'for i in $(seq 1 10); do valkey-cli -c -p 6379 get key:$i; done'

# see which slot/shard a key lands in
kubectl exec try-shard-0-0 -- valkey-cli -p 6379 cluster keyslot key:1
```

Interactive shell into the cluster:

```bash
kubectl exec -it try-shard-0-0 -- valkey-cli -c -p 6379
# then: set foo bar / get foo / cluster nodes / cluster info
```

## 3. Automatic failover (HA)

```bash
# note the current primary of shard 1
kubectl get valkeycluster try -o jsonpath='{.status.shards[1].primaryPod}'; echo

# kill it
kubectl delete pod try-shard-1-0

# within ~5s a replica is promoted; the cluster stays covered
kubectl exec try-shard-0-0 -- valkey-cli --cluster check 127.0.0.1:6379 | grep covered
kubectl get valkeycluster try -o jsonpath='{.status.shards[1].primaryPod}'; echo   # new primary
# the deleted pod returns and rejoins as a replica
kubectl get pods -l app.kubernetes.io/instance=try
```

## 4. Data-preserving resharding (scale shards)

```bash
# write a marker set first
kubectl exec try-shard-0-0 -- sh -c 'for i in $(seq 1 100); do valkey-cli -c -p 6379 set m:$i $i >/dev/null; done; echo wrote 100'

# grow 3 -> 5 shards
kubectl patch valkeycluster try --type merge -p '{"spec":{"shards":5}}'
kubectl get valkeycluster try -w        # passes through Resharding back to Ready (READY=5)

# verify exactly 5 primaries, full coverage, and data survived
kubectl exec try-shard-0-0 -- valkey-cli --cluster check 127.0.0.1:6379 | grep -E "primaries|covered"
kubectl exec try-shard-0-0 -- sh -c 'ok=0; for i in $(seq 1 100); do [ "$(valkey-cli -c -p 6379 get m:$i)" = "$i" ] && ok=$((ok+1)); done; echo "preserved $ok/100"'

# shrink back 5 -> 3 (slots migrate off departing shards first; no data loss)
kubectl patch valkeycluster try --type merge -p '{"spec":{"shards":3}}'
kubectl get valkeycluster try -w
```

## 5. Scale replicas (HA copies per shard)

```bash
kubectl patch valkeycluster try --type merge -p '{"spec":{"replicasPerShard":2}}'
# each shard's StatefulSet goes to 3 pods; no resharding
kubectl get statefulset -l app.kubernetes.io/instance=try
```

## 6. Tune the HA policy (durability vs availability)

```bash
# require a replica ack before accepting writes (stronger durability)
kubectl patch valkeycluster try --type merge -p '{"spec":{"haPolicy":{"minReplicasToWrite":1}}}'
```

## 7. Self-healing demo (operator reconciles drift)

```bash
# delete a StatefulSet out-of-band; the operator recreates it
kubectl delete statefulset try-shard-2
kubectl get statefulset -l app.kubernetes.io/instance=try -w
```

## 8. Benchmarks (optional)

```bash
REQUESTS=50000 bash bench/benchmark.sh try          # throughput + WAIT durability  -> bench/RESULTS.md
bash bench/failover-bench.sh try                    # failover window vs node-timeout -> bench/FAILOVER-RESULTS.md
```

## 9. Teardown

```bash
kubectl delete -f try/valkeycluster.yaml            # finalizer reclaims PVCs; owner refs GC the rest
# (optional) remove the operator and/or the kind cluster:
make undeploy
make kind-delete
```

## Troubleshooting

```bash
kubectl -n valkeycluster-system logs deploy/valkeycluster-controller-manager   # operator logs
kubectl get valkeycluster try -o yaml | yq '.status'                           # full status
kubectl exec try-shard-0-0 -- valkey-cli cluster nodes                         # raw membership
```
