# ember helm chart

deploy [ember](https://github.com/kacy/ember) on kubernetes.

## install

```bash
helm repo add ember https://charts.emberdb.com
helm install ember ember/ember
```

with custom values:

```bash
helm install ember ember/ember \
  --set ember.maxMemory=256mb \
  --set ember.requirepass=secret \
  --set resources.limits.memory=512Mi
```

## uninstall

```bash
helm uninstall my-ember
```

## configuration

| parameter | description | default |
|-----------|-------------|---------|
| `replicaCount` | number of pod replicas | `1` |
| `image.repository` | container image name | `ember` |
| `image.tag` | image tag | `latest` |
| `image.pullPolicy` | image pull policy | `IfNotPresent` |
| `service.type` | kubernetes service type | `ClusterIP` |
| `service.port` | service port | `6379` |
| `ember.port` | ember listen port | `6379` |
| `ember.metricsPort` | prometheus metrics port | `9100` |
| `ember.maxMemory` | max memory limit (e.g. `256mb`) | `""` (unlimited) |
| `ember.evictionPolicy` | eviction policy when memory is full | `noeviction` |
| `ember.appendonly` | enable AOF persistence | `false` |
| `ember.appendfsync` | AOF fsync policy (`always`, `everysec`, `no`) | `everysec` |
| `ember.concurrent` | use concurrent mode instead of sharded | `false` |
| `ember.requirepass` | require password for client connections | `""` (disabled) |
| `resources` | CPU/memory resource requests and limits | `{}` |
| `serviceAccount.create` | create a service account | `true` |
| `serviceAccount.name` | override service account name | `""` |

## examples

### production with memory limits and auth

```yaml
# values-prod.yaml
replicaCount: 1

ember:
  maxMemory: 1gb
  evictionPolicy: allkeys-lru
  appendonly: true
  appendfsync: everysec
  requirepass: "my-secret-password"

resources:
  limits:
    cpu: "4"
    memory: 2Gi
  requests:
    cpu: "1"
    memory: 1Gi
```

```bash
helm install ember-prod ember/ember -f values-prod.yaml
```

### development (no persistence, no auth)

```bash
helm install ember-dev ember/ember \
  --set resources.limits.memory=256Mi
```
