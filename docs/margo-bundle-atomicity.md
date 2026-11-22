# Margo bundle replacement and version ordering

Replacing a device's bundle must not temporarily remove its state row. In the
previous path, `UpsertDeploymentBundle` deleted the row and waited 500 ms before
inserting the replacement. A concurrent `ShouldReplaceBundle` poll could see
NotFound, create an initial version-1 bundle, and cause an already-synchronized
device to reject the regressed manifest. The device's rollback check is correct
and is not changed by this fix.

The replacement now uses a single state-provider Upsert (a mutex-protected map
assignment for MemoryStateProvider; one HSET for RedisStateProvider). Failed writes
leave the old row present. Only typed NotFound permits first-time initialization;
transport errors and decode failures propagate without creating version 1.

Version allocation, bundle construction and publication are serialized with
first-time initialization for each device. Bounded striped locks are shared by
NBI/SBI database wrappers in the same process. A separate write lock protects
version checks against late cache/event writes. Older versions and same-version
content changes are refused; exact same-version delivery is idempotent. Event
callbacks run after the storage lock is released.

These are process-local locks for the reference WFM. They do not implement a
cross-replica transaction or distributed fencing; a multi-replica deployment
needs provider-level conditional updates/locking. This change also does not
repair a version counter that was already lost: recovery must preserve the
device watermark and advance authoritative WFM state through valid operations.

`bundle_concurrency_test.go` uses a gated provider around MemoryStateProvider to
pause a real replacement before commit while another goroutine polls. Other
cases cover concurrent initialization across manager wrappers, concurrent
rebuilds, backend/decode failures, failed writes, old/conflicting events and
synchronous cache callbacks. Its deployment collection is intentionally empty;
it tests state/version concurrency, not registry pulls or workload installation.

Run from `api`:

```sh
go test -race -count=25 ./pkg/apis/v1alpha1/managers/margo
go vet ./pkg/apis/v1alpha1/managers/margo
```

The gated-poller and concurrent-initialization/read-error regressions fail on
unmodified main at `932ef320b6491da3ec484cf4aa2d69900dbb4445` (the full base SHA is
recorded by the pull request). No running WFM or device database needs resetting
to run these tests.
