# world-state-aggregator

Viam module that aggregates runtime-attached geometries from many producers into a single world state store. Other modules push transforms via `DoCommand`; consumers read them through the standard `rdk:service:world_state_store` API.

## Model viam:world-state-aggregator:default

An in-memory aggregator implementing `rdk:service:world_state_store`. Producers push via `DoCommand`; consumers either poll (`ListUUIDs` + `GetTransform`) or subscribe (`StreamTransformChanges`).

State is in-memory only and not persisted across restarts. Producers are the source of truth — they republish on their own startup.

### Configuration

```json
{
  "subscriber_buffer_size": 64
}
```

| Attribute | Type | Required | Description |
|---|---|---|---|
| `subscriber_buffer_size` | int | no | Per-subscriber channel buffer. Default 64. When a subscriber's buffer fills, changes are dropped and logged for that subscriber only; producers are never blocked. |

### Read API

Standard `rdk:service:world_state_store` methods:

- `ListUUIDs(ctx, extra)` — snapshot of all current transform UUIDs.
- `GetTransform(ctx, uuid, extra)` — current value for one UUID.
- `StreamTransformChanges(ctx, extra)` — subscribe to add/update/remove events. No replay of existing state; consumers call `ListUUIDs` + `GetTransform` first to bootstrap.

### Write API (DoCommand)

> **Status:** stub — not implemented yet. Will land in a follow-up PR.

| Command | Args | Effect |
|---|---|---|
| `set_transform` | `{uuid, reference_frame, pose_in_observer_frame, physical_object?, metadata?}` | Upsert. Emits `ADDED` if new, `UPDATED` if existing. |
| `remove_transform` | `{uuid}` | Delete. Emits `REMOVED`. Unknown UUID is a silent no-op. |
| `list_transforms` | `{}` | Snapshot of all transforms. For CLI debug. |

UUIDs are operator-namespaced strings on the wire (e.g. `tool-changer-1/attached`), stored as bytes internally.

## Status

Under construction. PR #1 lands the module scaffold with a stub Service. Storage and write API in follow-up PRs.
