# DurableStack Node.js Contracts (Phase 0)

## Run Status Vocabulary

- `pending`
- `leased`
- `succeeded`
- `failed`

## Event Types

- `job_claimed`
- `job_started`
- `job_succeeded`
- `job_failed`
- `job_retried`
- `retry_scheduled`
- `worker_heartbeat`

## Event Version

- `2`

## Runtime-Control Command Types

- `set_schedule_enabled`
- `run_schedule_now`
- `update_schedule_cron`

## Runtime-Control Receipt Status Values

- `leased`
- `acknowledged`
- `succeeded`
- `failed`

## External Payload Contract Policy

- Field names and value semantics align with current platform-facing .NET payloads.
- Validators are strict for required fields and permissive for nullable optional fields.
- Breaking payload changes require coordinated runtime + platform versioning.
