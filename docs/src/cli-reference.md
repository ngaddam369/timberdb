# CLI Reference

timberdb ships a command-line interface for working with databases directly from the shell.

## Installation

```bash
make build          # produces ./bin/timberdb
# or
go install github.com/ngaddam369/timberdb/cmd/timberdb@latest
```

## Commands

- [`append`](#append) — write one or more records
- [`scan`](#scan) — read records by time range
- [`inspect partitions`](#inspect-partitions) — list partitions with state and size
- [`inspect sstable`](#inspect-sstable) — inspect a single SSTable file
- [`inspect wal`](#inspect-wal) — inspect a WAL segment
- [`inspect manifest`](#inspect-manifest) — inspect the manifest version history

---

## append

Write a single record or a batch of records from an NDJSON file.

```
timberdb append --db <path> [--source <id>] [--timestamp <RFC3339>] [--payload <bytes>]
timberdb append --db <path> --file <ndjson>
```

### Flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--db` | yes | — | Path to the database directory |
| `--source` | no | `""` | Source identifier for the record |
| `--timestamp` | no | now | Record timestamp in RFC3339 format |
| `--payload` | no | `""` | Record payload bytes |
| `--file` | no | — | Path to an NDJSON file containing records to batch-append |

`--file` cannot be combined with `--source`, `--timestamp`, or `--payload`.

### Single record

```bash
timberdb append \
  --db /var/lib/timberdb \
  --source app-server \
  --timestamp 2025-06-20T14:30:00Z \
  --payload '{"level":"info","msg":"service started"}'
```

### Batch from NDJSON file

Each line in the NDJSON file must be a JSON object with `timestamp`, `source`, and `payload` fields. `source` and `payload` are base64-encoded byte slices.

```json
{"timestamp":"2025-06-20T14:30:00Z","source":"YXBwLXNlcnZlcg==","payload":"eyJsZXZlbCI6ImluZm8ifQ=="}
{"timestamp":"2025-06-20T14:30:01Z","source":"ZGItcHJveHk=","payload":"eyJsZXZlbCI6Indhcm4ifQ=="}
```

```bash
timberdb append --db /var/lib/timberdb --file /tmp/events.ndjson
```

---

## scan

Read records within a time range and output them as NDJSON.

```
timberdb scan --db <path> --start <RFC3339> [--end <RFC3339>] [--source <id>]
```

### Flags

| Flag | Required | Default | Description |
|---|---|---|---|
| `--db` | yes | — | Path to the database directory |
| `--start` | yes | — | Start of the scan window (inclusive), RFC3339 |
| `--end` | no | now | End of the scan window (exclusive), RFC3339 |
| `--source` | no | all sources | Filter output to a single source ID |

### Output

One JSON object per line (NDJSON). `source` and `payload` are base64-encoded.

```bash
timberdb scan \
  --db /var/lib/timberdb \
  --start 2025-06-20T00:00:00Z \
  --end 2025-06-20T01:00:00Z
```

```json
{"timestamp":"2025-06-20T00:00:01.123456789Z","source":"YXBwLXNlcnZlcg==","payload":"eyJsZXZlbCI6ImluZm8ifQ=="}
{"timestamp":"2025-06-20T00:00:02.456789012Z","source":"ZGItcHJveHk=","payload":"eyJsZXZlbCI6Indhcm4ifQ=="}
```

### Filtering by source

```bash
timberdb scan \
  --db /var/lib/timberdb \
  --start 2025-06-20T00:00:00Z \
  --source app-server
```

### Piping to jq

```bash
timberdb scan --db /var/lib/timberdb --start 2025-06-20T00:00:00Z \
  | jq '.payload | @base64d | fromjson | .level'
```

---

## inspect partitions

List all partitions with their state, SSTable file count, and on-disk size.

```
timberdb inspect partitions --db <path>
```

### Flags

| Flag | Required | Description |
|---|---|---|
| `--db` | yes | Path to the database directory |

### Output

```bash
timberdb inspect partitions --db /var/lib/timberdb
```

```
WINDOW                                           STATE    SST FILES  SIZE
2025-06-19T00:00:00Z → 2025-06-19T01:00:00Z     sealed   1          48 KiB
2025-06-20T13:00:00Z → 2025-06-20T14:00:00Z     sealed   3          1.2 MiB
2025-06-20T14:00:00Z → 2025-06-20T15:00:00Z     open     1          204 KiB
```

**States:**
- `open` — currently accepting writes; has an active memtable
- `sealed` — time window has closed; read-only; eligible for compaction and retention
- `deleted` — listed only during the brief window between manifest update and process observation

---

## inspect sstable

Inspect the metadata footer of a single SSTable file.

```
timberdb inspect sstable --file <path>
```

### Flags

| Flag | Required | Description |
|---|---|---|
| `--file` | yes | Path to the `.sst` file |

### Output

```bash
timberdb inspect sstable --file /var/lib/timberdb/partition-2025062014/00000001.sst
```

```
File:            /var/lib/timberdb/partition-2025062014/00000001.sst
PartitionStart:  2025-06-20T14:00:00Z
PartitionEnd:    2025-06-20T15:00:00Z
MinTimestamp:    2025-06-20T14:00:01.123456789Z
MaxTimestamp:    2025-06-20T14:30:59.987654321Z
RecordCount:     18432
```

This reads only the 80-byte footer — it does not scan any records.

---

## inspect wal

Inspect a WAL segment and report how many records it contains.

```
timberdb inspect wal --file <path>
```

### Flags

| Flag | Required | Description |
|---|---|---|
| `--file` | yes | Path to the `.wal` file |

### Output

```bash
timberdb inspect wal --file /var/lib/timberdb/wal-000003.wal
```

```
File:        /var/lib/timberdb/wal-000003.wal
RecordCount: 4096
```

This replays the WAL segment, verifying each record's CRC32, and counts the valid records.

---

## inspect manifest

Inspect the manifest file, showing the complete version-edit history and the current live SSTable count.

```
timberdb inspect manifest --db <path>
```

### Flags

| Flag | Required | Description |
|---|---|---|
| `--db` | yes | Path to the database directory |

### Output

```bash
timberdb inspect manifest --db /var/lib/timberdb
```

```
Edit #1: +1 file(s), -0 file(s)
  + /var/lib/timberdb/partition-2025062013/00000001.sst [18432 records, 2025-06-20T13:00:01Z → 2025-06-20T13:59:59Z]

Edit #2: +1 file(s), -0 file(s)
  + /var/lib/timberdb/partition-2025062014/00000001.sst [4096 records, 2025-06-20T14:00:01Z → 2025-06-20T14:15:00Z]

Edit #3: +1 file(s), -1 file(s)
  + /var/lib/timberdb/partition-2025062013/00000002.sst [18432 records, 2025-06-20T13:00:01Z → 2025-06-20T13:59:59Z]
  - /var/lib/timberdb/partition-2025062013/00000001.sst

Live SSTable count: 2
```

Each `Edit` entry corresponds to one `VersionEdit` written by a flush or compaction. `+` lines are added files; `-` lines are deleted files. The final line reports how many SSTable files are live (referenced by the current manifest state).

---

## Timestamp formats

All time inputs accept RFC3339 with or without nanosecond precision:

```
2025-06-20T14:30:00Z              # seconds
2025-06-20T14:30:00.123456789Z   # nanoseconds
2025-06-20T14:30:00+05:30        # with timezone offset
```

Scan and inspect outputs use RFC3339Nano (`2025-06-20T14:30:00.123456789Z`) for full precision.
