# Test run against the live broker

This runs the plugin against the real MQTT broker and ACL database without
InfluxDB. Metrics are printed as InfluxDB line protocol instead of being
written to a database.

## 1. Build

```powershell
go build -o mqtt_consumer_db.exe .   # run on this Windows machine
.\build.ps1                          # Linux/amd64 binary, run on the server
```

## 2. Create a test ACL row

Do not reuse the production `client_id`: the broker disconnects the existing
session when a second client connects with the same ID. Create a copy of the
production row under a new ID instead. The `password` column holds the hashed
broker password, so copying it keeps the production password valid for the
test client.

```sql
INSERT INTO vmq_auth_acl (mountpoint, client_id, username, password, publish_acl, subscribe_acl)
SELECT mountpoint, 'server1_readonly_test', 'server1_readonly_test', password, publish_acl, subscribe_acl
FROM vmq_auth_acl
WHERE client_id = 'server1_readonly';
```

Adjust the column list if your table differs. Delete the row after the test.

## 3. Configure

```powershell
Copy-Item test\plugin.conf test\plugin.local.conf
```

Fill in every `CHANGE_ME` in `test/plugin.local.conf` from the production
`plugin.conf`. The `.local.conf` file is gitignored. Set `data_format` to
`json_v2` for JSON payloads or remove it to parse single float values.

## 4. Run

Directly, without Telegraf. Metrics go to stdout, logs to stderr. The process
stops when stdin closes or on Ctrl+C, so keep the terminal attached.

```powershell
.\mqtt_consumer_db.exe -config test\plugin.local.conf
```

```sh
tail -f /dev/null | ./mqtt_consumer_db -config test/plugin.local.conf
```

Or through Telegraf with execd, which is the production setup with the output
swapped for stdout:

```sh
telegraf --config test/telegraf.conf
```

## 5. What to expect

Log lines carry a level prefix: `[I]` info, `[D]` debug, `[E]` error. With
`debug = true` the plugin also prints a `[mqtt_consumer_db:test]` line per
step and one line per forwarded metric. On startup, in this order:

1. `found 1 plugin instance(s)` and `started instance 0`.
2. `Topics: [...]` with the patterns from the ACL row.
3. `Listening for topic changes on channel "mqtt_topics_changed"`.
4. `Connected [ssl://...]` (logged at `[D]`).
5. Line protocol on stdout as messages arrive.

## 6. Checks

| Experiment | Expected |
|---|---|
| Publish `{"ts":"abc","x":1}` to a subscribed topic (json_v2) | One error line about the timestamp, process keeps running. Previously this crashed the plugin. |
| Publish `{"ts":1700000000000,"x":1}` (json_v2) or `23.5` (value) | One line of line protocol on stdout. |
| Restart the broker | `connection lost`, then `Connected` again and messages flowing, without a plugin restart. Let more than 1000 messages through to be sure the flow does not stall. |
| Stop the broker before starting the plugin, then start the broker | `mqtt_consumer start failed, retrying in ...`, later `MQTT consumer restarted`. |
| Change the test row's `subscribe_acl`, then `NOTIFY mqtt_topics_changed, 'server1_readonly_test';` | `Subscription topics changed, restarting MQTT consumer with N topics`. |
| Send the same NOTIFY without changing the ACL | `subscription topics unchanged` (only with `debug = true`). |
| Restart PostgreSQL | `topic listener failed, reconnecting in ...`, later `Listening for topic changes` again. Messages keep flowing meanwhile. |
| Stop PostgreSQL before starting the plugin, then start it | `loading subscription topics failed, starting MQTT consumer once the database is reachable`, later `Starting MQTT consumer with N topics`. |
| Add a second `[[inputs.mqtt_consumer_db]]` with a wrong broker port | `running 2 of 2 plugin instance(s)`. The bad instance logs `mqtt_consumer start failed, retrying in ...` every 30 s, the good one keeps delivering. Errors from the broker carry the `[mqtt_consumer_db:<server_id>]` prefix. |
| Add a second instance without `client_id` | `error initializing instance 1 (...), skipping it`, then `running 1 of 2 plugin instance(s)`. |
