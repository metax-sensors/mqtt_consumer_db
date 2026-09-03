# Install Instructions

Build the telegraf image with the plugin included (recommended):

$ docker build -t metax-sensors/mqtt_consumer_db:latest .

Or build only the binary

$ ./build.ps1

Create a plugin.conf for the external plugin configuration. See [mqtt_consumer_db][] for its content.

Create a telegraf config file with the following content:

```
[[inputs.execd]]
  command = ["/etc/telegraf/extern_plugin/mqtt_consumer_db", "-config", "/etc/telegraf/extern_plugin/plugin.conf"]
  signal = "none"
  data_format = "influx"
  name_override = "dot"
  tags = {dot = "default_event_log"}
  
[[outputs.influxdb_v2]]
  urls = ["http://influxdb:8086"]
  token = "SOME-TOKEN"
  organization = "SOME-ORG"
  bucket = "SOME-BUCKET"
  tagexclude = ["dot", "bone"]
  
  data_format = "json"
  
  [outputs.influxdb_v2.tagpass]
    dot = ["default_event_log"]
```

[mqtt_consumer_db]: /plugins/inputs/mqtt_consumer_db/README.md