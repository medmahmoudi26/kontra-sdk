# Temporal's Grafana dashboards

Vendored unchanged from [temporalio/dashboards](https://github.com/temporalio/dashboards)
at commit `4994df2cf98a901ec78217c111eda95ff087480d`, under its MIT licence ([LICENSE](LICENSE),
Copyright (c) 2025 Temporal Technologies Inc.):

| File | Upstream path | Reads |
|---|---|---|
| `temporal-core-sdks-otel.json` | `sdk/temporal-core-sdks-otel.json` | the metrics of `kontra serve --metrics` (Python is a Core-based SDK: histograms in milliseconds) |
| `server-general.json` | `server/server-general.json` | the metrics of Kontra's Temporal server |

`kontra dev up --observability` provisions both into Grafana (`compose.yaml`,
[`../grafana`](../grafana)). To move to a newer upstream commit, fetch the files again and
change the commit above:

```sh
commit=<upstream commit>
for path in LICENSE sdk/temporal-core-sdks-otel.json server/server-general.json; do
  curl -fsSL --proto '=https' -o "$(basename "$path")" \
    "https://raw.githubusercontent.com/temporalio/dashboards/$commit/$path"
done
```
