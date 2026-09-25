---
title: "dbt Integration"
description: "Ingest dbt models, sources, and lineage from manifest.json/catalog.json into Semantica's KG pipeline."
icon: "diagram-project"
---

> Turn a dbt project's `manifest.json`/`catalog.json` artifacts into a Context Graph — models, sources, seeds, and snapshots as nodes, with dbt's own model-to-model and model-to-source lineage carried over as relationships.


## Installation

Phase 1 (local artifacts) needs nothing beyond Semantica's core dependencies — `manifest.json`/`catalog.json` are parsed with the standard library, no network access and no dbt install required.

```bash
# Only needed for the dbt Cloud Metadata API (Phase 2)
pip install "semantica[ingest-dbt]"
```


## Basic Usage

Generate the artifacts from your dbt project first:

```bash
dbt docs generate   # writes target/manifest.json and target/catalog.json
```

Then parse them:

```python
from semantica.ingest import DbtIngestor

ingestor = DbtIngestor(
    manifest_path="target/manifest.json",
    catalog_path="target/catalog.json",   # optional — adds confirmed column types
)

data = ingestor.parse_manifest()
print(f"{len(data.nodes)} nodes, {len(data.lineage)} lineage edges")
```

<Tip>
`catalog_path` is optional. Without it you still get every model, source, and the full lineage graph from `parent_map` — you just lose the *materialized* column types that only `catalog.json` (built from `information_schema`) has. Column descriptions from `manifest.json` are kept either way.
</Tip>


## What Gets Parsed

From `manifest.json`'s `nodes`, only `model`, `seed`, and `snapshot` resource types become graph nodes — `test`, `macro`, `analysis`, and `exposure` entries are skipped since they aren't lineage-graph entities. Every entry under `sources` becomes a `source` node.

Lineage edges come from `manifest.json`'s `parent_map`, restricted to edges where both endpoints were kept (an edge into a dropped `test` node is discarded, not left dangling).

| dbt concept | Semantica shape |
|---|---|
| Model / seed / snapshot | Entity document, `type: "dbt_model"` / `"dbt_seed"` / `"dbt_snapshot"` |
| Source | Entity document, `type: "dbt_source"` |
| `parent_map` edge | Relationship document, `type: "feeds"`, `metadata.relationship_type: "dbt_lineage"` |
| Column (manifest description + catalog type) | Entry in the node document's `columns` dict |


## Document Export

Convert parsed dbt data to the Semantica document format for use with `GraphBuilder`:

```python
documents = ingestor.export_as_documents(data)

# Entity document:
# {
#   "id": "model.jaffle_shop.stg_customers",
#   "name": "stg_customers",
#   "type": "dbt_model",
#   "text": "Cleaned customer records.",
#   "database": "analytics",
#   "schema": "staging",
#   "materialized": "view",
#   "tags": ["staging"],
#   "columns": {"customer_id": {"description": "Unique customer id.", "data_type": "INTEGER"}},
#   "source": "dbt",
#   "metadata": {"source": "dbt", "resource_type": "model"},
# }

# Relationship document (lineage edge):
# {
#   "source": "source.jaffle_shop.raw.customers",
#   "target": "model.jaffle_shop.stg_customers",
#   "type": "feeds",
#   "metadata": {"source": "dbt", "relationship_type": "dbt_lineage"},
# }
```

<Note>
`metadata` (not a flat extra key) is where provenance/discriminator tags live, matching the convention `RelationalSchemaMapper` established (see `kg/schema_mapper.py`) — descriptive fields stay flat, `metadata` carries `source`/`table`-like tags. This isn't just a style choice: `GraphBuilder`'s `GraphStore` persistence path reads relationship extras from `rel["metadata"]` specifically, so a flat extra key would silently never reach a persisted edge's properties.
</Note>

`GraphBuilder` accepts relationship dicts (`source`/`target`) directly, so dbt's lineage graph is carried into the Context Graph as-is rather than needing entity extraction to re-derive it from SQL:

```python
from semantica.kg import GraphBuilder

builder = GraphBuilder()
kg = builder.build(documents)
```


## dbt Cloud Metadata API (Phase 2)

For hosted projects without local artifacts, query the dbt Cloud Metadata API instead. Every request is validated through Semantica's SSRF guard (`request_with_ssrf_guard`) since the API host is user-configurable (multi-cell accounts use a per-account host).

```python
import os
from semantica.ingest import DbtIngestor

ingestor = DbtIngestor(
    account_id=os.getenv("DBT_CLOUD_ACCOUNT_ID"),
    token=os.getenv("DBT_CLOUD_TOKEN"),          # dbt Cloud service token
    base_url=os.getenv("DBT_CLOUD_BASE_URL"),     # optional, for multi-cell accounts
)

data = ingestor.fetch_cloud_lineage(job_id=123456)
documents = ingestor.export_as_documents(data)
```

<Warning>
`fetch_cloud_lineage` has not been exercised against a live dbt Cloud account. `job(id: $jobId: BigInt!)` and the `parentsModels`/`parentsSources` field names are confirmed against dbt's own Discovery API reference docs, but the rest of the queried field list and whether `models`/`sources` need cursor pagination (`first`/`after`) on large projects are not independently verified -- a project with more models than the API's default page size may come back truncated with no error. Smoke-test against your own account before relying on this in production. `parse_manifest` (Phase 1, local artifacts) has no such gap.
</Warning>

<Note>
`fetch_cloud_lineage` returns the same `DbtData` shape as `parse_manifest`, so both flows feed the same `export_as_documents` call.
</Note>

### Environment variables

| Variable | Parameter | Default |
|---|---|---|
| `DBT_MANIFEST_PATH` | `manifest_path` | — |
| `DBT_CATALOG_PATH` | `catalog_path` | — |
| `DBT_CLOUD_ACCOUNT_ID` | `account_id` | — |
| `DBT_CLOUD_TOKEN` | `token` | — |
| `DBT_CLOUD_BASE_URL` | `base_url` | `https://metadata.cloud.getdbt.com` |


## Reusing a Connector

```python
from semantica.ingest import DbtConnector, DbtIngestor

connector = DbtConnector(manifest_path="target/manifest.json", catalog_path="target/catalog.json")
ingestor = DbtIngestor(connector=connector)
data = ingestor.parse_manifest()
ingestor.close()
```


## See Also

- [Ingest Module](../reference/ingest) — Full `DbtIngestor` API and all other ingestors.
- [Snowflake Integration](/integrations/snowflake) — Relational warehouse connector, the source dbt models usually materialize into.
- [Installation](../installation) — All optional dependency extras.
- [Knowledge Graph](../reference/kg) — Build a KG from ingested dbt lineage.
