"""Tests for the dbt ingestor.

Phase 1 (local ``manifest.json``/``catalog.json``) never touches the
filesystem outside ``tmp_path`` fixtures. Phase 2 (dbt Cloud Metadata API)
never touches a live dbt Cloud account: every outbound request goes through
``semantica.ingest.ssrf.request_with_ssrf_guard`` (see ``dbt_ingestor.py``),
so those tests patch that single entry point.

Covered:
- Local artifact parsing: models/sources/seeds/snapshots kept, tests/macros
  dropped, lineage resolved from ``parent_map``
- Column merge: manifest descriptions + catalog types, by column name
- ``export_as_documents`` yields entity docs (id/name/type) *and*
  relationship docs (source/target) that ``GraphBuilder`` accepts directly
- Connector validation (requires local path or Cloud credentials)
- dbt Cloud Metadata API: SSRF-guarded GraphQL request, auth header, error
  propagation
- Public import through the lazy export in ``semantica.ingest``
"""

import json

import pytest
import requests
from unittest.mock import MagicMock, patch

from semantica.ingest import DbtConnector, DbtData, DbtIngestor, DbtNode
from semantica.kg import GraphBuilder
from semantica.utils.exceptions import ProcessingError, ValidationError

MANIFEST = {
    "metadata": {
        "project_name": "jaffle_shop",
        "dbt_version": "1.8.0",
        "generated_at": "2026-09-01T00:00:00Z",
    },
    "nodes": {
        "model.jaffle_shop.stg_customers": {
            "resource_type": "model",
            "name": "stg_customers",
            "database": "analytics",
            "schema": "staging",
            "alias": "stg_customers",
            "description": "Cleaned customer records.",
            "config": {"materialized": "view"},
            "tags": ["staging"],
            "package_name": "jaffle_shop",
            "path": "staging/stg_customers.sql",
            "columns": {
                "customer_id": {"description": "Unique customer id."},
            },
        },
        "model.jaffle_shop.stg_orders": {
            "resource_type": "model",
            "name": "stg_orders",
            "database": "analytics",
            "schema": "staging",
            "alias": "stg_orders",
            "description": "Cleaned order records.",
            "config": {"materialized": "view"},
            "tags": ["staging"],
            "package_name": "jaffle_shop",
            "path": "staging/stg_orders.sql",
            "columns": {},
        },
        "model.jaffle_shop.customer_orders": {
            "resource_type": "model",
            "name": "customer_orders",
            "database": "analytics",
            "schema": "marts",
            "alias": "customer_orders",
            "description": "One row per customer order.",
            "config": {"materialized": "table"},
            "tags": ["marts"],
            "package_name": "jaffle_shop",
            "path": "marts/customer_orders.sql",
            "columns": {},
        },
        "seed.jaffle_shop.country_codes": {
            "resource_type": "seed",
            "name": "country_codes",
            "database": "analytics",
            "schema": "seeds",
            "alias": "country_codes",
            "description": "ISO country codes.",
            "config": {},
            "tags": [],
            "package_name": "jaffle_shop",
            "path": "seeds/country_codes.csv",
            "columns": {},
        },
        "test.jaffle_shop.not_null_stg_customers_customer_id": {
            "resource_type": "test",
            "name": "not_null_stg_customers_customer_id",
            "columns": {},
        },
    },
    "sources": {
        "source.jaffle_shop.raw.customers": {
            "name": "customers",
            "source_name": "raw",
            "database": "raw_db",
            "schema": "raw",
            "identifier": "customers",
            "description": "Raw customer export.",
            "package_name": "jaffle_shop",
            "columns": {
                "customer_id": {"description": "Raw customer id."},
            },
        },
        "source.jaffle_shop.raw.orders": {
            "name": "orders",
            "source_name": "raw",
            "database": "raw_db",
            "schema": "raw",
            "identifier": "orders",
            "description": "Raw order export.",
            "package_name": "jaffle_shop",
            "columns": {},
        },
    },
    "parent_map": {
        "model.jaffle_shop.stg_customers": ["source.jaffle_shop.raw.customers"],
        "model.jaffle_shop.stg_orders": ["source.jaffle_shop.raw.orders"],
        "model.jaffle_shop.customer_orders": [
            "model.jaffle_shop.stg_customers",
            "model.jaffle_shop.stg_orders",
        ],
        # A test node's dependency: dropped nodes must not leak lineage edges.
        "test.jaffle_shop.not_null_stg_customers_customer_id": [
            "model.jaffle_shop.stg_customers"
        ],
    },
}

CATALOG = {
    "nodes": {
        "model.jaffle_shop.stg_customers": {
            "columns": {"customer_id": {"type": "INTEGER"}},
        },
        "model.jaffle_shop.customer_orders": {
            "columns": {"order_id": {"type": "INTEGER"}},
        },
    },
    "sources": {
        "source.jaffle_shop.raw.customers": {
            "columns": {"customer_id": {"type": "VARCHAR"}},
        },
    },
}


@pytest.fixture()
def artifacts(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    catalog_path = tmp_path / "catalog.json"
    manifest_path.write_text(json.dumps(MANIFEST), encoding="utf-8")
    catalog_path.write_text(json.dumps(CATALOG), encoding="utf-8")
    return str(manifest_path), str(catalog_path)


class TestLocalArtifactParsing:
    def test_models_sources_and_seeds_are_kept(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()

        by_id = {n.unique_id: n for n in data.nodes}
        assert "model.jaffle_shop.stg_customers" in by_id
        assert "model.jaffle_shop.customer_orders" in by_id
        assert "seed.jaffle_shop.country_codes" in by_id
        assert "source.jaffle_shop.raw.customers" in by_id
        assert by_id["seed.jaffle_shop.country_codes"].resource_type == "seed"
        assert by_id["source.jaffle_shop.raw.customers"].resource_type == "source"

    def test_tests_and_macros_are_dropped(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()

        ids = {n.unique_id for n in data.nodes}
        assert "test.jaffle_shop.not_null_stg_customers_customer_id" not in ids

    def test_lineage_resolved_from_parent_map(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()

        assert (
            "source.jaffle_shop.raw.customers",
            "model.jaffle_shop.stg_customers",
        ) in data.lineage
        assert (
            "model.jaffle_shop.stg_customers",
            "model.jaffle_shop.customer_orders",
        ) in data.lineage
        assert (
            "model.jaffle_shop.stg_orders",
            "model.jaffle_shop.customer_orders",
        ) in data.lineage

    def test_lineage_edge_to_dropped_node_is_not_created(self, artifacts):
        """A test node's parent_map entry must not leak in as a lineage edge."""
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()

        target_ids = {child for _, child in data.lineage}
        assert "test.jaffle_shop.not_null_stg_customers_customer_id" not in target_ids

    def test_project_metadata_captured(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()

        assert data.project_name == "jaffle_shop"
        assert data.dbt_version == "1.8.0"
        assert data.source == "local"

    def test_catalog_is_optional(self, artifacts):
        manifest_path, _ = artifacts
        ing = DbtIngestor(manifest_path=manifest_path)
        data = ing.parse_manifest()

        by_id = {n.unique_id: n for n in data.nodes}
        col = by_id["model.jaffle_shop.stg_customers"].columns["customer_id"]
        assert col["description"] == "Unique customer id."
        assert col["data_type"] is None

    def test_missing_manifest_file_raises_validation_error(self, tmp_path):
        ing = DbtIngestor(manifest_path=str(tmp_path / "does-not-exist.json"))
        with pytest.raises(ValidationError, match="not found"):
            ing.parse_manifest()

    def test_invalid_manifest_json_raises_processing_error(self, tmp_path):
        bad = tmp_path / "manifest.json"
        bad.write_text("{not valid json", encoding="utf-8")
        ing = DbtIngestor(manifest_path=str(bad))
        with pytest.raises(ProcessingError, match="not valid JSON"):
            ing.parse_manifest()

    def test_manifest_missing_nodes_key_raises_processing_error(self, tmp_path):
        bad = tmp_path / "manifest.json"
        bad.write_text(json.dumps({"metadata": {}}), encoding="utf-8")
        ing = DbtIngestor(manifest_path=str(bad))
        with pytest.raises(ProcessingError, match="'nodes'"):
            ing.parse_manifest()


class TestColumnMerge:
    def test_manifest_description_and_catalog_type_merged(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()

        by_id = {n.unique_id: n for n in data.nodes}
        col = by_id["model.jaffle_shop.stg_customers"].columns["customer_id"]
        assert col["description"] == "Unique customer id."
        assert col["data_type"] == "INTEGER"

    def test_catalog_only_column_gets_empty_description(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()

        by_id = {n.unique_id: n for n in data.nodes}
        col = by_id["model.jaffle_shop.customer_orders"].columns["order_id"]
        assert col["description"] == ""
        assert col["data_type"] == "INTEGER"


class TestDocumentExport:
    def test_nodes_become_entity_documents(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()
        docs = ing.export_as_documents(data)

        entity_docs = [d for d in docs if "id" in d]
        by_id = {d["id"]: d for d in entity_docs}
        node = by_id["model.jaffle_shop.stg_customers"]
        assert node["name"] == "stg_customers"
        assert node["type"] == "dbt_model"
        assert node["materialized"] == "view"
        assert node["schema"] == "staging"

    def test_lineage_becomes_relationship_documents(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()
        docs = ing.export_as_documents(data)

        rel_docs = [d for d in docs if "source" in d and "target" in d]
        assert {
            "source": "source.jaffle_shop.raw.customers",
            "target": "model.jaffle_shop.stg_customers",
            "type": "feeds",
            "metadata": {"source": "dbt", "relationship_type": "dbt_lineage"},
        } in rel_docs

    def test_document_count_matches_nodes_plus_lineage(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()
        docs = ing.export_as_documents(data)

        assert len(docs) == len(data.nodes) + len(data.lineage)


class TestConnectorValidation:
    def test_requires_manifest_path_or_cloud_credentials(self):
        with pytest.raises(ValidationError):
            DbtConnector()

    def test_local_path_alone_is_sufficient(self, artifacts):
        manifest_path, _ = artifacts
        assert DbtConnector(manifest_path=manifest_path) is not None

    def test_cloud_credentials_alone_are_sufficient(self):
        assert DbtConnector(account_id="123", token="tok") is not None


class TestDbtCloudMetadataAPI:
    def _cloud_payload(self):
        return {
            "job": {
                "models": [
                    {
                        "uniqueId": "model.jaffle_shop.stg_customers",
                        "name": "stg_customers",
                        "database": "analytics",
                        "schema": "staging",
                        "alias": "stg_customers",
                        "materializedType": "view",
                        "tags": ["staging"],
                        "description": "Cleaned customer records.",
                        "parentsModels": [],
                        "parentsSources": [
                            {"uniqueId": "source.jaffle_shop.raw.customers"}
                        ],
                        "columns": [
                            {
                                "name": "customer_id",
                                "description": "Unique customer id.",
                                "type": "INTEGER",
                            }
                        ],
                    }
                ],
                "sources": [
                    {
                        "uniqueId": "source.jaffle_shop.raw.customers",
                        "name": "customers",
                        "sourceName": "raw",
                        "database": "raw_db",
                        "schema": "raw",
                        "identifier": "customers",
                        "description": "Raw customer export.",
                        "columns": [],
                    }
                ],
            }
        }

    def test_query_goes_through_ssrf_guard_with_bearer_auth(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"data": self._cloud_payload()}

        with patch(
            "semantica.ingest.dbt_ingestor.request_with_ssrf_guard",
            return_value=resp,
        ) as guard:
            ing = DbtIngestor(account_id="123", token="dbtc_secret")
            data = ing.fetch_cloud_lineage(job_id=987)

        assert guard.call_count == 1
        method, url = guard.call_args[0]
        assert method == "POST"
        assert url == "https://metadata.cloud.getdbt.com/graphql"
        headers = guard.call_args.kwargs.get("headers", {})
        assert headers["Authorization"] == "Bearer dbtc_secret"
        assert data.source == "dbt_cloud"

    def test_cloud_lineage_edges_resolved(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"data": self._cloud_payload()}

        with patch(
            "semantica.ingest.dbt_ingestor.request_with_ssrf_guard",
            return_value=resp,
        ):
            ing = DbtIngestor(account_id="123", token="dbtc_secret")
            data = ing.fetch_cloud_lineage(job_id=987)

        assert (
            "source.jaffle_shop.raw.customers",
            "model.jaffle_shop.stg_customers",
        ) in data.lineage

    def test_graphql_errors_raise_processing_error(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"errors": [{"message": "job not found"}]}

        with patch(
            "semantica.ingest.dbt_ingestor.request_with_ssrf_guard",
            return_value=resp,
        ):
            ing = DbtIngestor(account_id="123", token="dbtc_secret")
            with pytest.raises(ProcessingError, match="job not found"):
                ing.fetch_cloud_lineage(job_id=987)

    def test_http_error_raises_processing_error(self):
        resp = MagicMock()
        resp.status_code = 401
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError("401")

        with patch(
            "semantica.ingest.dbt_ingestor.request_with_ssrf_guard",
            return_value=resp,
        ):
            ing = DbtIngestor(account_id="123", token="bad-token")
            with pytest.raises(ProcessingError, match="request failed"):
                ing.fetch_cloud_lineage(job_id=987)

    def test_custom_base_url_is_respected(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"data": {"job": {"models": [], "sources": []}}}

        with patch(
            "semantica.ingest.dbt_ingestor.request_with_ssrf_guard",
            return_value=resp,
        ) as guard:
            connector = DbtConnector(
                account_id="123",
                token="tok",
                base_url="https://acme123.metadata.cloud.getdbt.com",
            )
            ing = DbtIngestor(connector=connector)
            ing.fetch_cloud_lineage(job_id=1)

        _, url = guard.call_args[0]
        assert url == "https://acme123.metadata.cloud.getdbt.com/graphql"


class TestNodeDocument:
    def test_to_documents_includes_source_marker(self):
        node = DbtNode(
            unique_id="model.x.y",
            resource_type="model",
            name="y",
        )
        data = DbtData(nodes=[node])
        doc = data.to_documents()[0]
        assert doc["source"] == "dbt"
        assert doc["id"] == "model.x.y"
        assert doc["name"] == "y"
        assert doc["type"] == "dbt_model"
        assert doc["metadata"] == {"source": "dbt", "resource_type": "model"}


class TestGraphBuilderIntegration:
    """Empirical proof, not just field-name tracing, that documents survive
    the real ``GraphBuilder.build()`` call -- entities and relationships are
    dicts with 'id'/'name' and 'source'/'target' respectively, which
    ``GraphBuilder._process_item`` special-cases to pass through unchanged
    rather than re-deriving via entity/relation extraction. This also proves
    lineage 'metadata' (not a flat extra key) is what's needed for the
    relationship_type tag to survive -- GraphBuilder's GraphStore persistence
    path (add_edges) reads relationship extras from rel['metadata']
    specifically.
    """

    def test_documents_round_trip_through_graph_builder(self, artifacts):
        manifest_path, catalog_path = artifacts
        ing = DbtIngestor(manifest_path=manifest_path, catalog_path=catalog_path)
        data = ing.parse_manifest()
        docs = ing.export_as_documents(data)

        result = GraphBuilder().build(docs)

        entities_by_id = {e["id"]: e for e in result["entities"]}
        node = entities_by_id["model.jaffle_shop.stg_customers"]
        assert node["database"] == "analytics"
        assert node["materialized"] == "view"
        assert node["metadata"]["source"] == "dbt"

        rels = result["relationships"]
        edge = next(
            r
            for r in rels
            if r["source"] == "source.jaffle_shop.raw.customers"
            and r["target"] == "model.jaffle_shop.stg_customers"
        )
        assert edge["metadata"]["relationship_type"] == "dbt_lineage"


class TestLazyExport:
    def test_public_import_through_lazy_export(self):
        # __getattr__ should resolve the lazy export once the object is created.
        assert callable(DbtConnector)
        assert callable(DbtIngestor)
        assert callable(DbtData)
        assert callable(DbtNode)
