"""dbt ingestion module.

Parses dbt's build artifacts (``manifest.json`` / ``catalog.json``) into
document dicts the pipeline can feed to ``GraphBuilder`` — models, sources,
seeds and snapshots as nodes, plus the model-to-model / model-to-source
lineage graph dbt already computes as relationships.

Why this exists
----------------
dbt is the de facto transformation layer of most modern data stacks, and its
artifacts are one of the richest *existing* sources of column-level lineage
and model dependency graphs available without re-deriving anything: every
``dbt build`` / ``dbt docs generate`` already writes ``parent_map`` (which
model was built from which) and per-column types/descriptions to disk. This
connector reads that instead of guessing lineage from SQL.

Design notes
------------
Four classes, matching the SAP OData ingestor's shape (an artifact/schema
source rather than a row-cursor source):
    - ``DbtNode``: one model/source/seed/snapshot, with the columns merged
      from ``manifest.json`` (descriptions) and ``catalog.json`` (types).
    - ``DbtData``: the parsed project — nodes plus lineage edges — flattened
      to document dicts via ``to_documents()``. Unlike the SAP/Snowflake
      connectors, this emits **both** entity documents *and* explicit
      ``{"source": ..., "target": ...}`` relationship documents, since
      ``GraphBuilder`` accepts relationship dicts directly (see
      ``kg/graph_builder.py``) and dbt's lineage graph is the whole reason
      this connector has value over a flat table dump.
    - ``DbtConnector``: Phase 1 loads local artifact files (plain
      ``json.load``, no network, no optional dependency). Phase 2 (dbt
      Cloud Metadata API) is opt-in via ``account_id`` + ``token``; every
      request goes through ``request_with_ssrf_guard`` exactly like the SAP
      connector's OAuth2 token exchange, since the API host is user
      configurable.
    - ``DbtIngestor``: ``parse_manifest`` (Phase 1), ``fetch_cloud_lineage``
      (Phase 2) and ``export_as_documents``.

Local artifacts vs. dbt Cloud
------------------------------
``manifest.json`` always has the full project graph (models, sources,
seeds, snapshots, ``parent_map``) but only the *declared* column types from
source configs. ``catalog.json`` has the *actual* materialized column types
from ``information_schema`` but no lineage. Loading both and merging on
``unique_id`` (models/seeds/snapshots) gives the fullest picture; catalog is
optional since a manifest-only export still has the lineage graph, just
without confirmed column types.

Nodes outside the requested set (``tests``, ``macros``, ``analyses``,
``exposures``) are dropped — they are not graph entities dbt lineage
consumers care about, and keeping them would need a different schema.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from ..utils.exceptions import ProcessingError, ValidationError
from ..utils.logging import get_logger
from .ssrf import parse_bool, request_with_ssrf_guard

__all__ = [
    "DbtNode",
    "DbtData",
    "DbtConnector",
    "DbtIngestor",
]

_logger = get_logger("dbt_ingestor")

# manifest.json also has "test", "macro", "analysis", "exposure", "metric"
# entries -- these aren't lineage-graph entities, so they're skipped.
_RESOURCE_TYPES = frozenset({"model", "seed", "snapshot"})

_DEFAULT_CLOUD_BASE_URL = "https://metadata.cloud.getdbt.com"

# Verified against docs.getdbt.com's Discovery API reference:
# `job(id: $jobId: BigInt!)` and the `parentsModels`/`parentsSources` field
# names are confirmed correct. NOT independently verified against a live dbt
# Cloud account: the exact field list below (database/schema/alias/
# materializedType/tags/description on `models`/`sources`), and whether
# `models`/`sources` require cursor pagination (`first`/`after`) on large
# projects -- this query requests an unpaginated list, so a project with more
# models than the API's default page size may come back truncated with no
# error. Treat `fetch_cloud_lineage` as needing a live-account smoke test
# before production use; `parse_manifest` (Phase 1) has no such gap.
_CLOUD_LINEAGE_QUERY = """
query DbtSemanticaLineage($jobId: BigInt!) {
  job(id: $jobId) {
    models {
      uniqueId
      name
      database
      schema
      alias
      materializedType
      tags
      description
      parentsModels { uniqueId }
      parentsSources { uniqueId }
      columns { name description type }
    }
    sources {
      uniqueId
      name
      sourceName
      database
      schema
      identifier
      description
      columns { name description type }
    }
  }
}
"""


@dataclass
class DbtNode:
    """One dbt model, source, seed or snapshot.

    ``columns`` maps column name -> ``{"description": str, "data_type":
    Optional[str]}``, merged from manifest (descriptions) and catalog
    (types) when both are available.
    """

    unique_id: str
    resource_type: str
    name: str
    database: Optional[str] = None
    schema: Optional[str] = None
    identifier: Optional[str] = None
    description: str = ""
    materialized: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    package_name: Optional[str] = None
    source_name: Optional[str] = None
    path: Optional[str] = None
    columns: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class DbtData:
    """A parsed dbt project: every node plus its lineage edges.

    ``lineage`` holds ``(parent_unique_id, child_unique_id)`` pairs resolved
    from ``manifest.json``'s ``parent_map`` (or the dbt Cloud equivalent),
    restricted to edges where both endpoints are in ``nodes`` -- an edge to a
    node outside the requested resource types (e.g. a test) is dropped
    rather than left dangling.
    """

    nodes: List[DbtNode]
    lineage: List[Tuple[str, str]] = field(default_factory=list)
    project_name: Optional[str] = None
    dbt_version: Optional[str] = None
    generated_at: Optional[str] = None
    source: str = "local"
    ingested_at: datetime = field(default_factory=datetime.now)

    def to_documents(self) -> List[Dict[str, Any]]:
        """Flatten nodes and lineage edges into ``GraphBuilder``-ready dicts.

        Each node becomes an entity document (``id``/``name``/``type``).
        Each lineage edge becomes a relationship document with
        ``source``/``target``, which ``GraphBuilder`` consumes directly
        without needing entity extraction to re-derive the same edges dbt
        already computed.

        Follows the same ``metadata`` convention as
        ``RelationalSchemaMapper`` (see ``kg/schema_mapper.py``): descriptive
        fields stay flat at the top level, while a ``metadata`` dict carries
        provenance (``source``, plus a table-like discriminator). This
        matters beyond style -- ``GraphBuilder``'s ``GraphStore`` persistence
        path (``add_edges``) reads relationship extras from ``rel["metadata"]``
        specifically; a flat extra key here would silently never reach a
        persisted edge's properties.
        """
        docs: List[Dict[str, Any]] = [self._node_document(node) for node in self.nodes]
        for parent_id, child_id in self.lineage:
            docs.append(
                {
                    "source": parent_id,
                    "target": child_id,
                    "type": "feeds",
                    "metadata": {"source": "dbt", "relationship_type": "dbt_lineage"},
                }
            )
        return docs

    @staticmethod
    def _node_document(node: DbtNode) -> Dict[str, Any]:
        return {
            "id": node.unique_id,
            "name": node.name,
            "type": f"dbt_{node.resource_type}",
            "text": node.description or node.name,
            "database": node.database,
            "schema": node.schema,
            "identifier": node.identifier,
            "materialized": node.materialized,
            "tags": list(node.tags),
            "package": node.package_name,
            "source_name": node.source_name,
            "path": node.path,
            "columns": node.columns,
            "source": "dbt",
            "metadata": {"source": "dbt", "resource_type": node.resource_type},
        }


class DbtConnector:
    """Loads dbt artifacts, either from local files or the dbt Cloud API.

    Example usage::

        >>> # Phase 1: local artifacts, no network, no optional dependency.
        >>> connector = DbtConnector(
        ...     manifest_path="target/manifest.json",
        ...     catalog_path="target/catalog.json",
        ... )
        >>> manifest = connector.load_manifest()

        >>> # Phase 2: dbt Cloud Metadata API.
        >>> connector = DbtConnector(account_id="12345", token="dbtc_...")
        >>> data = connector.query_cloud_metadata(query, {"jobId": 987})
    """

    def __init__(
        self,
        manifest_path: Optional[str] = None,
        catalog_path: Optional[str] = None,
        *,
        account_id: Optional[str] = None,
        token: Optional[str] = None,
        base_url: Optional[str] = None,
        allow_private_ips: bool = False,
        **config: Any,
    ) -> None:
        """Initialize the dbt connector.

        Args:
            manifest_path: Path to a local ``manifest.json``. Required for
                the Phase 1 (local artifact) flow.
            catalog_path: Path to a local ``catalog.json``. Optional even in
                the local flow -- without it, lineage is still available,
                just without confirmed column types from
                ``information_schema``.
            account_id: dbt Cloud account id. Required for the Phase 2
                (dbt Cloud Metadata API) flow.
            token: dbt Cloud service token (``Authorization: Bearer`` on the
                Metadata API). Required for the Phase 2 flow.
            base_url: dbt Cloud Metadata API host. Defaults to
                ``https://metadata.cloud.getdbt.com``; multi-cell accounts
                use a per-account host instead.
            allow_private_ips: Opt into private/loopback/link-local dbt
                Cloud endpoints. Defaults to False (SSRF-safe).
            **config: Extra options, notably ``timeout``.
        """
        self.logger = _logger
        self.manifest_path = manifest_path or os.getenv("DBT_MANIFEST_PATH")
        self.catalog_path = catalog_path or os.getenv("DBT_CATALOG_PATH")
        self.account_id = account_id or os.getenv("DBT_CLOUD_ACCOUNT_ID")
        self.token = token or os.getenv("DBT_CLOUD_TOKEN")
        self.base_url = (
            base_url or os.getenv("DBT_CLOUD_BASE_URL") or _DEFAULT_CLOUD_BASE_URL
        )
        self.allow_private_ips = parse_bool(
            config.pop("allow_private_ips", allow_private_ips), default=False
        )
        self.config = config

        if not self.manifest_path and not (self.account_id and self.token):
            raise ValidationError(
                "dbt connector requires either 'manifest_path' (local "
                "artifacts) or 'account_id' + 'token' (dbt Cloud Metadata "
                "API)."
            )

        self.session = requests.Session()

    def load_manifest(self, path: Optional[str] = None) -> Dict[str, Any]:
        """Read and parse a local ``manifest.json``."""
        target = path or self.manifest_path
        if not target:
            raise ValidationError("No 'manifest_path' configured.")
        return self._load_json(target, label="manifest")

    def load_catalog(self, path: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Read and parse a local ``catalog.json``, or None if not configured."""
        target = path or self.catalog_path
        if not target:
            return None
        return self._load_json(target, label="catalog")

    @staticmethod
    def _load_json(path: str, *, label: str) -> Dict[str, Any]:
        file_path = Path(path)
        if not file_path.is_file():
            raise ValidationError(f"dbt {label} file not found: {path}")
        try:
            with file_path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except OSError as exc:
            raise ProcessingError(f"Failed to read dbt {label} file '{path}': {exc}") from exc
        except ValueError as exc:
            raise ProcessingError(f"dbt {label} file '{path}' is not valid JSON: {exc}") from exc

    def query_cloud_metadata(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """POST a GraphQL query to the dbt Cloud Metadata API (SSRF-guarded).

        Args:
            query: GraphQL query string.
            variables: GraphQL variables, e.g. ``{"jobId": 987}``.

        Returns:
            The ``data`` object of the GraphQL response.
        """
        if not (self.account_id and self.token):
            raise ValidationError(
                "dbt Cloud Metadata API requires 'account_id' and 'token'."
            )
        url = f"{self.base_url.rstrip('/')}/graphql"
        resp = request_with_ssrf_guard(
            "POST",
            url,
            session=self.session,
            allow_private_ips=self.allow_private_ips,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables or {}},
            timeout=self.config.get("timeout", 30),
        )
        try:
            resp.raise_for_status()
        except requests.exceptions.RequestException as exc:
            raise ProcessingError(f"dbt Cloud Metadata API request failed: {exc}") from exc
        try:
            payload = resp.json()
        except ValueError as exc:
            raise ProcessingError("dbt Cloud Metadata API did not return JSON.") from exc

        if payload.get("errors"):
            raise ProcessingError(
                f"dbt Cloud Metadata API returned errors: {payload['errors']}"
            )
        return payload.get("data") or {}

    def close(self) -> None:
        """Close the underlying :mod:`requests` session."""
        self.session.close()


class DbtIngestor:
    """Parse dbt artifacts into lineage-aware documents for ``GraphBuilder``.

    Example usage::

        >>> from semantica.ingest import DbtIngestor
        >>> ing = DbtIngestor(
        ...     manifest_path="target/manifest.json",
        ...     catalog_path="target/catalog.json",
        ... )
        >>> data = ing.parse_manifest()
        >>> docs = ing.export_as_documents(data)
        >>> from semantica.kg import GraphBuilder
        >>> kg = GraphBuilder().build(docs)
    """

    def __init__(
        self,
        manifest_path: Optional[str] = None,
        catalog_path: Optional[str] = None,
        connector: Optional[DbtConnector] = None,
        **config: Any,
    ) -> None:
        """Initialize the dbt ingestor.

        Args:
            manifest_path: Local ``manifest.json`` path. Ignored if
                ``connector`` is given.
            catalog_path: Local ``catalog.json`` path. Ignored if
                ``connector`` is given.
            connector: An existing :class:`DbtConnector`. When provided, its
                configuration is reused as-is.
            **config: Passed to :class:`DbtConnector` when one is created
                (e.g. ``account_id``/``token`` for the dbt Cloud flow).
        """
        self.logger = _logger
        self.connector = connector or DbtConnector(
            manifest_path=manifest_path, catalog_path=catalog_path, **config
        )

    def parse_manifest(
        self,
        manifest_path: Optional[str] = None,
        catalog_path: Optional[str] = None,
    ) -> DbtData:
        """Parse local ``manifest.json`` (+ optional ``catalog.json``) artifacts.

        Args:
            manifest_path: Overrides the connector's configured path.
            catalog_path: Overrides the connector's configured path. Column
                types are left unset (but descriptions still populated from
                the manifest) if no catalog is available.

        Returns:
            A :class:`DbtData` with every model/seed/snapshot/source node
            and the lineage edges resolved from ``parent_map``.
        """
        manifest = self.connector.load_manifest(manifest_path)
        catalog = self.connector.load_catalog(catalog_path)
        return self._build_from_manifest(manifest, catalog)

    def fetch_cloud_lineage(self, job_id: int) -> DbtData:
        """Fetch a project's models/sources/lineage from the dbt Cloud Metadata API.

        Args:
            job_id: The dbt Cloud job id whose most recent run is queried.

        Returns:
            A :class:`DbtData` in the same shape ``parse_manifest`` returns.
        """
        payload = self.connector.query_cloud_metadata(
            _CLOUD_LINEAGE_QUERY, {"jobId": job_id}
        )
        return self._build_from_cloud(payload)

    def export_as_documents(self, data: DbtData) -> List[Dict[str, Any]]:
        """Convert parsed dbt data to document dicts, ready for ``GraphBuilder``."""
        return data.to_documents()

    def close(self) -> None:
        """Close the underlying connector's session."""
        self.connector.close()

    # -- manifest.json / catalog.json parsing --------------------------------

    def _build_from_manifest(
        self, manifest: Dict[str, Any], catalog: Optional[Dict[str, Any]]
    ) -> DbtData:
        if not isinstance(manifest, dict) or "nodes" not in manifest:
            raise ProcessingError(
                "dbt manifest.json is missing the expected 'nodes' mapping."
            )

        catalog_nodes: Dict[str, Any] = (catalog or {}).get("nodes") or {}
        catalog_sources: Dict[str, Any] = (catalog or {}).get("sources") or {}

        nodes: List[DbtNode] = []
        for unique_id, raw in (manifest.get("nodes") or {}).items():
            if (raw or {}).get("resource_type") not in _RESOURCE_TYPES:
                continue
            nodes.append(
                self._node_from_manifest(unique_id, raw, catalog_nodes.get(unique_id))
            )

        for unique_id, raw in (manifest.get("sources") or {}).items():
            nodes.append(
                self._source_from_manifest(unique_id, raw, catalog_sources.get(unique_id))
            )

        known_ids = {node.unique_id for node in nodes}
        lineage: List[Tuple[str, str]] = []
        for child_id, parent_ids in (manifest.get("parent_map") or {}).items():
            if child_id not in known_ids:
                continue
            for parent_id in parent_ids or []:
                if parent_id in known_ids:
                    lineage.append((parent_id, child_id))

        metadata = manifest.get("metadata") or {}
        return DbtData(
            nodes=nodes,
            lineage=lineage,
            project_name=metadata.get("project_name"),
            dbt_version=metadata.get("dbt_version"),
            generated_at=metadata.get("generated_at"),
            source="local",
        )

    def _node_from_manifest(
        self, unique_id: str, raw: Dict[str, Any], catalog_entry: Optional[Dict[str, Any]]
    ) -> DbtNode:
        config = raw.get("config") or {}
        return DbtNode(
            unique_id=unique_id,
            resource_type=raw.get("resource_type") or "model",
            name=raw.get("name") or unique_id,
            database=raw.get("database"),
            schema=raw.get("schema"),
            identifier=raw.get("alias") or raw.get("name"),
            description=raw.get("description") or "",
            materialized=config.get("materialized"),
            tags=list(raw.get("tags") or []),
            package_name=raw.get("package_name"),
            path=raw.get("path"),
            columns=self._merge_columns(
                raw.get("columns") or {}, (catalog_entry or {}).get("columns") or {}
            ),
        )

    def _source_from_manifest(
        self, unique_id: str, raw: Dict[str, Any], catalog_entry: Optional[Dict[str, Any]]
    ) -> DbtNode:
        return DbtNode(
            unique_id=unique_id,
            resource_type="source",
            name=raw.get("name") or unique_id,
            database=raw.get("database"),
            schema=raw.get("schema"),
            identifier=raw.get("identifier") or raw.get("name"),
            description=raw.get("description") or "",
            tags=list(raw.get("tags") or []),
            package_name=raw.get("package_name"),
            source_name=raw.get("source_name"),
            columns=self._merge_columns(
                raw.get("columns") or {}, (catalog_entry or {}).get("columns") or {}
            ),
        )

    @staticmethod
    def _merge_columns(
        manifest_columns: Dict[str, Any], catalog_columns: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """Merge manifest descriptions with catalog types, keyed by column name."""
        merged: Dict[str, Dict[str, Any]] = {}
        for name, col in manifest_columns.items():
            merged[name] = {
                "description": (col or {}).get("description") or "",
                "data_type": (col or {}).get("data_type"),
            }
        for name, col in catalog_columns.items():
            entry = merged.setdefault(name, {"description": "", "data_type": None})
            if not entry.get("data_type"):
                entry["data_type"] = (col or {}).get("type")
        return merged

    # -- dbt Cloud Metadata API parsing ---------------------------------------

    def _build_from_cloud(self, payload: Dict[str, Any]) -> DbtData:
        job = payload.get("job") or {}
        raw_models = job.get("models") or []
        raw_sources = job.get("sources") or []

        nodes: List[DbtNode] = []
        known_ids = set()

        for raw in raw_sources:
            unique_id = raw.get("uniqueId")
            if not unique_id:
                continue
            known_ids.add(unique_id)
            nodes.append(
                DbtNode(
                    unique_id=unique_id,
                    resource_type="source",
                    name=raw.get("name") or unique_id,
                    database=raw.get("database"),
                    schema=raw.get("schema"),
                    identifier=raw.get("identifier") or raw.get("name"),
                    description=raw.get("description") or "",
                    source_name=raw.get("sourceName"),
                    columns=self._cloud_columns(raw.get("columns")),
                )
            )

        for raw in raw_models:
            unique_id = raw.get("uniqueId")
            if unique_id:
                known_ids.add(unique_id)

        lineage: List[Tuple[str, str]] = []
        for raw in raw_models:
            unique_id = raw.get("uniqueId")
            if not unique_id:
                continue
            nodes.append(
                DbtNode(
                    unique_id=unique_id,
                    resource_type="model",
                    name=raw.get("name") or unique_id,
                    database=raw.get("database"),
                    schema=raw.get("schema"),
                    identifier=raw.get("alias") or raw.get("name"),
                    description=raw.get("description") or "",
                    materialized=raw.get("materializedType"),
                    tags=list(raw.get("tags") or []),
                    columns=self._cloud_columns(raw.get("columns")),
                )
            )
            for parent in (raw.get("parentsModels") or []) + (raw.get("parentsSources") or []):
                parent_id = (parent or {}).get("uniqueId")
                if parent_id and parent_id in known_ids:
                    lineage.append((parent_id, unique_id))

        return DbtData(nodes=nodes, lineage=lineage, source="dbt_cloud")

    @staticmethod
    def _cloud_columns(raw_columns: Optional[List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        for col in raw_columns or []:
            name = (col or {}).get("name")
            if not name:
                continue
            result[name] = {
                "description": (col or {}).get("description") or "",
                "data_type": (col or {}).get("type"),
            }
        return result
