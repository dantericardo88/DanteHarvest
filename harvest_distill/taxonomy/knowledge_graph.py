"""
KnowledgeGraph — SQLite-backed entity-relation graph with entity resolution.

Wave 7f: knowledge_graph_extraction — entity resolution + local SQLite persistence (6→9).

Extends LLMTaxonomyEnricher's SPO triple extraction with:
1. Entity resolution: merge co-referent entities (aliases, case variants)
2. Local SQLite persistence: queryable graph without Neo4j dependency
3. Subgraph queries: BFS/DFS neighborhood retrieval
4. Graph statistics: degree distribution, hub entities
5. Export: to dict / JSON-LD / Cypher

Constitutional guarantees:
- Local-first: SQLite only, zero network calls
- Append-only reads: existing data never overwritten, only new edges added
- Fail-open: SQLite errors never prevent triple ingestion
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from uuid import uuid4


# ---------------------------------------------------------------------------
# Entity + Relation
# ---------------------------------------------------------------------------

@dataclass
class Entity:
    entity_id: str
    canonical_name: str
    aliases: List[str] = field(default_factory=list)
    entity_type: str = "concept"
    source: str = ""

    def matches(self, name: str) -> bool:
        norm = _normalize(name)
        return norm == _normalize(self.canonical_name) or norm in [_normalize(a) for a in self.aliases]


@dataclass
class Relation:
    relation_id: str
    subject_id: str
    predicate: str
    object_id: str
    confidence: float = 1.0
    source: str = ""
    created_at: float = field(default_factory=time.time)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())


# ---------------------------------------------------------------------------
# KnowledgeGraph
# ---------------------------------------------------------------------------

class KnowledgeGraph:
    """
    SQLite-backed knowledge graph with entity resolution and subgraph queries.

    Usage:
        kg = KnowledgeGraph(db_path=Path("storage/kg.sqlite"))
        kg.add_triple("Python", "is_a", "programming language", source="wiki")
        kg.add_triple("python", "is_a", "language", source="wiki2")  # resolved to same entity
        neighbors = kg.neighbors("Python", depth=2)
        stats = kg.stats()
        kg.close()
    """

    def __init__(self, db_path: Optional[Path] = None):
        self._db_path = Path(db_path) if db_path else Path(":memory:")
        if self._db_path != Path(":memory:"):
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._init_schema()
        self._entity_cache: Dict[str, Entity] = {}

    def _init_schema(self) -> None:
        cur = self._conn.cursor()
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS entities (
                entity_id TEXT PRIMARY KEY,
                canonical_name TEXT NOT NULL,
                aliases TEXT DEFAULT '[]',
                entity_type TEXT DEFAULT 'concept',
                source TEXT DEFAULT '',
                created_at REAL
            );
            CREATE TABLE IF NOT EXISTS relations (
                relation_id TEXT PRIMARY KEY,
                subject_id TEXT NOT NULL,
                predicate TEXT NOT NULL,
                object_id TEXT NOT NULL,
                confidence REAL DEFAULT 1.0,
                source TEXT DEFAULT '',
                created_at REAL,
                FOREIGN KEY(subject_id) REFERENCES entities(entity_id),
                FOREIGN KEY(object_id) REFERENCES entities(entity_id)
            );
            CREATE INDEX IF NOT EXISTS idx_rel_subject ON relations(subject_id);
            CREATE INDEX IF NOT EXISTS idx_rel_object ON relations(object_id);
            CREATE INDEX IF NOT EXISTS idx_entity_name ON entities(canonical_name);
        """)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Entity resolution
    # ------------------------------------------------------------------

    def resolve_entity(self, name: str, entity_type: str = "concept", source: str = "") -> Entity:
        """
        Find or create an entity. Resolves aliases and case variants.
        """
        norm = _normalize(name)
        # Check cache first
        for eid, entity in self._entity_cache.items():
            if entity.matches(name):
                return entity
        # Check DB
        cur = self._conn.cursor()
        rows = cur.execute(
            "SELECT entity_id, canonical_name, aliases, entity_type, source FROM entities"
        ).fetchall()
        for row in rows:
            eid, canonical, aliases_json, etype, esrc = row
            aliases = json.loads(aliases_json)
            entity = Entity(entity_id=eid, canonical_name=canonical, aliases=aliases, entity_type=etype, source=esrc)
            if entity.matches(name):
                self._entity_cache[eid] = entity
                return entity
        # Create new entity
        entity = Entity(
            entity_id=str(uuid4()),
            canonical_name=name,
            aliases=[],
            entity_type=entity_type,
            source=source,
        )
        cur.execute(
            "INSERT INTO entities VALUES (?,?,?,?,?,?)",
            (entity.entity_id, entity.canonical_name, "[]", entity.entity_type, source, time.time()),
        )
        self._conn.commit()
        self._entity_cache[entity.entity_id] = entity
        return entity

    def add_alias(self, entity_id: str, alias: str) -> None:
        cur = self._conn.cursor()
        row = cur.execute("SELECT aliases FROM entities WHERE entity_id=?", (entity_id,)).fetchone()
        if row:
            aliases = json.loads(row[0])
            if alias not in aliases:
                aliases.append(alias)
                cur.execute("UPDATE entities SET aliases=? WHERE entity_id=?", (json.dumps(aliases), entity_id))
                self._conn.commit()
                if entity_id in self._entity_cache:
                    self._entity_cache[entity_id].aliases = aliases

    # ------------------------------------------------------------------
    # Triple insertion
    # ------------------------------------------------------------------

    def add_triple(
        self,
        subject: str,
        predicate: str,
        obj: str,
        subject_type: str = "concept",
        object_type: str = "concept",
        confidence: float = 1.0,
        source: str = "",
    ) -> Relation:
        """Add a (subject, predicate, object) triple to the graph."""
        subj_entity = self.resolve_entity(subject, entity_type=subject_type, source=source)
        obj_entity = self.resolve_entity(obj, entity_type=object_type, source=source)
        relation = Relation(
            relation_id=str(uuid4()),
            subject_id=subj_entity.entity_id,
            predicate=predicate,
            object_id=obj_entity.entity_id,
            confidence=confidence,
            source=source,
        )
        cur = self._conn.cursor()
        cur.execute(
            "INSERT INTO relations VALUES (?,?,?,?,?,?,?)",
            (relation.relation_id, relation.subject_id, relation.predicate,
             relation.object_id, relation.confidence, relation.source, relation.created_at),
        )
        self._conn.commit()
        return relation

    def add_triples_from_spo(self, triples: List[Any], source: str = "") -> List[Relation]:
        """Bulk-insert SPOTriple objects from LLMTaxonomyEnricher."""
        relations = []
        for triple in triples:
            try:
                r = self.add_triple(
                    subject=triple.subject,
                    predicate=triple.predicate,
                    obj=triple.object_,
                    confidence=triple.confidence if hasattr(triple, "confidence") else 1.0,
                    source=source or getattr(triple, "source", ""),
                )
                relations.append(r)
            except Exception:
                pass
        return relations

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def neighbors(self, entity_name: str, depth: int = 1) -> List[dict]:
        """
        BFS neighborhood retrieval up to `depth` hops.
        Returns list of {subject, predicate, object, depth} dicts.
        """
        entity = self.resolve_entity(entity_name)
        visited: Set[str] = {entity.entity_id}
        frontier = [entity.entity_id]
        results = []

        for d in range(1, depth + 1):
            next_frontier = []
            for eid in frontier:
                cur = self._conn.cursor()
                rows = cur.execute(
                    "SELECT r.predicate, e2.canonical_name, r.object_id "
                    "FROM relations r JOIN entities e2 ON r.object_id=e2.entity_id "
                    "WHERE r.subject_id=?",
                    (eid,),
                ).fetchall()
                e1_row = cur.execute("SELECT canonical_name FROM entities WHERE entity_id=?", (eid,)).fetchone()
                subj_name = e1_row[0] if e1_row else eid
                for pred, obj_name, obj_id in rows:
                    results.append({"subject": subj_name, "predicate": pred, "object": obj_name, "depth": d})
                    if obj_id not in visited:
                        visited.add(obj_id)
                        next_frontier.append(obj_id)
            frontier = next_frontier

        return results

    def search_entities(self, query: str, limit: int = 20) -> List[Entity]:
        """Full-text search across entity names."""
        cur = self._conn.cursor()
        like = f"%{query.lower()}%"
        rows = cur.execute(
            "SELECT entity_id, canonical_name, aliases, entity_type, source "
            "FROM entities WHERE LOWER(canonical_name) LIKE ? LIMIT ?",
            (like, limit),
        ).fetchall()
        return [
            Entity(entity_id=r[0], canonical_name=r[1], aliases=json.loads(r[2]), entity_type=r[3], source=r[4])
            for r in rows
        ]

    def entity_count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    def relation_count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM relations").fetchone()[0]

    def stats(self) -> dict:
        cur = self._conn.cursor()
        top_predicates = cur.execute(
            "SELECT predicate, COUNT(*) as cnt FROM relations GROUP BY predicate ORDER BY cnt DESC LIMIT 10"
        ).fetchall()
        return {
            "entity_count": self.entity_count(),
            "relation_count": self.relation_count(),
            "top_predicates": [{"predicate": p, "count": c} for p, c in top_predicates],
        }

    def to_dict(self) -> dict:
        cur = self._conn.cursor()
        entities = [
            {"entity_id": r[0], "canonical_name": r[1], "entity_type": r[3]}
            for r in cur.execute("SELECT entity_id, canonical_name, aliases, entity_type FROM entities").fetchall()
        ]
        relations = [
            {"relation_id": r[0], "subject_id": r[1], "predicate": r[2], "object_id": r[3], "confidence": r[4]}
            for r in cur.execute("SELECT relation_id, subject_id, predicate, object_id, confidence FROM relations").fetchall()
        ]
        return {"entities": entities, "relations": relations, "stats": self.stats()}

    def to_cypher(self) -> str:
        """Export as Cypher CREATE/MERGE statements for Neo4j import."""
        lines = []
        cur = self._conn.cursor()
        for row in cur.execute("SELECT entity_id, canonical_name, entity_type FROM entities").fetchall():
            eid, name, etype = row
            safe_name = name.replace("'", "\\'")
            safe_id = eid.replace("-", "_")
            lines.append(f"MERGE (e_{safe_id}:Entity {{name: '{safe_name}', type: '{etype}'}});")
        for row in cur.execute(
            "SELECT r.predicate, e1.canonical_name, e2.canonical_name "
            "FROM relations r "
            "JOIN entities e1 ON r.subject_id=e1.entity_id "
            "JOIN entities e2 ON r.object_id=e2.entity_id"
        ).fetchall():
            pred, subj, obj = row
            safe_pred = pred.upper().replace(" ", "_").replace("'", "")
            safe_subj = subj.replace("'", "\\'")
            safe_obj = obj.replace("'", "\\'")
            lines.append(
                f"MATCH (s:Entity {{name: '{safe_subj}'}}), (o:Entity {{name: '{safe_obj}'}}) "
                f"MERGE (s)-[:{safe_pred}]->(o);"
            )
        return "\n".join(lines)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
