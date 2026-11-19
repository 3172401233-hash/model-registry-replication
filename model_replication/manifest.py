"""Version manifest model.

A manifest binds a version identifier to the complete set of objects
(one params file plus one or more weight shards) that make up a readable
model version. Each object is content-addressed by its SHA256 digest.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

KIND_PARAMS = "params"
KIND_SHARD = "shard"


@dataclass(frozen=True)
class ObjectEntry:
    """A single object referenced by a manifest."""

    name: str
    sha256: str
    size: int
    kind: str = KIND_SHARD

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "sha256": self.sha256,
            "size": self.size,
            "kind": self.kind,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ObjectEntry":
        return ObjectEntry(
            name=str(data["name"]),
            sha256=str(data["sha256"]),
            size=int(data["size"]),
            kind=str(data.get("kind", KIND_SHARD)),
        )


@dataclass(frozen=True)
class Manifest:
    """Immutable description of a complete model version."""

    version: str
    objects: Tuple[ObjectEntry, ...] = field(default_factory=tuple)

    @property
    def total_size(self) -> int:
        return sum(obj.size for obj in self.objects)

    @property
    def params(self) -> Tuple[ObjectEntry, ...]:
        return tuple(obj for obj in self.objects if obj.kind == KIND_PARAMS)

    @property
    def shards(self) -> Tuple[ObjectEntry, ...]:
        return tuple(obj for obj in self.objects if obj.kind == KIND_SHARD)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "objects": [obj.to_dict() for obj in self.objects],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Manifest":
        objects = [ObjectEntry.from_dict(item) for item in data["objects"]]
        return Manifest(version=str(data["version"]), objects=tuple(objects))

    @staticmethod
    def from_json(text: str) -> "Manifest":
        return Manifest.from_dict(json.loads(text))

    def validate(self) -> List[str]:
        """Return a list of structural problems (empty means consistent)."""
        problems = []
        if not self.version:
            problems.append("manifest has an empty version")
        if not self.objects:
            problems.append("manifest lists no objects")
        if not self.params:
            problems.append("manifest has no params object")
        if not self.shards:
            problems.append("manifest has no weight shard objects")
        seen_names = set()
        seen_digests = set()
        for obj in self.objects:
            if obj.name in seen_names:
                problems.append("duplicate object name: %s" % obj.name)
            seen_names.add(obj.name)
            if obj.sha256 in seen_digests:
                problems.append("duplicate object digest: %s" % obj.sha256)
            seen_digests.add(obj.sha256)
            if obj.size < 0:
                problems.append("negative size for object: %s" % obj.name)
            if len(obj.sha256) != 64:
                problems.append("invalid sha256 for object: %s" % obj.name)
        return problems
