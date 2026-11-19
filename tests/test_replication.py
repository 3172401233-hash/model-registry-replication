"""End-to-end tests for the replication library.

Source and target repositories are simulated with temporary directories;
no network access is involved.
"""

import hashlib
import os
import shutil
import tempfile
import unittest

from model_replication import (
    LocalTransport,
    Manifest,
    ManifestInconsistentError,
    ObjectCorruptError,
    ObjectEntry,
    Repository,
    RollbackError,
    SyncEngine,
    TransferError,
)


def make_version(repo, version, shards, params=b'{"lr": 0.001}'):
    """Write objects + manifest for a version into a (source) repository."""
    entries = []
    payloads = [("params.json", "params", params)]
    for index, data in enumerate(shards):
        payloads.append(("shard-%03d.bin" % index, "shard", data))
    for name, kind, data in payloads:
        digest = hashlib.sha256(data).hexdigest()
        with open(repo.object_path(digest), "wb") as handle:
            handle.write(data)
        entries.append(
            ObjectEntry(name=name, sha256=digest, size=len(data), kind=kind)
        )
    manifest = Manifest(version=version, objects=tuple(entries))
    repo.save_manifest(manifest)
    return manifest


class FlakyTransport(LocalTransport):
    """Drops the link after ``fail_after`` bytes have been transferred."""

    def __init__(self, fail_after):
        self.fail_after = fail_after
        self.seen = 0

    def fetch(self, source, sha256, offset, max_bytes):
        data = super().fetch(source, sha256, offset, max_bytes)
        self.seen += len(data)
        if self.seen > self.fail_after:
            raise TransferError("simulated link failure")
        return data


class CorruptingTransport(LocalTransport):
    """Returns garbage bytes, simulating a corrupted transfer channel."""

    def fetch(self, source, sha256, offset, max_bytes):
        data = super().fetch(source, sha256, offset, max_bytes)
        return bytes(b ^ 0xFF for b in data)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class ReplicationTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="repl-test-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.source = Repository(os.path.join(self.tmp, "source"))
        self.target = Repository(os.path.join(self.tmp, "target"))

    def engine(self, **kwargs):
        kwargs.setdefault("chunk_size", 64)
        return SyncEngine(self.source, self.target, **kwargs)

    def test_full_sync_publishes_version(self):
        manifest = make_version(self.source, "v1", [b"a" * 500, b"b" * 300])
        report = self.engine().sync_version("v1")
        self.assertTrue(report.completed)
        self.assertEqual(report.transferred_bytes, manifest.total_size)
        self.assertEqual(report.objects_transferred, 3)
        self.assertEqual(self.target.read_current(), "v1")
        self.assertTrue(self.target.is_version_complete("v1"))

    def test_budget_exhaustion_saves_resume_position(self):
        manifest = make_version(self.source, "v1", [b"x" * 200, b"y" * 200])
        budget = 150
        report = self.engine(budget_bytes=budget).sync_version("v1")
        self.assertFalse(report.completed)
        self.assertEqual(report.transferred_bytes, budget)
        self.assertIsNone(self.target.read_current())
        self.assertTrue(os.path.isfile(self.target.state_path))
        # Partial data lives only in the staging area, never in objects/.
        objects = os.listdir(os.path.join(self.target.root, "objects"))
        for digest in objects:
            self.assertTrue(self.target.verify_object(digest))
        resumed = self.engine().sync_version("v1")
        self.assertTrue(resumed.completed)
        self.assertTrue(resumed.resumed)
        self.assertEqual(resumed.transferred_bytes, manifest.total_size - budget)
        self.assertEqual(self.target.read_current(), "v1")
        self.assertTrue(self.target.is_version_complete("v1"))
        self.assertFalse(os.path.isfile(self.target.state_path))

    def test_resume_across_multiple_budget_windows(self):
        make_version(self.source, "v1", [b"s" * 100, b"t" * 100, b"u" * 100])
        params_size = len(b'{"lr": 0.001}')
        total = params_size + 300
        spent = 0
        report = None
        for _ in range(20):
            report = self.engine(budget_bytes=50).sync_version("v1")
            spent += report.transferred_bytes
            if report.completed:
                break
        self.assertTrue(report.completed)
        self.assertEqual(spent, total)
        self.assertTrue(self.target.is_version_complete("v1"))

    def test_corrupt_shard_on_target_is_retransferred(self):
        manifest = make_version(self.source, "v1", [b"c" * 400, b"d" * 100])
        self.engine().sync_version("v1")
        shard = manifest.shards[0]
        with open(self.target.object_path(shard.sha256), "r+b") as handle:
            handle.write(b"\x00" * 16)
        report = self.engine().sync_version("v1")
        self.assertTrue(report.completed)
        self.assertEqual(report.transferred_bytes, shard.size)
        self.assertEqual(report.objects_transferred, 1)
        self.assertTrue(self.target.is_version_complete("v1"))

    def test_idempotent_second_run_transfers_nothing(self):
        manifest = make_version(self.source, "v1", [b"e" * 300])
        self.engine().sync_version("v1")
        report = self.engine().sync_version("v1")
        self.assertTrue(report.completed)
        self.assertEqual(report.transferred_bytes, 0)
        self.assertEqual(report.objects_transferred, 0)
        self.assertEqual(report.objects_skipped, len(manifest.objects))

    def test_manifest_inconsistency_blocks_publish(self):
        manifest = make_version(self.source, "v1", [b"f" * 200])
        # Corrupt a source object after the manifest was written.
        shard = manifest.shards[0]
        with open(self.source.object_path(shard.sha256), "r+b") as handle:
            handle.write(b"\xAA" * 8)
        with self.assertRaises(ManifestInconsistentError):
            self.engine().sync_version("v1")
        self.assertIsNone(self.target.read_current())
        self.assertEqual(self.target.list_versions(), [])

    def test_corrupting_channel_raises_and_publishes_nothing(self):
        make_version(self.source, "v1", [b"g" * 200])
        engine = self.engine(transport=CorruptingTransport(), max_retries=2)
        with self.assertRaises(ObjectCorruptError):
            engine.sync_version("v1")
        self.assertIsNone(self.target.read_current())
        self.assertEqual(self.target.list_versions(), [])

    def test_mid_transfer_failure_never_exposes_partial_version(self):
        manifest = make_version(self.source, "v1", [b"h" * 500, b"i" * 500])
        flaky = FlakyTransport(fail_after=120)
        with self.assertRaises(TransferError):
            self.engine(transport=flaky).sync_version("v1")
        # Version pointer untouched: no readable half-published version.
        self.assertIsNone(self.target.read_current())
        self.assertFalse(self.target.is_version_complete("v1"))
        # A later run with a healthy link resumes and completes.
        report = self.engine().sync_version("v1")
        self.assertTrue(report.completed)
        self.assertEqual(self.target.read_current(), "v1")
        # 77 bytes (13 params + 64 shard) arrived before the link dropped.
        self.assertEqual(report.transferred_bytes, manifest.total_size - 77)

    def test_rollback_restores_previous_version(self):
        make_version(self.source, "v1", [b"one" * 100])
        make_version(self.source, "v2", [b"two" * 100])
        engine = self.engine()
        engine.sync_version("v1")
        engine.sync_version("v2")
        self.assertEqual(self.target.read_current(), "v2")
        restored = engine.rollback()
        self.assertEqual(restored, "v1")
        self.assertEqual(self.target.read_current(), "v1")
        self.assertTrue(self.target.is_version_complete("v1"))
        with self.assertRaises(RollbackError):
            engine.rollback()

    def test_rollback_without_history_fails(self):
        with self.assertRaises(RollbackError):
            self.engine().rollback()

    def test_delete_version_garbage_collects_unreferenced_objects(self):
        shared = b"shared" * 50
        make_version(self.source, "v1", [shared, b"old" * 100])
        make_version(self.source, "v2", [shared, b"new" * 100])
        engine = self.engine()
        engine.sync_version("v1")
        engine.sync_version("v2")
        v1_manifest = self.target.load_manifest("v1")
        v2_manifest = self.target.load_manifest("v2")
        removed = engine.delete_version("v1")
        self.assertNotIn("v1", self.target.list_versions())
        self.assertEqual(self.target.read_current(), "v2")
        # Objects unique to v1 are gone; shared objects survive.
        v2_digests = {obj.sha256 for obj in v2_manifest.objects}
        for obj in v1_manifest.objects:
            if obj.sha256 in v2_digests:
                self.assertNotIn(obj.sha256, removed)
                self.assertTrue(self.target.verify_object(obj.sha256))
            else:
                self.assertIn(obj.sha256, removed)
                self.assertFalse(self.target.has_object(obj.sha256))
        self.assertTrue(self.target.is_version_complete("v2"))

    def test_delete_current_version_rejected(self):
        make_version(self.source, "v1", [b"z" * 100])
        engine = self.engine()
        engine.sync_version("v1")
        with self.assertRaises(Exception):
            engine.delete_version("v1")
        self.assertEqual(self.target.read_current(), "v1")

    def test_bandwidth_limit_throttles_transfer(self):
        manifest = make_version(self.source, "v1", [b"w" * 1000])
        clock = FakeClock()
        slept = []

        def fake_sleep(duration):
            slept.append(duration)
            clock.now += duration

        bps = 1000.0
        engine = self.engine(bandwidth_bps=bps, clock=clock, sleep=fake_sleep)
        report = engine.sync_version("v1")
        self.assertTrue(report.completed)
        expected = manifest.total_size / bps
        self.assertAlmostEqual(sum(slept), expected, places=6)
        self.assertAlmostEqual(clock.now, expected, places=6)

    def test_manifest_structural_problems_rejected(self):
        bad = Manifest(version="v9", objects=())
        self.source.save_manifest(bad)
        with self.assertRaises(ManifestInconsistentError):
            self.engine().sync_version("v9")
        self.assertIsNone(self.target.read_current())


if __name__ == "__main__":
    unittest.main()
