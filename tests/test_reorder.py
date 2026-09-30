import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as app_module
from database import ContinuityDB, DomainError, VersionConflict


def make_db(path):
    db = ContinuityDB(path)
    producer = db.add_user("制片", "producer")
    continuity = db.add_user("场记", "continuity")
    reviewer = db.add_user("审片", "reviewer")
    production = db.create_production("测试影片", "非线性拍摄", producer)
    scene = db.add_scene(production, "S01", "雨夜", 1)
    s1 = db.add_shot(scene, "S01-01", 3, 1, "位置1", continuity)
    s2 = db.add_shot(scene, "S01-02", 2, 2, "位置2", continuity)
    s3 = db.add_shot(scene, "S01-03", 1, 3, "位置3", continuity)
    injury = db.add_element(production, "手臂伤痕", "injury", "monotonic", "只能加重")
    db.set_element_state(s1, injury, "轻度", 1, "", continuity)
    db.set_element_state(s2, injury, "中度", 2, "", continuity)
    db.set_element_state(s3, injury, "重度", 3, "", continuity)
    return db, dict(producer=producer, continuity=continuity, reviewer=reviewer,
                    production=production, scene=scene, s1=s1, s2=s2, s3=s3, injury=injury)


class ReorderFlowTest(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db"); os.close(fd)
        self.db, self.ids = make_db(self.path)

    def tearDown(self):
        self.db.close(); os.unlink(self.path)

    def version(self, shot_id):
        return self.db.conn.execute("SELECT version FROM shots WHERE id=?", (shot_id,)).fetchone()[0]

    def order(self):
        return [r[0] for r in self.db.conn.execute(
            "SELECT id FROM shots WHERE scene_id=? ORDER BY narrative_order", (self.ids["scene"],))]

    def test_reorder_recalculates_conflicts_and_invalidates_plan_and_lock(self):
        i = self.ids
        # Clean narrative chain 1,2,3: lock all three conclusions.
        self.db.lock_shot(i["s1"], i["continuity"])
        self.db.lock_shot(i["s2"], i["continuity"])
        self.db.lock_shot(i["s3"], i["continuity"])
        # A fourth (unlocked) shot with a lighter injury value creates a regression.
        s4 = self.db.add_shot(i["scene"], "S01-04", 4, 4, "后来补拍", i["continuity"])
        self.db.set_element_state(s4, i["injury"], "无伤", 0, "", i["continuity"])
        conflicts = self.db.check_scene(i["scene"])
        self.assertEqual([(i["s3"], s4)], [(c["from_shot_id"], c["to_shot_id"]) for c in conflicts])
        plan = self.db.propose_adjustment(conflicts[0]["id"], "重度", 3, "把补拍镜头伤势调整为叙事合理值", i["continuity"])

        # Move s4 from position 4 into position 2.
        r = self.db.reorder_shot(s4, 2, self.version(s4), i["continuity"], "req-1")
        new_order = [i["s1"], s4, i["s2"], i["s3"]]
        self.assertEqual(new_order, self.order())
        # The old conflict pair (s3,s4) disappears -> conflict and plan invalidated.
        self.assertEqual([conflicts[0]["id"]], r["invalidated_conflict_ids"])
        self.assertEqual([plan], r["invalidated_plan_ids"])
        # Neighbors changed for s1, s2 and s3: their lock conclusions die.
        self.assertEqual(sorted([i["s1"], i["s2"], i["s3"]]), sorted(r["unlocked_shot_ids"]))
        statuses = dict(self.db.conn.execute("SELECT id,status FROM shots").fetchall())
        self.assertEqual("planned", statuses[i["s1"]])
        self.assertEqual("planned", statuses[i["s2"]])
        self.assertEqual("planned", statuses[i["s3"]])
        # Conflicts recomputed on the new chain: s1(1)->s4(0) regresses.
        self.assertEqual(1, len(r["conflicts"]))
        self.assertEqual((i["s1"], s4), (r["conflicts"][0]["from_shot_id"], r["conflicts"][0]["to_shot_id"]))
        # The stale plan must no longer be reviewable.
        with self.assertRaisesRegex(DomainError, "失效"):
            self.db.review_adjustment(plan, True, i["reviewer"], "通过")
        # Report surfaces invalidated plans and the current narrative order.
        report = self.db.continuity_report(i["production"])
        self.assertEqual(1, report["invalidated_plans"])
        self.assertTrue(report["plans"][0]["invalidated"])
        self.assertEqual([i["s1"], s4, i["s2"], i["s3"]],
                         [s["id"] for s in report["scenes"][0]["shots"]])

    def test_stale_version_rejected_with_version_conflict(self):
        i = self.ids
        seen = self.version(i["s3"])
        self.db.reorder_shot(i["s3"], 2, seen, i["continuity"], "req-a")
        with self.assertRaisesRegex(VersionConflict, "版本冲突"):
            self.db.reorder_shot(i["s3"], 1, seen, i["continuity"], "req-b")
        # The losing request changed nothing: order stays at the winner's result.
        self.assertEqual([i["s1"], i["s3"], i["s2"]], self.order())

    def test_missing_version_and_request_id_rejected(self):
        i = self.ids
        with self.assertRaisesRegex(DomainError, "版本号"):
            self.db.reorder_shot(i["s1"], 2, None, i["continuity"], "req-x")
        with self.assertRaisesRegex(DomainError, "请求编号"):
            self.db.reorder_shot(i["s1"], 2, self.version(i["s1"]), i["continuity"], "")

    def test_idempotent_retry_returns_first_result(self):
        i = self.ids
        seen = self.version(i["s3"])
        first = self.db.reorder_shot(i["s3"], 1, seen, i["continuity"], "req-retry")
        self.assertFalse(first["retried"])
        self.assertEqual(1, first["attempt"])
        # Retry with the same request id: same payload, flagged as a replay.
        again = self.db.reorder_shot(i["s3"], 1, seen, i["continuity"], "req-retry")
        self.assertTrue(again["retried"])
        self.assertEqual(2, again["attempt"])
        self.assertEqual(first["version"], again["version"])
        self.assertEqual([i["s3"], i["s1"], i["s2"]], self.order())
        # Versions bumped only once (order 3 -> s3 v2; replay adds no bump).
        self.assertEqual(seen + 1, self.version(i["s3"]))
        # A different operation cannot reuse the request number.
        with self.assertRaisesRegex(DomainError, "不能复用"):
            self.db.reorder_shot(i["s1"], 3, self.version(i["s1"]), i["continuity"], "req-retry")

    def test_failed_attempt_leaves_no_partial_change(self):
        i = self.ids
        seen = self.version(i["s3"])
        with self.assertRaises(DomainError):
            self.db.reorder_shot(i["s3"], 9, seen, i["continuity"], "req-fail")
        # Nothing moved, no version bump, no idempotency row consumed.
        self.assertEqual([i["s1"], i["s2"], i["s3"]], self.order())
        self.assertEqual(seen, self.version(i["s3"]))
        self.assertEqual(0, self.db.conn.execute(
            "SELECT COUNT(*) FROM idempotent_requests WHERE request_id='req-fail'").fetchone()[0])
        # The request number can be reused for a corrected submission.
        ok = self.db.reorder_shot(i["s3"], 1, seen, i["continuity"], "req-fail")
        self.assertFalse(ok["retried"])

    def test_locked_shot_cannot_be_moved(self):
        i = self.ids
        self.db.lock_shot(i["s2"], i["continuity"])
        with self.assertRaisesRegex(DomainError, "已锁定"):
            self.db.reorder_shot(i["s2"], 3, self.version(i["s2"]), i["continuity"], "req-lock")

    def test_two_shot_swap_marks_both_affected(self):
        i = self.ids
        # Clean 2-shot scene: lock one, then swap order; the locked conclusion must die.
        scene2 = self.db.add_scene(i["production"], "S02", "另一处", 2)
        a = self.db.add_shot(scene2, "S02-01", 2, 1, "一", i["continuity"])
        b = self.db.add_shot(scene2, "S02-02", 1, 2, "二", i["continuity"])
        self.db.lock_shot(a, i["continuity"])
        r = self.db.reorder_shot(b, 1, self.version(b), i["continuity"], "req-swap")
        self.assertEqual([b, a], [x[0] for x in self.db.conn.execute(
            "SELECT id FROM shots WHERE scene_id=? ORDER BY narrative_order", (scene2,))])
        self.assertEqual(sorted([a, b]), sorted(r["affected_shot_ids"]))
        self.assertEqual([a], r["unlocked_shot_ids"])

    def test_far_shots_keep_their_locks(self):
        i = self.ids
        s4 = self.db.add_shot(i["scene"], "S01-04", 4, 4, "四", i["continuity"])
        s5 = self.db.add_shot(i["scene"], "S01-05", 5, 5, "五", i["continuity"])
        s6 = self.db.add_shot(i["scene"], "S01-06", 6, 6, "六", i["continuity"])
        s7 = self.db.add_shot(i["scene"], "S01-07", 7, 7, "七", i["continuity"])
        for sid in (i["s1"], i["s2"], i["s3"], s4, s5, s6):
            self.db.lock_shot(sid, i["continuity"])  # s7 stays unlocked: it is being moved
        # Move s7 from the tail to the head: the middle segment s4/s5/s6 keeps neighborhoods.
        r = self.db.reorder_shot(s7, 1, self.version(s7), i["continuity"], "req-local")
        old_seq = [i["s1"], i["s2"], i["s3"], s4, s5, s6, s7]
        new_order = [s7, i["s1"], i["s2"], i["s3"], s4, s5, s6]
        self.assertEqual(new_order, self.order())
        old_map, new_map = self.db._neighbor_map(old_seq), self.db._neighbor_map(new_order)
        expect = {sid for sid in (i["s1"], i["s2"], i["s3"], s4, s5, s6) if old_map[sid] != new_map[sid]}
        self.assertEqual(sorted(expect), sorted(r["unlocked_shot_ids"]))
        statuses = dict(self.db.conn.execute("SELECT id,status FROM shots").fetchall())
        # Only s1 (new predecessor s7) and s6 (old predecessor s7 moved away) are affected.
        self.assertEqual({i["s1"], s6}, expect)
        # The inner segment keeps its neighborhoods and its lock conclusions.
        for sid in (i["s2"], i["s3"], s4, s5):
            self.assertEqual("locked", statuses[sid])


class ReorderHttpTest(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db"); os.close(fd)
        self.db, self.ids = make_db(self.path)
        app_module.Handler.db = self.db
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), app_module.Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()
        self.db.close(); os.unlink(self.path)
        for suffix in ("-wal", "-shm"):
            p = self.path + suffix
            if os.path.exists(p):
                os.unlink(p)

    def _post(self, path, body):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_http_version_conflict_409_and_idempotent_retry(self):
        i = self.ids
        seen = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (i["s3"],)).fetchone()[0]
        status, body = self._post(f"/api/shots/{i['s3']}/reorder", {
            "narrative_order": 2, "expected_version": seen, "user_id": i["continuity"], "request_id": "http-1"})
        self.assertEqual(200, status)
        self.assertFalse(body["retried"])
        status, body = self._post(f"/api/shots/{i['s3']}/reorder", {
            "narrative_order": 1, "expected_version": seen, "user_id": i["continuity"], "request_id": "http-2"})
        self.assertEqual(409, status)
        self.assertEqual("版本冲突", body["error"])
        self.assertIn(str(seen), body["detail"])
        # Retry the original request id: server replays the first result.
        status, body = self._post(f"/api/shots/{i['s3']}/reorder", {
            "narrative_order": 1, "expected_version": seen, "user_id": i["continuity"], "request_id": "http-1"})
        self.assertEqual(200, status)
        self.assertTrue(body["retried"])
        self.assertEqual(2, body["attempt"])
        # First request moved s3 to position 2; replay does not re-apply anything.
        codes = [row["shot_code"] for row in body["narrative_order"]]
        self.assertEqual(["S01-01", "S01-03", "S01-02"], codes)

    def test_concurrent_scene_commit_only_first_wins(self):
        i = self.ids
        # Both terminals opened the editor at the same version and submit at once.
        seen = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (i["s1"],)).fetchone()[0]
        barrier = threading.Barrier(2)
        results = []

        def worker(req_id):
            db = ContinuityDB(self.path)
            barrier.wait()
            try:
                results.append(("ok", db.reorder_shot(i["s1"], 3, seen, i["continuity"], req_id)))
            except VersionConflict as exc:
                results.append(("conflict", str(exc)))
            finally:
                db.close()

        t1 = threading.Thread(target=worker, args=("con-1",))
        t2 = threading.Thread(target=worker, args=("con-2",))
        t1.start(); t2.start(); t1.join(); t2.join()
        outcomes = sorted(r[0] for r in results)
        self.assertEqual(["conflict", "ok"], outcomes)


if __name__ == "__main__":
    unittest.main()
