import os, sys, tempfile, threading, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from database import ContinuityDB, DomainError, VERSION_CONFLICT


class VersionControlTest(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db"); os.close(fd)
        self.db = ContinuityDB(self.path)
        self.producer = self.db.add_user("制片", "producer")
        self.continuity = self.db.add_user("场记", "continuity")
        self.reviewer = self.db.add_user("审片", "reviewer")
        self.production = self.db.create_production("测试影片", "非线性拍摄", self.producer)
        self.scene = self.db.add_scene(self.production, "S01", "雨夜", 1)
        self.s1 = self.db.add_shot(self.scene, "S01-01", 2, 1, "受伤后", self.continuity)
        self.s2 = self.db.add_shot(self.scene, "S01-02", 1, 2, "受伤前", self.continuity)
        self.injury = self.db.add_element(self.production, "手臂伤痕", "injury", "monotonic", "只能加重")
        self.db.set_element_state(self.s1, self.injury, "重度", 3, "", self.continuity)
        self.db.set_element_state(self.s2, self.injury, "轻度", 1, "", self.continuity)

    def tearDown(self):
        self.db.close(); os.unlink(self.path)

    def test_version_conflict_on_stale_submit(self):
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()
        v0 = shot["version"]
        # First terminal submits based on v0
        self.db.set_element_state(self.s1, self.injury, "中度", 2, "A改", self.continuity,
                                  expected_version=v0, request_id="req-A")
        # Second terminal submits based on stale v0 -> must get version conflict
        with self.assertRaisesRegex(DomainError, "版本冲突"):
            self.db.set_element_state(self.s1, self.injury, "重度", 3, "B改", self.continuity,
                                      expected_version=v0, request_id="req-B")
        # B's state must NOT have been applied
        state = self.db.conn.execute("SELECT state_value,numeric_value FROM element_states WHERE shot_id=? AND element_id=?",
                                     (self.s1, self.injury)).fetchone()
        self.assertEqual("中度", state["state_value"])
        self.assertEqual(2, state["numeric_value"])

    def test_concurrent_submits_first_wins(self):
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()
        v0 = shot["version"]
        results = []
        barrier = threading.Barrier(2)

        def submit(req_id, val):
            barrier.wait()
            try:
                self.db.set_element_state(self.s1, self.injury, val, 2, req_id, self.continuity,
                                          expected_version=v0, request_id=req_id)
                results.append((req_id, "ok"))
            except DomainError as e:
                results.append((req_id, "conflict"))

        t1 = threading.Thread(target=submit, args=("req-A", "中度"))
        t2 = threading.Thread(target=submit, args=("req-B", "重度"))
        t1.start(); t2.start(); t1.join(); t2.join()
        oks = [r for r in results if r[1] == "ok"]
        conflicts = [r for r in results if r[1] == "conflict"]
        self.assertEqual(1, len(oks), f"only one should win, got {results}")
        self.assertEqual(1, len(conflicts), f"one should conflict, got {results}")

    def test_idempotent_retry_no_double_apply(self):
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()
        v0 = shot["version"]
        # Submit once
        self.db.set_element_state(self.s1, self.injury, "中度", 2, "首次", self.continuity,
                                  expected_version=v0, request_id="req-same")
        v1 = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()["version"]
        # Retry with SAME request_id and SAME payload -> replay, no extra version bump
        self.db.set_element_state(self.s1, self.injury, "中度", 2, "首次", self.continuity,
                                  expected_version=v0, request_id="req-same")
        v2 = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()["version"]
        self.assertEqual(v1, v2, "retry must not bump version again")
        # State applied exactly once
        state = self.db.conn.execute("SELECT state_value,numeric_value FROM element_states WHERE shot_id=? AND element_id=?",
                                     (self.s1, self.injury)).fetchone()
        self.assertEqual("中度", state["state_value"])
        self.assertEqual(2, state["numeric_value"])

    def test_failure_leaves_no_half_changes_locked_state(self):
        # Approve a plan to clear the conflict, then lock, then attempt a state change -> fails.
        conflicts = self.db.check_scene(self.scene)
        plan = self.db.propose_adjustment(conflicts[0]["id"], "重度", 3, "调整伤势", self.continuity)
        self.db.review_adjustment(plan, True, self.reviewer, "通过")
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()
        self.db.lock_shot(self.s1, self.continuity, expected_version=shot["version"], request_id="lock-1")
        v_after_lock = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()["version"]
        with self.assertRaisesRegex(DomainError, "锁定"):
            self.db.set_element_state(self.s1, self.injury, "重度", 3, "尝试", self.continuity,
                                      expected_version=v_after_lock, request_id="req-fail")
        v_after_fail = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()["version"]
        self.assertEqual(v_after_lock, v_after_fail, "failed write must not change version")


class ReorderTest(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db"); os.close(fd)
        self.db = ContinuityDB(self.path)
        self.producer = self.db.add_user("制片", "producer")
        self.continuity = self.db.add_user("场记", "continuity")
        self.reviewer = self.db.add_user("审片", "reviewer")
        self.production = self.db.create_production("测试影片", "非线性拍摄", self.producer)
        self.scene = self.db.add_scene(self.production, "S01", "雨夜", 1)
        # narrative order: s1=1 (受伤后/重度), s2=2 (受伤前/轻度) -> regression conflict
        self.s1 = self.db.add_shot(self.scene, "S01-01", 2, 1, "受伤后", self.continuity)
        self.s2 = self.db.add_shot(self.scene, "S01-02", 1, 2, "受伤前", self.continuity)
        self.injury = self.db.add_element(self.production, "手臂伤痕", "injury", "monotonic", "只能加重")
        self.db.set_element_state(self.s1, self.injury, "重度", 3, "", self.continuity)
        self.db.set_element_state(self.s2, self.injury, "轻度", 1, "", self.continuity)
        self.db.check_scene(self.scene)

    def tearDown(self):
        self.db.close(); os.unlink(self.path)

    def test_reorder_recomputes_conflicts_in_new_order(self):
        # Move s2 (narrative_order=2) to narrative_order=1, shifting s1 to 2.
        # New order: s2=1 (轻度), s1=2 (重度) -> monotonic increasing, NO conflict.
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s2,)).fetchone()
        result = self.db.reorder_shot(self.s2, 1, self.continuity,
                                      expected_version=shot["version"], request_id="reorder-1")
        self.assertEqual([], result["conflicts"], "reordering should resolve the regression")
        # Verify orders swapped
        s1_order = self.db.conn.execute("SELECT narrative_order FROM shots WHERE id=?", (self.s1,)).fetchone()["narrative_order"]
        s2_order = self.db.conn.execute("SELECT narrative_order FROM shots WHERE id=?", (self.s2,)).fetchone()["narrative_order"]
        self.assertEqual(2, s1_order)
        self.assertEqual(1, s2_order)

    def test_reorder_voids_approved_plan(self):
        conflicts = self.db.check_scene(self.scene)
        plan = self.db.propose_adjustment(conflicts[0]["id"], "重度", 3, "调整伤势", self.continuity)
        self.db.review_adjustment(plan, True, self.reviewer, "通过")
        # After approval, conflict resolved. Now reorder to bring it back differently.
        # Move s1 to order 2 (swap): s2=1(轻度), s1=2(重度) -> no conflict.
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s1,)).fetchone()
        self.db.reorder_shot(self.s1, 2, self.continuity,
                             expected_version=shot["version"], request_id="reorder-2")
        p = self.db.conn.execute("SELECT status FROM adjustment_plans WHERE id=?", (plan,)).fetchone()
        self.assertEqual("voided", p["status"], "approved plan must be voided after reorder")

    def test_reorder_invalidates_lock_conclusion(self):
        # Lock both shots (scene has a conflict though -> can't lock). First resolve via plan.
        conflicts = self.db.check_scene(self.scene)
        plan = self.db.propose_adjustment(conflicts[0]["id"], "重度", 3, "调整伤势", self.continuity)
        self.db.review_adjustment(plan, True, self.reviewer, "通过")
        self.db.lock_shot(self.s1, self.continuity, request_id="lock-s1")
        self.db.lock_shot(self.s2, self.continuity, request_id="lock-s2")
        # Now reorder an unlocked... both are locked. Unlock one first? Reorder requires unlocked.
        # Instead: create a third unlocked shot, lock s1/s2, then reorder s3.
        s3 = self.db.add_shot(self.scene, "S01-03", 3, 3, "补拍", self.continuity)
        self.db.set_element_state(s3, self.injury, "中度", 2, "", self.continuity, request_id="s3-state")
        # Reorder s3 to position 1 -> shifts everything, lock conclusion invalidated
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (s3,)).fetchone()
        self.db.reorder_shot(s3, 1, self.continuity, expected_version=shot["version"], request_id="reorder-3")
        # All shots should be unlocked now
        for sid in (self.s1, self.s2, s3):
            status = self.db.conn.execute("SELECT status FROM shots WHERE id=?", (sid,)).fetchone()["status"]
            self.assertEqual("planned", status, f"shot {sid} must be unlocked after reorder")

    def test_reorder_version_conflict(self):
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s2,)).fetchone()
        v0 = shot["version"]
        self.db.reorder_shot(self.s2, 1, self.continuity, expected_version=v0, request_id="r-A")
        with self.assertRaisesRegex(DomainError, "版本冲突"):
            self.db.reorder_shot(self.s2, 2, self.continuity, expected_version=v0, request_id="r-B")


class ReorderConflictRecomputeTest(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db"); os.close(fd)
        self.db = ContinuityDB(self.path)
        self.producer = self.db.add_user("制片", "producer")
        self.continuity = self.db.add_user("场记", "continuity")
        self.production = self.db.create_production("测试影片", "非线性拍摄", self.producer)
        self.scene = self.db.add_scene(self.production, "S01", "雨夜", 1)
        # stable element: red -> blue -> red (narrative order)
        self.s1 = self.db.add_shot(self.scene, "S01-01", 1, 1, "", self.continuity)
        self.s2 = self.db.add_shot(self.scene, "S01-02", 2, 2, "", self.continuity)
        self.s3 = self.db.add_shot(self.scene, "S01-03", 3, 3, "", self.continuity)
        self.costume = self.db.add_element(self.production, "外套颜色", "costume", "stable", "")
        self.db.set_element_state(self.s1, self.costume, "红", None, "", self.continuity)
        self.db.set_element_state(self.s2, self.costume, "蓝", None, "", self.continuity)
        self.db.set_element_state(self.s3, self.costume, "红", None, "", self.continuity)

    def tearDown(self):
        self.db.close(); os.unlink(self.path)

    def test_reorder_changes_conflict_pairs(self):
        # Current order s1,s2,s3: red,blue,red -> 2 state_changed conflicts
        conflicts = self.db.check_scene(self.scene)
        self.assertEqual(2, len(conflicts))
        # Move s3 to position 1: new order s3(红),s1(红),s2(蓝) -> red,red,blue -> 1 conflict
        shot = self.db.conn.execute("SELECT version FROM shots WHERE id=?", (self.s3,)).fetchone()
        result = self.db.reorder_shot(self.s3, 1, self.continuity,
                                      expected_version=shot["version"], request_id="r-x")
        self.assertEqual(1, len(result["conflicts"]))
        # The remaining conflict should be between s1 and s2 (red->blue)
        c = result["conflicts"][0]
        self.assertEqual(self.s1, c["from_shot_id"])
        self.assertEqual(self.s2, c["to_shot_id"])


if __name__ == "__main__":
    unittest.main()
