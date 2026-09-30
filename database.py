from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


class DomainError(ValueError):
    """Business rule violation."""


ELEMENT_KINDS = {"character", "costume", "prop", "injury"}
RULES = {"stable", "monotonic", "allowed"}

VERSION_CONFLICT = "镜头版本冲突：他人已先提交，请刷新后基于最新版本重试"


class ContinuityDB:
    """Non-linear film continuity checker with reviewable corrections."""

    def __init__(self, path: str = "continuity.db") -> None:
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL")
        self._schema()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self):
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                yield
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def _idempotent(self, request_id, scene_id, shot_id, operation, fn):
        """Run *fn* exactly once for a client-supplied request_id.

        The operation and its request_log row commit in the same transaction,
        so a retry with the same request_id replays the stored outcome instead
        of applying the write twice. Failures roll back (no half-applied
        changes) and are not cached, so a retry re-attempts the write.
        """
        with self.transaction():
            if request_id is not None:
                row = self.conn.execute(
                    "SELECT outcome FROM request_log WHERE request_id=?", (request_id,)
                ).fetchone()
                if row:
                    return json.loads(row["outcome"])
                if not str(request_id).strip():
                    raise DomainError("请求编号无效")
            result = fn()
            if request_id is not None:
                self.conn.execute(
                    "INSERT INTO request_log(request_id,scene_id,shot_id,operation,outcome,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (request_id, scene_id, shot_id, operation, json.dumps(result, ensure_ascii=False),
                     datetime.now().isoformat()),
                )
            return result

    def _check_shot_version(self, shot: sqlite3.Row, expected_version) -> None:
        if expected_version is not None and shot["version"] != expected_version:
            raise DomainError(
                f"{VERSION_CONFLICT}（当前版本 {shot['version']}，提交版本 {expected_version}）"
            )

    def _schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE,
              role TEXT NOT NULL CHECK(role IN ('producer','continuity','reviewer'))
            );
            CREATE TABLE IF NOT EXISTS productions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              title TEXT NOT NULL,
              description TEXT NOT NULL DEFAULT '',
              created_by INTEGER NOT NULL REFERENCES users(id),
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS scenes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              production_id INTEGER NOT NULL REFERENCES productions(id) ON DELETE CASCADE,
              scene_number TEXT NOT NULL,
              title TEXT NOT NULL,
              narrative_order INTEGER NOT NULL CHECK(narrative_order > 0),
              UNIQUE(production_id,scene_number),
              UNIQUE(production_id,narrative_order)
            );
            CREATE TABLE IF NOT EXISTS shots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              scene_id INTEGER NOT NULL REFERENCES scenes(id) ON DELETE CASCADE,
              shot_code TEXT NOT NULL,
              shoot_order INTEGER NOT NULL CHECK(shoot_order > 0),
              narrative_order INTEGER NOT NULL CHECK(narrative_order > 0),
              description TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'planned' CHECK(status IN ('planned','locked')),
              version INTEGER NOT NULL DEFAULT 0,
              updated_by INTEGER NOT NULL REFERENCES users(id),
              updated_at TEXT NOT NULL,
              UNIQUE(scene_id,shot_code),
              UNIQUE(scene_id,narrative_order)
            );
            CREATE TABLE IF NOT EXISTS elements (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              production_id INTEGER NOT NULL REFERENCES productions(id) ON DELETE CASCADE,
              name TEXT NOT NULL,
              kind TEXT NOT NULL CHECK(kind IN ('character','costume','prop','injury')),
              rule TEXT NOT NULL CHECK(rule IN ('stable','monotonic','allowed')),
              description TEXT NOT NULL DEFAULT '',
              UNIQUE(production_id,name)
            );
            CREATE TABLE IF NOT EXISTS element_transitions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              element_id INTEGER NOT NULL REFERENCES elements(id) ON DELETE CASCADE,
              from_state TEXT NOT NULL,
              to_state TEXT NOT NULL,
              note TEXT NOT NULL DEFAULT '',
              UNIQUE(element_id,from_state,to_state)
            );
            CREATE TABLE IF NOT EXISTS element_states (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              shot_id INTEGER NOT NULL REFERENCES shots(id) ON DELETE CASCADE,
              element_id INTEGER NOT NULL REFERENCES elements(id) ON DELETE CASCADE,
              state_value TEXT NOT NULL,
              numeric_value REAL,
              note TEXT NOT NULL DEFAULT '',
              updated_by INTEGER NOT NULL REFERENCES users(id),
              updated_at TEXT NOT NULL,
              UNIQUE(shot_id,element_id)
            );
            CREATE TABLE IF NOT EXISTS conflicts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              scene_id INTEGER NOT NULL REFERENCES scenes(id) ON DELETE CASCADE,
              element_id INTEGER NOT NULL REFERENCES elements(id) ON DELETE CASCADE,
              from_shot_id INTEGER NOT NULL REFERENCES shots(id),
              to_shot_id INTEGER NOT NULL REFERENCES shots(id),
              kind TEXT NOT NULL,
              detail TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','exempted','resolved')),
              active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
              fingerprint TEXT NOT NULL UNIQUE,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS adjustment_plans (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              conflict_id INTEGER NOT NULL UNIQUE REFERENCES conflicts(id),
              shot_id INTEGER NOT NULL REFERENCES shots(id),
              element_id INTEGER NOT NULL REFERENCES elements(id),
              new_value TEXT NOT NULL,
              numeric_value REAL,
              reason TEXT NOT NULL,
              proposed_by INTEGER NOT NULL REFERENCES users(id),
              status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected','voided')),
              reviewed_by INTEGER REFERENCES users(id),
              review_note TEXT NOT NULL DEFAULT '',
              proposed_at TEXT NOT NULL,
              reviewed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS exemptions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              conflict_id INTEGER NOT NULL UNIQUE REFERENCES conflicts(id),
              reason TEXT NOT NULL,
              approved_by INTEGER NOT NULL REFERENCES users(id),
              approved_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS request_log (
              request_id TEXT PRIMARY KEY,
              scene_id INTEGER,
              shot_id INTEGER,
              operation TEXT NOT NULL,
              outcome TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            """
        )
        self.conn.commit()
        self._migrate()

    def _migrate(self) -> None:
        """Rebuild tables whose CHECK constraints predate newer status values."""
        row = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='adjustment_plans'"
        ).fetchone()
        if row and "'voided'" not in (row["sql"] or ""):
            self.conn.executescript(
                """
                CREATE TABLE adjustment_plans_new (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  conflict_id INTEGER NOT NULL UNIQUE REFERENCES conflicts(id),
                  shot_id INTEGER NOT NULL REFERENCES shots(id),
                  element_id INTEGER NOT NULL REFERENCES elements(id),
                  new_value TEXT NOT NULL,
                  numeric_value REAL,
                  reason TEXT NOT NULL,
                  proposed_by INTEGER NOT NULL REFERENCES users(id),
                  status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected','voided')),
                  reviewed_by INTEGER REFERENCES users(id),
                  review_note TEXT NOT NULL DEFAULT '',
                  proposed_at TEXT NOT NULL,
                  reviewed_at TEXT
                );
                INSERT INTO adjustment_plans_new SELECT * FROM adjustment_plans;
                DROP TABLE adjustment_plans;
                ALTER TABLE adjustment_plans_new RENAME TO adjustment_plans;
                """
            )
            self.conn.commit()

    def seed_demo(self) -> None:
        if self.conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]:
            return
        producer = self.add_user("制片", "producer")
        continuity = self.add_user("场记", "continuity")
        reviewer = self.add_user("审片", "reviewer")
        production = self.create_production("雨夜追踪", "非线性拍摄出的连续性示例", producer)
        scene = self.add_scene(production, "S01", "巷口相遇", 1)
        s01 = self.add_shot(scene, "S01-01", 2, 1, "角色受伤后", continuity)
        s02 = self.add_shot(scene, "S01-02", 1, 2, "角色尚未受伤", continuity)
        injury = self.add_element(production, "主角左臂伤痕", "injury", "monotonic", "伤痕严重程度只能递增")
        self.set_element_state(s01, injury, "重度", 3, "", continuity)
        self.set_element_state(s02, injury, "轻度", 1, "", continuity)
        self.check_scene(scene)

    def add_user(self, name: str, role: str) -> int:
        if not name.strip() or role not in {"producer", "continuity", "reviewer"}:
            raise DomainError("用户名或角色无效")
        with self.transaction():
            try:
                cur = self.conn.execute("INSERT INTO users(name,role) VALUES(?,?)", (name.strip(), role))
            except sqlite3.IntegrityError as exc:
                raise DomainError("用户名已存在") from exc
        return int(cur.lastrowid)

    def create_production(self, title: str, description: str, user_id: int) -> int:
        user = self.conn.execute("SELECT role FROM users WHERE id=?", (user_id,)).fetchone()
        if not user or user["role"] != "producer" or not title.strip():
            raise DomainError("只有制片人可以创建项目")
        with self.transaction():
            cur = self.conn.execute(
                "INSERT INTO productions(title,description,created_by,created_at) VALUES(?,?,?,?)",
                (title.strip(), description.strip(), user_id, datetime.now().isoformat()),
            )
        return int(cur.lastrowid)

    def _production_for_user(self, production_id: int, user_id: int) -> sqlite3.Row:
        user = self.conn.execute("SELECT role FROM users WHERE id=?", (user_id,)).fetchone()
        if not user:
            raise DomainError("用户不存在")
        if user["role"] == "reviewer":
            raise DomainError("审片人员只能审核方案和豁免，不能直接编排")
        return user

    def add_scene(self, production_id: int, scene_number: str, title: str, narrative_order: int) -> int:
        if not self.conn.execute("SELECT 1 FROM productions WHERE id=?", (production_id,)).fetchone():
            raise DomainError("项目不存在")
        if not scene_number.strip() or not title.strip() or narrative_order <= 0:
            raise DomainError("场次参数无效")
        with self.transaction():
            try:
                cur = self.conn.execute(
                    "INSERT INTO scenes(production_id,scene_number,title,narrative_order) VALUES(?,?,?,?)",
                    (production_id, scene_number.strip(), title.strip(), narrative_order),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("场次编号或叙事顺序重复") from exc
        return int(cur.lastrowid)

    def add_shot(self, scene_id: int, shot_code: str, shoot_order: int, narrative_order: int,
                 description: str, user_id: int) -> int:
        scene = self.conn.execute("SELECT production_id FROM scenes WHERE id=?", (scene_id,)).fetchone()
        if not scene:
            raise DomainError("场次不存在")
        user = self._production_for_user(scene["production_id"], user_id)
        if user["role"] not in {"producer", "continuity"}:
            raise DomainError("无权创建镜头")
        if not shot_code.strip() or shoot_order <= 0 or narrative_order <= 0:
            raise DomainError("镜头参数无效")
        with self.transaction():
            try:
                cur = self.conn.execute(
                    "INSERT INTO shots(scene_id,shot_code,shoot_order,narrative_order,description,updated_by,updated_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (scene_id, shot_code.strip(), shoot_order, narrative_order, description.strip(), user_id, datetime.now().isoformat()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("场次内镜头编号或叙事顺序重复") from exc
        return int(cur.lastrowid)

    def add_element(self, production_id: int, name: str, kind: str, rule: str, description: str = "") -> int:
        if not self.conn.execute("SELECT 1 FROM productions WHERE id=?", (production_id,)).fetchone():
            raise DomainError("项目不存在")
        if not name.strip() or kind not in ELEMENT_KINDS or rule not in RULES:
            raise DomainError("连续性元素参数无效")
        with self.transaction():
            try:
                cur = self.conn.execute(
                    "INSERT INTO elements(production_id,name,kind,rule,description) VALUES(?,?,?,?,?)",
                    (production_id, name.strip(), kind, rule, description.strip()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("项目内元素名称不能重复") from exc
        return int(cur.lastrowid)

    def add_transition(self, element_id: int, from_state: str, to_state: str, note: str = "") -> int:
        element = self.conn.execute("SELECT rule FROM elements WHERE id=?", (element_id,)).fetchone()
        if not element or element["rule"] != "allowed":
            raise DomainError("只有 allowed 规则元素需要配置状态转移")
        if not from_state.strip() or not to_state.strip() or from_state == to_state:
            raise DomainError("状态转移必须包含两个不同状态")
        with self.transaction():
            try:
                cur = self.conn.execute(
                    "INSERT INTO element_transitions(element_id,from_state,to_state,note) VALUES(?,?,?,?)",
                    (element_id, from_state.strip(), to_state.strip(), note.strip()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("该状态转移已存在") from exc
        return int(cur.lastrowid)

    def set_element_state(self, shot_id: int, element_id: int, state_value: str, numeric_value: float | None,
                          note: str, user_id: int, expected_version: int | None = None,
                          request_id: str | None = None) -> dict:
        shot = self.conn.execute("SELECT s.*,sc.production_id FROM shots s JOIN scenes sc ON sc.id=s.scene_id WHERE s.id=?", (shot_id,)).fetchone()
        element = self.conn.execute("SELECT * FROM elements WHERE id=?", (element_id,)).fetchone()
        if not shot or not element or shot["production_id"] != element["production_id"]:
            raise DomainError("镜头与元素不属于同一项目")
        user = self._production_for_user(shot["production_id"], user_id)
        if user["role"] not in {"producer", "continuity"}:
            raise DomainError("无权修改连续性状态")
        if not state_value.strip():
            raise DomainError("状态值不能为空")
        if element["rule"] == "monotonic" and numeric_value is None:
            raise DomainError("单调规则必须提供 numeric_value")
        scene_id = shot["scene_id"]

        def impl():
            cur = self.conn.execute("SELECT * FROM shots WHERE id=?", (shot_id,)).fetchone()
            self._check_shot_version(cur, expected_version)
            if cur["status"] == "locked":
                raise DomainError("镜头已锁定，不能直接修改状态")
            try:
                self.conn.execute(
                    "INSERT INTO element_states(shot_id,element_id,state_value,numeric_value,note,updated_by,updated_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (shot_id, element_id, state_value.strip(), numeric_value, note.strip(), user_id, datetime.now().isoformat()),
                )
            except sqlite3.IntegrityError:
                self.conn.execute(
                    "UPDATE element_states SET state_value=?,numeric_value=?,note=?,updated_by=?,updated_at=? WHERE shot_id=? AND element_id=?",
                    (state_value.strip(), numeric_value, note.strip(), user_id, datetime.now().isoformat(), shot_id, element_id),
                )
            self.conn.execute("UPDATE shots SET version=version+1,updated_by=?,updated_at=? WHERE id=?", (user_id, datetime.now().isoformat(), shot_id))
            self._sync_conflicts(scene_id)
            return {"shot_id": shot_id, "element_id": element_id, "conflicts": self.list_conflicts(scene_id)}

        return self._idempotent(request_id, scene_id, shot_id, "set_state", impl)

    def _detect_conflicts(self, scene_id: int) -> list[dict]:
        scene = self.conn.execute("SELECT * FROM scenes WHERE id=?", (scene_id,)).fetchone()
        if not scene:
            raise DomainError("场次不存在")
        shots = self.conn.execute(
            "SELECT * FROM shots WHERE scene_id=? ORDER BY narrative_order", (scene_id,)
        ).fetchall()
        elements = self.conn.execute("SELECT * FROM elements WHERE production_id=? ORDER BY id", (scene["production_id"],)).fetchall()
        detected: list[dict] = []
        for element in elements:
            sequence = []
            for shot in shots:
                state = self.conn.execute(
                    "SELECT * FROM element_states WHERE shot_id=? AND element_id=?", (shot["id"], element["id"])
                ).fetchone()
                if state:
                    sequence.append((shot, state))
            for (prev_shot, prev), (shot, current) in zip(sequence, sequence[1:]):
                kind = None
                detail = ""
                if element["rule"] == "stable":
                    if current["state_value"] != prev["state_value"]:
                        kind = "state_changed"
                        detail = f"{element['name']} 应为稳定状态，却从 {prev['state_value']} 变为 {current['state_value']}"
                elif element["rule"] == "monotonic":
                    if current["numeric_value"] is None or prev["numeric_value"] is None:
                        kind = "missing_numeric_value"
                        detail = f"{element['name']} 缺少可比较的数值"
                    elif current["numeric_value"] < prev["numeric_value"]:
                        kind = "regression"
                        detail = f"{element['name']} 在叙事顺序中从 {prev['numeric_value']} 回退到 {current['numeric_value']}"
                else:
                    allowed = self.conn.execute(
                        "SELECT 1 FROM element_transitions WHERE element_id=? AND from_state=? AND to_state=?",
                        (element["id"], prev["state_value"], current["state_value"]),
                    ).fetchone()
                    if not allowed:
                        kind = "transition_not_allowed"
                        detail = f"{element['name']} 不允许从 {prev['state_value']} 变为 {current['state_value']}"
                if kind:
                    fingerprint = f"{scene_id}:{element['id']}:{prev_shot['id']}:{shot['id']}:{kind}"
                    detected.append({
                        "scene_id": scene_id, "element_id": element["id"], "element_name": element["name"],
                        "from_shot_id": prev_shot["id"], "to_shot_id": shot["id"], "kind": kind,
                        "detail": detail, "fingerprint": fingerprint,
                    })
        return detected

    def _sync_conflicts(self, scene_id: int, invalidate_plans: bool = False) -> None:
        detected = self._detect_conflicts(scene_id)
        active_fingerprints = {row["fingerprint"] for row in detected}
        if invalidate_plans:
            existing = self.conn.execute("SELECT * FROM conflicts WHERE scene_id=?", (scene_id,)).fetchall()
        else:
            existing = self.conn.execute("SELECT * FROM conflicts WHERE scene_id=? AND active=1", (scene_id,)).fetchall()
        for row in existing:
            if row["fingerprint"] not in active_fingerprints:
                if row["active"]:
                    self.conn.execute(
                        "UPDATE conflicts SET active=0,status='resolved',updated_at=? WHERE id=?",
                        (datetime.now().isoformat(), row["id"]),
                    )
                if invalidate_plans:
                    self._void_plan_for_conflict(row["id"])
        for issue in detected:
            existing = self.conn.execute("SELECT * FROM conflicts WHERE fingerprint=?", (issue["fingerprint"],)).fetchone()
            if existing:
                status = "exempted" if existing["status"] == "exempted" else "open"
                self.conn.execute(
                    "UPDATE conflicts SET active=1,status=?,detail=?,updated_at=? WHERE id=?",
                    (status, issue["detail"], datetime.now().isoformat(), existing["id"]),
                )
            else:
                self.conn.execute(
                    "INSERT INTO conflicts(scene_id,element_id,from_shot_id,to_shot_id,kind,detail,status,active,fingerprint,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?, 'open',1,?,?,?)",
                    (issue["scene_id"], issue["element_id"], issue["from_shot_id"], issue["to_shot_id"], issue["kind"], issue["detail"], issue["fingerprint"], datetime.now().isoformat(), datetime.now().isoformat()),
                )

    def _void_plan_for_conflict(self, conflict_id: int) -> None:
        self.conn.execute(
            "UPDATE adjustment_plans SET status='voided',reviewed_at=? WHERE conflict_id=? AND status IN ('pending','approved')",
            (datetime.now().isoformat(), conflict_id),
        )

    def check_scene(self, scene_id: int) -> list[dict]:
        if not self.conn.execute("SELECT 1 FROM scenes WHERE id=?", (scene_id,)).fetchone():
            raise DomainError("场次不存在")
        with self.transaction():
            self._sync_conflicts(scene_id)
        return self.list_conflicts(scene_id)

    def list_conflicts(self, scene_id: int, include_resolved: bool = False) -> list[dict]:
        clause = "" if include_resolved else "AND c.active=1"
        return [dict(row) for row in self.conn.execute(
            "SELECT c.*,e.name AS element_name,fs.shot_code AS from_shot_code,ts.shot_code AS to_shot_code "
            "FROM conflicts c JOIN elements e ON e.id=c.element_id JOIN shots fs ON fs.id=c.from_shot_id JOIN shots ts ON ts.id=c.to_shot_id "
            f"WHERE c.scene_id=? {clause} ORDER BY c.id", (scene_id,)
        ).fetchall()]

    def propose_adjustment(self, conflict_id: int, new_value: str, numeric_value: float | None,
                           reason: str, user_id: int) -> int:
        conflict = self.conn.execute("SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
        if not conflict or not conflict["active"]:
            raise DomainError("冲突不存在或已解决")
        if conflict["status"] != "open":
            raise DomainError("已豁免冲突不能提交状态调整方案")
        shot = self.conn.execute("SELECT * FROM shots WHERE id=?", (conflict["to_shot_id"],)).fetchone()
        element = self.conn.execute("SELECT * FROM elements WHERE id=?", (conflict["element_id"],)).fetchone()
        user = self._production_for_user(element["production_id"], user_id)
        if user["role"] not in {"producer", "continuity"}:
            raise DomainError("无权提出调整方案")
        if shot["status"] == "locked":
            raise DomainError("目标镜头已锁定")
        if not new_value.strip() or len(reason.strip()) < 3:
            raise DomainError("新状态和调整理由必须填写")
        if element["rule"] == "monotonic" and numeric_value is None:
            raise DomainError("单调规则调整必须提供 numeric_value")
        with self.transaction():
            try:
                cur = self.conn.execute(
                    "INSERT INTO adjustment_plans(conflict_id,shot_id,element_id,new_value,numeric_value,reason,proposed_by,proposed_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (conflict_id, shot["id"], element["id"], new_value.strip(), numeric_value, reason.strip(), user_id, datetime.now().isoformat()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("该冲突已有调整方案") from exc
        return int(cur.lastrowid)

    def review_adjustment(self, plan_id: int, approve: bool, reviewer_id: int, note: str = "") -> dict:
        reviewer = self.conn.execute("SELECT role FROM users WHERE id=?", (reviewer_id,)).fetchone()
        if not reviewer or reviewer["role"] != "reviewer":
            raise DomainError("只有审片人可以审核调整方案")
        plan = self.conn.execute("SELECT * FROM adjustment_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan or plan["status"] != "pending":
            raise DomainError("调整方案不存在或已审核")
        if plan["proposed_by"] == reviewer_id:
            raise DomainError("提案人不能审核自己的方案")
        shot = self.conn.execute("SELECT * FROM shots WHERE id=?", (plan["shot_id"],)).fetchone()
        if shot["status"] == "locked":
            raise DomainError("目标镜头已锁定")
        with self.transaction():
            status = "approved" if approve else "rejected"
            self.conn.execute(
                "UPDATE adjustment_plans SET status=?,reviewed_by=?,review_note=?,reviewed_at=? WHERE id=?",
                (status, reviewer_id, note.strip(), datetime.now().isoformat(), plan_id),
            )
            if approve:
                try:
                    self.conn.execute(
                        "INSERT INTO element_states(shot_id,element_id,state_value,numeric_value,note,updated_by,updated_at) VALUES(?,?,?,?,?,?,?)",
                        (plan["shot_id"], plan["element_id"], plan["new_value"], plan["numeric_value"], f"调整方案 #{plan_id}", reviewer_id, datetime.now().isoformat()),
                    )
                except sqlite3.IntegrityError:
                    self.conn.execute(
                        "UPDATE element_states SET state_value=?,numeric_value=?,note=?,updated_by=?,updated_at=? WHERE shot_id=? AND element_id=?",
                        (plan["new_value"], plan["numeric_value"], f"调整方案 #{plan_id}", reviewer_id, datetime.now().isoformat(), plan["shot_id"], plan["element_id"]),
                    )
                self.conn.execute(
                    "UPDATE conflicts SET active=0,status='resolved',updated_at=? WHERE id=?",
                    (datetime.now().isoformat(), plan["conflict_id"]),
                )
                self._sync_conflicts(shot["scene_id"])
        return {"plan_id": plan_id, "status": status, "conflicts": self.list_conflicts(shot["scene_id"])}

    def approve_exemption(self, conflict_id: int, reason: str, reviewer_id: int) -> int:
        reviewer = self.conn.execute("SELECT role FROM users WHERE id=?", (reviewer_id,)).fetchone()
        conflict = self.conn.execute("SELECT * FROM conflicts WHERE id=?", (conflict_id,)).fetchone()
        if not conflict or not conflict["active"] or not reviewer or reviewer["role"] != "reviewer":
            raise DomainError("冲突或审片人无效")
        if len(reason.strip()) < 8:
            raise DomainError("豁免理由至少8个字符")
        with self.transaction():
            try:
                cur = self.conn.execute(
                    "INSERT INTO exemptions(conflict_id,reason,approved_by,approved_at) VALUES(?,?,?,?)",
                    (conflict_id, reason.strip(), reviewer_id, datetime.now().isoformat()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("该冲突已经豁免") from exc
            self.conn.execute("UPDATE conflicts SET status='exempted',updated_at=? WHERE id=?", (datetime.now().isoformat(), conflict_id))
        return int(cur.lastrowid)

    def lock_shot(self, shot_id: int, user_id: int, expected_version: int | None = None,
                  request_id: str | None = None) -> dict:
        shot = self.conn.execute("SELECT s.*,sc.production_id FROM shots s JOIN scenes sc ON sc.id=s.scene_id WHERE s.id=?", (shot_id,)).fetchone()
        if not shot:
            raise DomainError("镜头不存在")
        user = self._production_for_user(shot["production_id"], user_id)
        if user["role"] not in {"producer", "continuity"}:
            raise DomainError("无权锁定镜头")
        scene_id = shot["scene_id"]

        def impl():
            cur = self.conn.execute("SELECT * FROM shots WHERE id=?", (shot_id,)).fetchone()
            self._check_shot_version(cur, expected_version)
            self._sync_conflicts(scene_id)
            blocking = self.conn.execute(
                "SELECT COUNT(*) FROM conflicts WHERE scene_id=? AND active=1 AND status!='exempted'", (scene_id,)
            ).fetchone()[0]
            if blocking:
                raise DomainError(f"场次仍有 {blocking} 个未处理冲突，不能锁定")
            self.conn.execute("UPDATE shots SET status='locked',version=version+1,updated_by=?,updated_at=? WHERE id=?", (user_id, datetime.now().isoformat(), shot_id))
            return {"shot_id": shot_id, "status": "locked", "conflicts": self.list_conflicts(scene_id)}

        return self._idempotent(request_id, scene_id, shot_id, "lock", impl)

    def reorder_shot(self, shot_id: int, new_narrative_order: int, user_id: int,
                     expected_version: int | None = None, request_id: str | None = None) -> dict:
        """Move an unlocked shot to a new narrative position.

        Recomputes conflicts in the new order, voids adjustment plans that
        resolved conflicts which disappeared, and invalidates prior lock
        conclusions (shots are unlocked and must be re-locked).
        """
        shot = self.conn.execute("SELECT s.*,sc.production_id FROM shots s JOIN scenes sc ON sc.id=s.scene_id WHERE s.id=?", (shot_id,)).fetchone()
        if not shot:
            raise DomainError("镜头不存在")
        user = self._production_for_user(shot["production_id"], user_id)
        if user["role"] not in {"producer", "continuity"}:
            raise DomainError("无权调整镜头叙事顺序")
        if new_narrative_order <= 0:
            raise DomainError("叙事顺序必须为正整数")
        scene_id = shot["scene_id"]

        def impl():
            cur = self.conn.execute("SELECT * FROM shots WHERE id=?", (shot_id,)).fetchone()
            self._check_shot_version(cur, expected_version)
            if cur["status"] == "locked":
                raise DomainError("镜头已锁定，不能调整叙事顺序")
            old_order = cur["narrative_order"]
            if new_narrative_order != old_order:
                max_order = self.conn.execute(
                    "SELECT COALESCE(MAX(narrative_order),0) FROM shots WHERE scene_id=?", (scene_id,)
                ).fetchone()[0]
                temp_base = max_order + 100000
                # Two-phase shift through a temp range to avoid unique-constraint collisions.
                # 1. Move the moved shot to temp, vacating its old position.
                self.conn.execute(
                    "UPDATE shots SET narrative_order=? WHERE id=?", (temp_base + old_order, shot_id)
                )
                # 2. Move affected shots into the temp range.
                if new_narrative_order < old_order:
                    self.conn.execute(
                        f"UPDATE shots SET narrative_order=narrative_order+{temp_base} "
                        "WHERE scene_id=? AND narrative_order>=? AND narrative_order<?",
                        (scene_id, new_narrative_order, old_order),
                    )
                else:
                    self.conn.execute(
                        f"UPDATE shots SET narrative_order=narrative_order+{temp_base} "
                        "WHERE scene_id=? AND narrative_order>? AND narrative_order<=?",
                        (scene_id, old_order, new_narrative_order),
                    )
                # 3. Move affected shots from temp to their final positions, bumping versions.
                if new_narrative_order < old_order:
                    self.conn.execute(
                        f"UPDATE shots SET narrative_order=narrative_order-{temp_base}+1, version=version+1 "
                        "WHERE scene_id=? AND narrative_order>=? AND narrative_order<?",
                        (scene_id, temp_base + new_narrative_order, temp_base + old_order),
                    )
                else:
                    self.conn.execute(
                        f"UPDATE shots SET narrative_order=narrative_order-{temp_base}-1, version=version+1 "
                        "WHERE scene_id=? AND narrative_order>? AND narrative_order<=?",
                        (scene_id, temp_base + old_order, temp_base + new_narrative_order),
                    )
                # 4. Move the moved shot to its final position, bumping version.
                self.conn.execute(
                    "UPDATE shots SET narrative_order=?, version=version+1, updated_by=?, updated_at=? WHERE id=?",
                    (new_narrative_order, user_id, datetime.now().isoformat(), shot_id),
                )
                # Prior lock conclusions are based on the old order: unlock and re-verify.
                self.conn.execute(
                    "UPDATE shots SET status='planned', version=version+1 WHERE scene_id=? AND status='locked'",
                    (scene_id,),
                )
                self._sync_conflicts(scene_id, invalidate_plans=True)
            return {
                "shot_id": shot_id,
                "scene_id": scene_id,
                "narrative_order": new_narrative_order,
                "conflicts": self.list_conflicts(scene_id),
            }

        return self._idempotent(request_id, scene_id, shot_id, "reorder", impl)

    def list_plans(self, scene_id: int) -> list[dict]:
        return [dict(row) for row in self.conn.execute(
            "SELECT p.*,fs.shot_code AS from_shot_code,ts.shot_code AS to_shot_code "
            "FROM adjustment_plans p "
            "JOIN conflicts c ON c.id=p.conflict_id "
            "JOIN shots fs ON fs.id=c.from_shot_id JOIN shots ts ON ts.id=c.to_shot_id "
            "WHERE c.scene_id=? ORDER BY p.id", (scene_id,)
        ).fetchall()]

    def continuity_report(self, production_id: int) -> dict:
        production = self.conn.execute("SELECT * FROM productions WHERE id=?", (production_id,)).fetchone()
        if not production:
            raise DomainError("项目不存在")
        scenes = []
        for scene in self.conn.execute("SELECT * FROM scenes WHERE production_id=? ORDER BY narrative_order", (production_id,)).fetchall():
            shots = [dict(r) for r in self.conn.execute("SELECT * FROM shots WHERE scene_id=? ORDER BY narrative_order", (scene["id"],))]
            conflicts = self.list_conflicts(scene["id"], include_resolved=True)
            plans = self.list_plans(scene["id"])
            scenes.append({**dict(scene), "shots": shots, "conflicts": conflicts, "plans": plans})
        return {
            "production": dict(production),
            "elements": [dict(r) for r in self.conn.execute("SELECT * FROM elements WHERE production_id=? ORDER BY id", (production_id,))],
            "scenes": scenes,
            "open_conflicts": sum(1 for scene in scenes for c in scene["conflicts"] if c["active"] and c["status"] == "open"),
            "exempted_conflicts": sum(1 for scene in scenes for c in scene["conflicts"] if c["active"] and c["status"] == "exempted"),
            "voided_plans": sum(1 for scene in scenes for p in scene["plans"] if p["status"] == "voided"),
        }

    def snapshot(self) -> dict:
        return {
            "users": [dict(r) for r in self.conn.execute("SELECT id,name,role FROM users ORDER BY id")],
            "productions": [dict(r) for r in self.conn.execute("SELECT * FROM productions ORDER BY id")],
            "scenes": [dict(r) for r in self.conn.execute("SELECT * FROM scenes ORDER BY production_id,narrative_order")],
            "shots": [dict(r) for r in self.conn.execute("SELECT * FROM shots ORDER BY scene_id,narrative_order")],
        }
