"""受控规则发布与历史重放引擎。

两条写流的分界（发布期间正常采集不停止）：

* 正常采集流（live）：新响应归档后，按*当前可见版本*的规则解析并 upsert
  到该版本的行表，只推进该版本自己的水位 high_seq。
* 重放流（replay）：启动时冻结一个 seq 上界，之后只处理冻结清单里的响应；
  运行期间新到达的响应即使已经归档，也不会被偷偷纳入重放范围，
  它们由正常采集流继续写入当前可见版本。

追赶机制：影子版本通过业务检查并执行切换时，在一个 IMMEDIATE 事务内
重新捕获水位上界，把冻结点之后到达的响应（含事务临界时刻到达的响应）
用新版本规则补齐处理，再原子翻转可见版本指针。采集全程不需要停止。

隔离保证：重放候选只写影子版本；失败与取消不触碰可见版本；
切换与回退都是单行指针的原子更新，读请求永远看到一个完整版本。
"""

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager

from .rules import (
    RuleError,
    RuleRegistry,
    canonical_json,
    classify_parse_error,
    digest_json,
)


class ReplayError(RuntimeError):
    """重放运行层面的错误（取消、已失败、状态不允许等）。"""


class RolloutStore:
    def __init__(self, path, registry):
        if not isinstance(registry, RuleRegistry):
            raise TypeError('需要 RuleRegistry 实例')
        self.registry = registry
        # check_same_thread=False：正常采集线程与发布线程可能共享连接，
        # 写并发由每个写事务的 BEGIN IMMEDIATE 串行化，读为 WAL 快照读。
        self.connection = sqlite3.connect(
            path, timeout=30, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.execute('PRAGMA journal_mode=WAL')
        self._create_schema()
        self._hydrate_registry()

    # ------------------------------------------------------------------ 基础
    def _create_schema(self):
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS ro_raw_archive(
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                page_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                digest TEXT NOT NULL,
                arrived_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS ro_rule_releases(
                name TEXT NOT NULL,
                version INTEGER NOT NULL,
                input_fields TEXT NOT NULL,
                output_fields TEXT NOT NULL,
                accepted_units TEXT NOT NULL,
                code_digest TEXT NOT NULL,
                source_hint TEXT NOT NULL,
                registered_at REAL NOT NULL,
                PRIMARY KEY(name, version));
            CREATE TABLE IF NOT EXISTS ro_versions(
                version_id TEXT PRIMARY KEY,
                rule_name TEXT NOT NULL,
                rule_version INTEGER NOT NULL,
                parent_version_id TEXT,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                switched_at REAL,
                switch_watermark INTEGER,
                bundle_digest TEXT,
                run_id TEXT,
                FOREIGN KEY(rule_name, rule_version)
                    REFERENCES ro_rule_releases(name, version));
            CREATE TABLE IF NOT EXISTS ro_version_rows(
                version_id TEXT NOT NULL,
                record_key TEXT NOT NULL,
                payload TEXT NOT NULL,
                source_seq INTEGER NOT NULL,
                rule_version INTEGER NOT NULL,
                PRIMARY KEY(version_id, record_key),
                FOREIGN KEY(version_id) REFERENCES ro_versions(version_id));
            CREATE TABLE IF NOT EXISTS ro_replay_runs(
                run_id TEXT PRIMARY KEY,
                rule_name TEXT NOT NULL,
                rule_version INTEGER NOT NULL,
                bundle_digest TEXT NOT NULL,
                frozen_high_seq INTEGER NOT NULL,
                run_params TEXT NOT NULL,
                contract_snapshot TEXT NOT NULL,
                status TEXT NOT NULL,
                failure_reason TEXT,
                created_at REAL NOT NULL,
                finished_at REAL);
            CREATE TABLE IF NOT EXISTS ro_replay_items(
                run_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                state TEXT NOT NULL,
                quarantine_id TEXT,
                PRIMARY KEY(run_id, seq),
                FOREIGN KEY(run_id) REFERENCES ro_replay_runs(run_id));
            CREATE TABLE IF NOT EXISTS ro_quarantine(
                quarantine_id TEXT PRIMARY KEY,
                run_id TEXT,
                phase TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                rule_version INTEGER NOT NULL,
                source_seq INTEGER,
                page_id TEXT,
                digest TEXT,
                kind TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS ro_events(
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                at REAL NOT NULL,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL);
            """
        )

    def _hydrate_registry(self):
        """重开库后校验已注册规则的代码摘要未被偷换。

        注册表本身在进程内不可覆盖；数据库里的发布记录用于跨进程核对，
        摘要不一致意味着规则代码在重放间隙被修改，直接拒绝继续。
        """
        for row in self.connection.execute(
            'SELECT name, version, code_digest FROM ro_rule_releases'
        ):
            release = self.registry.release(row['name'], row['version'])
            if release.code_digest != row['code_digest']:
                raise RuntimeError(
                    f"规则 {row['name']} v{row['version']} 代码摘要与注册时不一致，"
                    '规则代码在发布后被修改'
                )

    @contextmanager
    def transaction(self):
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    @contextmanager
    def write_lock(self):
        """显式持有写锁但不提交，供演示发布临界点的锁竞争。"""
        self.connection.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self):
        self.connection.close()

    def _log_event(self, kind, payload):
        self.connection.execute(
            'INSERT INTO ro_events(at, kind, payload) VALUES (?,?,?)',
            (time.time(), kind, canonical_json(payload)),
        )

    # ----------------------------------------------------------- 规则发布登记
    def register_rule(self, release):
        """把不可变规则发布落库；重复登记必须与既有摘要完全一致。"""
        row = self.connection.execute(
            'SELECT code_digest FROM ro_rule_releases WHERE name=? AND version=?',
            (release.name, release.version),
        ).fetchone()
        if row is not None:
            if row['code_digest'] != release.code_digest:
                raise ValueError(
                    f'规则版本已登记且摘要不同: {release.name} v{release.version}'
                )
            return
        with self.transaction():
            self.connection.execute(
                'INSERT INTO ro_rule_releases'
                '(name, version, input_fields, output_fields, accepted_units,'
                ' code_digest, source_hint, registered_at)'
                ' VALUES (?,?,?,?,?,?,?,?)',
                (
                    release.name,
                    release.version,
                    canonical_json(sorted(release.input_fields)),
                    canonical_json(sorted(release.output_fields)),
                    canonical_json(sorted(release.accepted_units)),
                    release.code_digest,
                    release.source_hint,
                    time.time(),
                ),
            )
            self._log_event('rule_registered', release.to_row())

    # ------------------------------------------------------------- 原始归档
    def _load_page(self, seq):
        row = self.connection.execute(
            'SELECT payload, digest FROM ro_raw_archive WHERE seq=?', (seq,)
        ).fetchone()
        if row is None:
            raise ReplayError(f'归档中不存在 seq={seq}')
        return json.loads(row['payload']), row['digest']

    def current_high_seq(self):
        row = self.connection.execute('SELECT max(seq) AS m FROM ro_raw_archive').fetchone()
        return row['m'] or 0

    # ------------------------------------------------------------- 版本管理

    # ------------------------------------------------------------- 版本管理
    def active_version(self):
        row = self.connection.execute(
            "SELECT version_id FROM ro_versions WHERE status='active'"
        ).fetchone()
        return row['version_id'] if row else None

    def _get_version(self, version_id):
        row = self.connection.execute(
            'SELECT * FROM ro_versions WHERE version_id=?', (version_id,)
        ).fetchone()
        if row is None:
            raise ReplayError(f'版本不存在: {version_id}')
        return row

    def bootstrap_version(self, name, version):
        """把第一个规则版本登记为初始可见版本（无父版本，空行集）。"""
        self.register_rule(self.registry.release(name, version))
        existing = self.active_version()
        if existing is not None:
            return existing
        version_id = f'v{version}-{uuid.uuid4().hex[:10]}'
        with self.transaction():
            self.connection.execute(
                'INSERT INTO ro_versions'
                '(version_id, rule_name, rule_version, parent_version_id, status,'
                ' created_at, switched_at, switch_watermark) VALUES (?,?,?,?,?,?,?,?)',
                (version_id, name, version, None, 'active', time.time(), time.time(), 0),
            )
            self._log_event('version_bootstrapped',
                            {'version_id': version_id, 'rule_version': version})
        return version_id

    # ----------------------------------------------------- 正常采集流（live）
    def live_ingest(self, page):
        """发布期间持续运行的写流：按当前可见版本规则写入。

        永远只写 active 版本；影子版本对该流不可见，因此重放不可能
        覆盖正在被查询的行。
        """
        active_id = self.active_version()
        if active_id is None:
            raise ReplayError('尚无可见版本，请先 bootstrap_version')
        active = self._get_version(active_id)
        name, rule_version = active['rule_name'], active['rule_version']
        with self.transaction():
            return self._live_ingest_locked(page, name, rule_version)

    def _live_ingest_locked(self, page, name=None, rule_version=None):
        """在调用方已持有的写事务内完成归档+解析+写入（用于更大的工作单元）。"""
        if name is None:
            active_id = self.active_version()
            if active_id is None:
                raise ReplayError('尚无可见版本，请先 bootstrap_version')
            active = self._get_version(active_id)
            name, rule_version = active['rule_name'], active['rule_version']
            active_id = active['version_id']
        else:
            active_id = self.active_version()
        encoded = canonical_json(page)
        digest = digest_json(page)
        cursor = self.connection.execute(
            'INSERT INTO ro_raw_archive(page_id, payload, digest, arrived_at)'
            ' VALUES (?,?,?,?)',
            (str(page.get('id', f'unknown-{digest[:8]}')), encoded, digest, time.time()))
        seq = cursor.lastrowid
        self._log_event('raw_archived', {'seq': seq, 'page_id': page.get('id'),
                                         'digest': digest})
        try:
            parsed = self.registry.parse(name, rule_version, page)
        except RuleError as error:
            kind, reason = classify_parse_error(self.registry.release(name, rule_version), error)
            quarantine_id = self._insert_quarantine(
                run_id=None, phase='live', name=name, rule_version=rule_version,
                seq=seq, page=page, digest=digest, kind=kind, reason=reason)
            self._log_event('live_quarantined',
                            {'seq': seq, 'quarantine_id': quarantine_id, 'reason': reason})
            return {'seq': seq, 'digest': digest, 'quarantined': True,
                    'quarantine_id': quarantine_id, 'reason': reason}
        self._upsert_row(active_id, parsed, seq, rule_version)
        self._log_event('live_ingested',
                        {'seq': seq, 'key': parsed['key'], 'version_id': active_id})
        return {'seq': seq, 'digest': digest, 'quarantined': False,
                'key': parsed['key'], 'version_id': active_id}

    def _upsert_row(self, version_id, parsed, source_seq, rule_version):
        key = parsed['key']
        if not isinstance(key, str) or not key:
            raise ValueError('记录缺少有效标识')
        self.connection.execute(
            'INSERT INTO ro_version_rows(version_id, record_key, payload, source_seq, rule_version)'
            ' VALUES (?,?,?,?,?)'
            ' ON CONFLICT(version_id, record_key) DO UPDATE SET'
            ' payload=excluded.payload, source_seq=excluded.source_seq,'
            ' rule_version=excluded.rule_version',
            (version_id, key, canonical_json(parsed), source_seq, rule_version),
        )

    def _insert_quarantine(self, run_id, phase, name, rule_version, seq, page,
                           digest, kind, reason):
        quarantine_id = f'q-{uuid.uuid4().hex[:12]}'
        self.connection.execute(
            'INSERT INTO ro_quarantine'
            '(quarantine_id, run_id, phase, rule_name, rule_version, source_seq,'
            ' page_id, digest, kind, reason, payload, created_at)'
            ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (quarantine_id, run_id, phase, name, rule_version, seq,
             str(page.get('id')) if isinstance(page, dict) else None,
             digest, kind.value if hasattr(kind, 'value') else str(kind),
             reason, canonical_json(page), time.time()),
        )
        return quarantine_id

    # ------------------------------------------------------------- 重放运行
    def start_replay(self, name, version, run_params=None):
        """冻结一个重放范围：当前归档全集 + 规则摘要 + 字段契约 + 运行参数。

        冻结后再 archive 的响应 seq 大于 frozen_high_seq，重放永不读取，
        从机制上保证运行期间范围不会偷偷扩大。
        """
        release = self.registry.release(name, version)
        self.register_rule(release)
        frozen_high_seq = self.current_high_seq()
        frozen_seqs = [
            row[0] for row in self.connection.execute(
                'SELECT seq FROM ro_raw_archive WHERE seq<=? ORDER BY seq',
                (frozen_high_seq,))
        ]
        bundle = {
            'rule': {'name': name, 'version': version, 'code_digest': release.code_digest},
            'frozen_seqs': frozen_seqs,
            'frozen_high_seq': frozen_high_seq,
            'contract': {
                'input_fields': sorted(release.input_fields),
                'output_fields': sorted(release.output_fields),
                'accepted_units': sorted(release.accepted_units),
            },
            'run_params': run_params or {},
        }
        bundle_digest = digest_json(bundle)
        run_id = f'run-{uuid.uuid4().hex[:12]}'
        with self.transaction():
            self.connection.execute(
                'INSERT INTO ro_replay_runs'
                '(run_id, rule_name, rule_version, bundle_digest, frozen_high_seq,'
                ' run_params, contract_snapshot, status, created_at)'
                ' VALUES (?,?,?,?,?,?,?,?,?)',
                (run_id, name, version, bundle_digest, frozen_high_seq,
                 canonical_json(run_params or {}),
                 canonical_json(bundle['contract']), 'running', time.time()),
            )
            self.connection.executemany(
                'INSERT INTO ro_replay_items(run_id, seq, state) VALUES (?,?,?)',
                [(run_id, seq, 'pending') for seq in frozen_seqs],
            )
            self._log_event('replay_started',
                            {'run_id': run_id, 'bundle_digest': bundle_digest,
                             'frozen_high_seq': frozen_high_seq, 'items': len(frozen_seqs)})
        return {'run_id': run_id, 'bundle': bundle, 'bundle_digest': bundle_digest,
                'frozen_seqs': frozen_seqs}

    def run_state(self, run_id):
        row = self.connection.execute(
            'SELECT * FROM ro_replay_runs WHERE run_id=?', (run_id,)
        ).fetchone()
        if row is None:
            raise ReplayError(f'重放任务不存在: {run_id}')
        counts = dict(self.connection.execute(
            'SELECT state, count(*) FROM ro_replay_items WHERE run_id=? GROUP BY state',
            (run_id,)).fetchall())
        return {
            'run_id': run_id,
            'status': row['status'],
            'failure_reason': row['failure_reason'],
            'frozen_high_seq': row['frozen_high_seq'],
            'bundle_digest': row['bundle_digest'],
            'counts': {state: counts.get(state, 0)
                       for state in ('pending', 'done', 'quarantined')},
        }

    def pending_items(self, run_id):
        self._assert_running(run_id)
        return [row[0] for row in self.connection.execute(
            "SELECT seq FROM ro_replay_items WHERE run_id=? AND state='pending' ORDER BY seq",
            (run_id,))]

    def _assert_running(self, run_id):
        row = self.connection.execute(
            'SELECT status, failure_reason FROM ro_replay_runs WHERE run_id=?',
            (run_id,)).fetchone()
        if row is None:
            raise ReplayError(f'重放任务不存在: {run_id}')
        if row['status'] == 'cancelled':
            raise ReplayError('重放任务已取消，不能继续处理')
        if row['status'] == 'failed':
            raise ReplayError(f"重放任务已失败: {row['failure_reason']}")
        if row['status'] != 'running':
            raise ReplayError(f"重放任务状态为 {row['status']}，不能继续处理")

    def process_one(self, run_id, seq):
        """处理冻结清单中的一条；每条独立事务，崩溃后可断点续跑。

        成功写影子行，坏页/字段不兼容写隔离区；除 RuleError 外的意外异常
        会在独立事务里把任务标记为 failed 并保留失败原因，任务从此不可提升。
        """
        try:
            with self.transaction():
                return self._process_one_locked(run_id, seq)
        except RuleError:
            raise
        except ReplayError:
            raise
        except Exception as error:  # noqa: BLE001 - 任何意外都要冻结任务状态
            reason = f'{type(error).__name__}: {error}'
            with self.transaction():
                row = self.connection.execute(
                    'SELECT status FROM ro_replay_runs WHERE run_id=?',
                    (run_id,)).fetchone()
                if row is not None and row['status'] == 'running':
                    self.connection.execute(
                        "UPDATE ro_replay_runs SET status='failed', failure_reason=?,"
                        ' finished_at=? WHERE run_id=?',
                        (reason, time.time(), run_id))
                    self._log_event('replay_failed',
                                    {'run_id': run_id, 'reason': reason})
            raise

    def _process_one_locked(self, run_id, seq):
        self._assert_running(run_id)
        item = self.connection.execute(
            'SELECT state FROM ro_replay_items WHERE run_id=? AND seq=?',
            (run_id, seq)).fetchone()
        if item is None:
            raise ReplayError(f'seq={seq} 不在重放冻结清单内，拒绝扩大范围')
        if item['state'] != 'pending':
            return {'run_id': run_id, 'seq': seq, 'state': item['state'], 'skipped': True}
        run = self.connection.execute(
            'SELECT rule_name, rule_version, frozen_high_seq FROM ro_replay_runs'
            ' WHERE run_id=?', (run_id,)).fetchone()
        if seq > run['frozen_high_seq']:
            # 双保险：即使清单被人为污染，也绝不读取冻结点之外的响应。
            raise ReplayError('seq 超过冻结上界，拒绝扩大重放范围')
        name, rule_version = run['rule_name'], run['rule_version']
        page, digest = self._load_page(seq)
        shadow_id = self._shadow_version_id(run_id)
        self._ensure_shadow_version(run_id, name, rule_version)
        try:
            parsed = self.registry.parse(name, rule_version, page)
        except RuleError as error:
            kind, reason = classify_parse_error(
                self.registry.release(name, rule_version), error)
            quarantine_id = self._insert_quarantine(
                run_id, 'replay', name, rule_version, seq, page, digest, kind, reason)
            self.connection.execute(
                "UPDATE ro_replay_items SET state='quarantined', quarantine_id=?"
                ' WHERE run_id=? AND seq=?',
                (quarantine_id, run_id, seq))
            self._log_event('replay_quarantined',
                            {'run_id': run_id, 'seq': seq, 'kind': kind.value,
                             'reason': reason, 'quarantine_id': quarantine_id})
            return {'run_id': run_id, 'seq': seq, 'state': 'quarantined',
                    'kind': kind.value, 'reason': reason,
                    'quarantine_id': quarantine_id}
        self._upsert_row(shadow_id, parsed, seq, rule_version)
        self.connection.execute(
            "UPDATE ro_replay_items SET state='done' WHERE run_id=? AND seq=?",
            (run_id, seq))
        self._log_event('replay_shadow_written',
                        {'run_id': run_id, 'seq': seq, 'key': parsed['key']})
        return {'run_id': run_id, 'seq': seq, 'state': 'done',
                'key': parsed['key'], 'shadow_version_id': shadow_id}

    def run_all(self, run_id):
        """续跑直到清单处理完；崩溃/取消后再次调用会跳过已完成项。"""
        results = []
        while True:
            pending = self.pending_items(run_id)
            if not pending:
                break
            for seq in pending:
                results.append(self.process_one(run_id, seq))
        return results

    def fail_run(self, run_id, reason):
        """显式把任务标记为失败（演示中断场景用）。"""
        with self.transaction():
            self._assert_running(run_id)
            self.connection.execute(
                "UPDATE ro_replay_runs SET status='failed', failure_reason=?,"
                ' finished_at=? WHERE run_id=?',
                (reason, time.time(), run_id))
            self._log_event('replay_failed', {'run_id': run_id, 'reason': reason})

    def cancel_run(self, run_id):
        """主动取消：影子版本保持不可见，当前版本一行不动。"""
        with self.transaction():
            self._assert_running(run_id)
            self.connection.execute(
                "UPDATE ro_replay_runs SET status='cancelled', finished_at=?"
                ' WHERE run_id=?',
                (time.time(), run_id))
            shadow_id = self._shadow_version_id(run_id)
            self.connection.execute(
                "UPDATE ro_versions SET status='cancelled' WHERE version_id=?",
                (shadow_id,))
            self._log_event('replay_cancelled', {'run_id': run_id})

    def _shadow_version_id(self, run_id):
        return f'shadow-{run_id}'

    def _ensure_shadow_version(self, run_id, name, rule_version):
        shadow_id = self._shadow_version_id(run_id)
        self.connection.execute(
            'INSERT OR IGNORE INTO ro_versions'
            '(version_id, rule_name, rule_version, parent_version_id, status,'
            ' created_at, bundle_digest, run_id)'
            ' VALUES (?,?,?,?,?,?,?,?)',
            (shadow_id, name, rule_version, self.active_version(), 'shadow',
             time.time(),
             self.connection.execute(
                 'SELECT bundle_digest FROM ro_replay_runs WHERE run_id=?',
                 (run_id,)).fetchone()[0],
             run_id))

    # ------------------------------------------------------------- 差异检查
    def diff_shadow(self, run_id):
        """业务检查：对比影子结果与当前可见版本，按键分类呈现差异。"""
        shadow_id = self._shadow_version_id(run_id)
        active_id = self.active_version()
        shadow = {row['record_key']: json.loads(row['payload'])
                  for row in self.connection.execute(
                      'SELECT record_key, payload FROM ro_version_rows WHERE version_id=?',
                      (shadow_id,))}
        current = {row['record_key']: json.loads(row['payload'])
                   for row in self.connection.execute(
                       'SELECT record_key, payload FROM ro_version_rows WHERE version_id=?',
                       (active_id,))} if active_id else {}
        changed, added, unchanged = [], [], []
        for key, payload in sorted(shadow.items()):
            if key not in current:
                added.append(key)
            elif canonical_json(current[key]) != canonical_json(payload):
                changed.append({'key': key, 'before': current[key], 'after': payload})
            else:
                unchanged.append(key)
        removed = sorted(set(current) - set(shadow))
        quarantined = [dict(row) for row in self.connection.execute(
            'SELECT page_id, kind, reason, source_seq FROM ro_quarantine WHERE run_id=?',
            (run_id,))]
        return {'changed': changed, 'added': added, 'removed': removed,
                'unchanged': unchanged, 'quarantined': quarantined}

    # ------------------------------------------------------------- 发布切换
    def promote(self, run_id, under_lock=None):
        """影子版本通过检查后原子切换为可见版本，含冻结点之后的追赶。

        整个切换在一个 IMMEDIATE 写事务内完成，发布期间正常采集不停止：
        并发采集事务先在写锁上排队，本事务拿到锁后重新捕获临界水位，
        因此排队期间提交的响应（发布临界点到达的响应）必然落在
        (冻结上界, 临界水位] 区间，被同事务按新版本规则追赶，不丢不重。

        under_lock: 可选回调，在已持有写锁、捕获临界水位之后、提交之前执行。
        回调返回的页面直接在同一事务内归档并追赶（便于确定性测试与演示）。
        """
        with self.transaction():
            run = self.connection.execute(
                'SELECT * FROM ro_replay_runs WHERE run_id=?', (run_id,)
            ).fetchone()
            if run is None:
                raise ReplayError(f'重放任务不存在: {run_id}')
            shadow_status = self.connection.execute(
                'SELECT status FROM ro_versions WHERE version_id=?',
                (self._shadow_version_id(run_id),)).fetchone()
            # 回退后影子版本处于 superseded，允许重新发布；cancelled/failed 不允许。
            if run['status'] not in ('running', 'promoted') or shadow_status is None \
                    or shadow_status['status'] not in ('shadow', 'superseded'):
                raise ReplayError(
                    f"任务状态 {run['status']} 不允许发布（失败与取消的重放不可提升）")
            pending = self.connection.execute(
                "SELECT count(*) FROM ro_replay_items WHERE run_id=? AND state='pending'",
                (run_id,)).fetchone()[0]
            if pending:
                raise ReplayError(f'还有 {pending} 条冻结项未处理，不能发布')
            active_id = self.active_version()
            shadow_id = self._shadow_version_id(run_id)
            name, rule_version = run['rule_name'], run['rule_version']

            # 临界水位：此刻之后到达的响应由正常采集流按新版本写入。
            high_at_switch = self.current_high_seq()
            arrivals = []
            if under_lock is not None:
                arrivals = under_lock() or []
                for page in arrivals:
                    # 与正常采集同一归档路径；持锁期间它只能排在本事务之后可见。
                    encoded = canonical_json(page)
                    cursor = self.connection.execute(
                        'INSERT INTO ro_raw_archive'
                        '(page_id, payload, digest, arrived_at) VALUES (?,?,?,?)',
                        (str(page.get('id')), encoded, digest_json(page), time.time()))
                    seq = cursor.lastrowid
                    high_at_switch = max(high_at_switch, seq)
                    self._log_event('raw_archived_at_switch',
                                    {'seq': seq, 'page_id': page.get('id')})

            # 追赶：冻结点之后（含临界时刻）到达的响应，用新版本规则补齐。
            catchup_seqs = [row[0] for row in self.connection.execute(
                'SELECT seq FROM ro_raw_archive WHERE seq>? AND seq<=? ORDER BY seq',
                (run['frozen_high_seq'], high_at_switch))]
            catchup = []
            for seq in catchup_seqs:
                page, digest = self._load_page(seq)
                try:
                    parsed = self.registry.parse(name, rule_version, page)
                except RuleError as error:
                    kind, reason = classify_parse_error(
                        self.registry.release(name, rule_version), error)
                    quarantine_id = self._insert_quarantine(
                        run_id, 'catchup', name, rule_version, seq, page, digest,
                        kind, reason)
                    catchup.append({'seq': seq, 'quarantined': True, 'reason': reason,
                                    'quarantine_id': quarantine_id})
                    continue
                self._upsert_row(shadow_id, parsed, seq, rule_version)
                catchup.append({'seq': seq, 'key': parsed['key']})

            self.connection.execute(
                "UPDATE ro_versions SET status='superseded', switched_at=? WHERE version_id=?",
                (time.time(), active_id))
            self.connection.execute(
                "UPDATE ro_versions SET status='active', switched_at=?,"
                ' switch_watermark=?, parent_version_id=? WHERE version_id=?',
                (time.time(), high_at_switch, active_id, shadow_id))
            self.connection.execute(
                "UPDATE ro_replay_runs SET status='promoted', finished_at=? WHERE run_id=?",
                (time.time(), run_id))
            self._log_event('version_promoted',
                            {'run_id': run_id, 'shadow_version_id': shadow_id,
                             'parent_version_id': active_id,
                             'switch_watermark': high_at_switch,
                             'catchup': catchup,
                             'critical_arrivals': [p.get('id') for p in arrivals]})
        return {'active_version_id': shadow_id, 'parent_version_id': active_id,
                'switch_watermark': high_at_switch, 'catchup': catchup}

    # ------------------------------------------------------------- 回退
    def rollback(self, target_version_id=None):
        """回退到旧版本：只翻转指针，新版本数据与证据全部保留，可再次发布。"""
        active_id = self.active_version()
        if active_id is None:
            raise ReplayError('没有可回退的可见版本')
        if target_version_id is None:
            row = self.connection.execute(
                'SELECT parent_version_id FROM ro_versions WHERE version_id=?',
                (active_id,)).fetchone()
            target_version_id = row['parent_version_id']
        if not target_version_id:
            raise ReplayError('当前版本没有父版本，无可回退目标')
        target = self._get_version(target_version_id)
        if target['status'] not in ('superseded', 'active'):
            raise ReplayError(f"目标版本状态 {target['status']} 不允许回退")
        with self.transaction():
            self.connection.execute(
                "UPDATE ro_versions SET status='superseded' WHERE version_id=?",
                (active_id,))
            self.connection.execute(
                "UPDATE ro_versions SET status='active', switched_at=? WHERE version_id=?",
                (time.time(), target_version_id))
            self._log_event('version_rolled_back',
                            {'from_version_id': active_id,
                             'to_version_id': target_version_id})
        return {'active_version_id': target_version_id,
                'rolled_back_from': active_id}

    # ------------------------------------------------------------- 查询与追溯
    def query_active(self):
        """查询服务使用：永远读当前可见版本，重放期间结果不受任何影响。"""
        active_id = self.active_version()
        if active_id is None:
            return []
        return [json.loads(row['payload']) for row in self.connection.execute(
            'SELECT payload FROM ro_version_rows WHERE version_id=? ORDER BY record_key',
            (active_id,))]

    def query_version(self, version_id):
        return [json.loads(row['payload']) for row in self.connection.execute(
            'SELECT payload FROM ro_version_rows WHERE version_id=? ORDER BY record_key',
            (version_id,))]

    def trace(self, key):
        """追溯一条当前可见记录：原始证据、规则版本、来源 seq 与完整事件链。"""
        active_id = self.active_version()
        row = self.connection.execute(
            'SELECT * FROM ro_version_rows WHERE version_id=? AND record_key=?',
            (active_id, key)).fetchone()
        if row is None:
            return None
        raw = self.connection.execute(
            'SELECT seq, page_id, payload, digest, arrived_at FROM ro_raw_archive'
            ' WHERE seq=?', (row['source_seq'],)).fetchone()
        version = self._get_version(active_id)
        release = self.registry.release(version['rule_name'], row['rule_version'])
        return {
            'key': key,
            'output': json.loads(row['payload']),
            'rule': {'name': version['rule_name'], 'version': row['rule_version'],
                     'code_digest': release.code_digest,
                     'output_fields': sorted(release.output_fields)},
            'version_id': active_id,
            'source': {'seq': raw['seq'], 'page_id': raw['page_id'],
                       'digest': raw['digest'], 'raw_payload': json.loads(raw['payload']),
                       'arrived_at': raw['arrived_at']},
        }

    def quarantine_list(self, run_id=None):
        sql = ('SELECT quarantine_id, run_id, phase, rule_name, rule_version,'
               ' source_seq, page_id, kind, reason, payload, created_at'
               ' FROM ro_quarantine')
        params = ()
        if run_id is not None:
            sql += ' WHERE run_id=?'
            params = (run_id,)
        sql += ' ORDER BY created_at'
        result = []
        for row in self.connection.execute(sql, params):
            item = dict(row)
            item['payload'] = json.loads(item['payload'])
            result.append(item)
        return result

    def events(self):
        return [dict(row) for row in self.connection.execute(
            'SELECT event_id, at, kind, payload FROM ro_events ORDER BY event_id')]

    def manifest(self, run_id):
        """返回重放冻结的完整清单（响应集合、规则摘要、契约、运行参数）。"""
        run = self.connection.execute(
            'SELECT * FROM ro_replay_runs WHERE run_id=?', (run_id,)).fetchone()
        if run is None:
            raise ReplayError(f'重放任务不存在: {run_id}')
        seqs = [row[0] for row in self.connection.execute(
            'SELECT seq FROM ro_replay_items WHERE run_id=? ORDER BY seq', (run_id,))]
        pages = []
        for seq in seqs:
            raw = self.connection.execute(
                'SELECT page_id, digest, payload FROM ro_raw_archive WHERE seq=?',
                (seq,)).fetchone()
            pages.append({'seq': seq, 'page_id': raw['page_id'],
                          'digest': raw['digest'], 'payload': json.loads(raw['payload'])})
        return {
            'run_id': run_id,
            'rule_name': run['rule_name'],
            'rule_version': run['rule_version'],
            'bundle_digest': run['bundle_digest'],
            'frozen_high_seq': run['frozen_high_seq'],
            'run_params': json.loads(run['run_params']),
            'contract_snapshot': json.loads(run['contract_snapshot']),
            'pages': pages,
        }

    # ----------------------------------------------------- 预演兼容检查（不入库重放）
    def preflight(self, name, version, pages, phase='preflight'):
        """对一批页面只做规则兼容检查，问题项直接进隔离区，不产生版本行。"""
        release = self.registry.release(name, version)
        self.register_rule(release)
        report = []
        with self.transaction():
            for page in pages:
                digest = digest_json(page)
                try:
                    self.registry.parse(name, version, page)
                    report.append({'page_id': page.get('id'), 'kind': 'compatible'})
                except RuleError as error:
                    kind, reason = classify_parse_error(release, error)
                    quarantine_id = self._insert_quarantine(
                        None, phase, name, version, None, page, digest, kind, reason)
                    report.append({'page_id': page.get('id'), 'kind': kind.value,
                                   'reason': reason, 'quarantine_id': quarantine_id})
            self._log_event('preflight_completed',
                            {'rule': f'{name} v{version}', 'report': report})
        return report
