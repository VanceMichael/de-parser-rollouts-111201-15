"""受控规则发布与历史重放的单元测试。

覆盖：规则不可覆盖与契约自检、兼容检查分类、冻结范围不扩大、
断点续跑（跨进程重开库）、失败/取消不污染当前版本、影子差异、
发布追赶与临界点响应、回退与追溯、代码摘要防篡改。
"""

import json
import tempfile
import unittest
from pathlib import Path

from collector.rollout import ReplayError, RolloutStore
from collector.rules import (
    CompatibilityKind,
    RuleError,
    RuleRegistry,
    RuleRelease,
    build_default_registry,
    check_compatibility,
    digest_bytes,
    digest_json,
    make_release,
    parse_measurement_v1,
    parse_measurement_v2,
)

FIXTURES = Path(__file__).resolve().parent.parent / 'fixtures'


def fixture_pages():
    return json.loads((FIXTURES / 'raw-responses.json').read_text(encoding='utf-8'))


class RuleTests(unittest.TestCase):
    def setUp(self):
        self.registry = build_default_registry()

    def test_two_contracts_parse_differently(self):
        page = {'id': 'r2', 'value': 2, 'unit': 'km'}
        self.assertEqual(
            self.registry.parse('measurement', 1, page),
            {'key': 'r2', 'value': '2', 'unit': 'km'})
        self.assertEqual(
            self.registry.parse('measurement', 2, page),
            {'key': 'r2', 'value_m': 2000.0})

    def test_registry_is_immutable(self):
        registry = build_default_registry()
        release = make_release('measurement', 1, parse_measurement_v1,
                               ('id', 'value', 'unit'), ('value', 'unit'))
        with self.assertRaises(ValueError):
            registry.register(release, parse_measurement_v1)

    def test_contract_lint_rejects_mismatched_rule(self):
        bad_release = RuleRelease(
            name='bad', version=1,
            input_fields=frozenset({'id', 'value', 'unit'}),
            output_fields=frozenset({'not_produced'}),
            code_digest=digest_bytes(b'x'))
        registry = RuleRegistry()
        with self.assertRaises(ValueError):
            registry.register(bad_release, parse_measurement_v1)

    def test_compatibility_classification(self):
        pages = fixture_pages()['responses']
        kinds = {
            page['id']: check_compatibility(self.registry, 'measurement', 2, page)[0]
            for page in pages
        }
        self.assertEqual(kinds['r1'], CompatibilityKind.COMPATIBLE)
        self.assertEqual(kinds['r2'], CompatibilityKind.COMPATIBLE)
        # r3：字段口径匹配但 value 非数值 —— 坏页
        self.assertEqual(kinds['r3'], CompatibilityKind.BAD_PAGE)
        # 改版后多出未声明字段 —— 字段不兼容，规则版本不能读取
        extra = {'id': 'x', 'value': 1, 'unit': 'm', 'source': 'new'}
        kind, detail = check_compatibility(self.registry, 'measurement', 2, extra)
        self.assertEqual(kind, CompatibilityKind.INCOMPATIBLE_FIELD)
        self.assertIn('未声明', detail)
        # 不接受的单位同样是字段口径不兼容
        kind, _ = check_compatibility(
            self.registry, 'measurement', 2, {'id': 'x', 'value': 1, 'unit': 'ft'})
        self.assertEqual(kind, CompatibilityKind.INCOMPATIBLE_FIELD)

    def test_v1_requires_exact_fields(self):
        with self.assertRaises(RuleError):
            parse_measurement_v1({'id': 'x', 'value': 1})
        with self.assertRaises(RuleError):
            parse_measurement_v2({'id': 'x', 'value': 'bad', 'unit': 'm'})


class RolloutTestBase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temporary.name) / 'rollout.sqlite3'
        self.store = RolloutStore(self.db_path, build_default_registry())
        self.store.bootstrap_version('measurement', 1)
        for page in fixture_pages()['responses']:
            self.store.live_ingest(page)

    def reopen(self):
        self.store.close()
        self.store = RolloutStore(self.db_path, build_default_registry())
        return self.store

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()


class FreezeAndReplayTests(RolloutTestBase):
    def test_replay_freezes_scope_and_ignores_late_arrivals(self):
        started = self.store.start_replay('measurement', 2, run_params={'k': 'v'})
        run_id, frozen = started['run_id'], started['bundle']['frozen_high_seq']
        # 运行期间新响应到达：归档成功，但不属于冻结清单
        late = self.store.live_ingest({'id': 'r4', 'value': 12, 'unit': 'm'})
        self.assertGreater(late['seq'], frozen)
        with self.assertRaises(ReplayError):
            self.store.process_one(run_id, late['seq'])
        manifest = self.store.manifest(run_id)
        self.assertEqual([p['page_id'] for p in manifest['pages']],
                         ['r1', 'r2', 'r3'])
        self.assertEqual(manifest['run_params'], {'k': 'v'})
        self.assertIn('value_m', manifest['contract_snapshot']['output_fields'])
        # 冻结摘要对相同内容稳定
        self.assertEqual(started['bundle_digest'],
                         self.store.manifest(run_id)['bundle_digest'])

    def test_resume_after_interruption_across_process_restart(self):
        started = self.store.start_replay('measurement', 2)
        run_id = started['run_id']
        self.store.process_one(run_id, started['frozen_seqs'][0])
        self.reopen()  # 模拟进程崩溃后重启
        state = self.store.run_state(run_id)
        self.assertEqual(state['counts']['done'], 1)
        self.assertEqual(state['counts']['pending'], 2)
        self.store.run_all(run_id)
        state = self.store.run_state(run_id)
        self.assertEqual(state['counts'], {'pending': 0, 'done': 2, 'quarantined': 1})
        # 重跑已完成项是空操作
        again = self.store.process_one(run_id, started['frozen_seqs'][0])
        self.assertTrue(again.get('skipped'))

    def test_bad_page_goes_to_quarantine_not_shadow(self):
        started = self.store.start_replay('measurement', 2)
        self.store.run_all(started['run_id'])
        quarantine = self.store.quarantine_list(started['run_id'])
        self.assertEqual(len(quarantine), 1)
        self.assertEqual(quarantine[0]['page_id'], 'r3')
        self.assertEqual(quarantine[0]['kind'], 'bad_page')
        diff = self.store.diff_shadow(started['run_id'])
        self.assertEqual([c['key'] for c in diff['changed']], ['r1', 'r2'])
        self.assertIn('r3', [q['page_id'] for q in diff['quarantined']])

    def test_failure_and_cancel_never_pollute_active_version(self):
        before = self.store.query_active()
        # 主动取消
        run_a = self.store.start_replay('measurement', 2)
        self.store.process_one(run_a['run_id'], run_a['frozen_seqs'][0])
        self.store.cancel_run(run_a['run_id'])
        with self.assertRaises(ReplayError):
            self.store.run_all(run_a['run_id'])
        # 显式失败
        run_b = self.store.start_replay('measurement', 2)
        self.store.fail_run(run_b['run_id'], '模拟解析服务不可用')
        with self.assertRaises(ReplayError):
            self.store.promote(run_b['run_id'])
        with self.assertRaises(ReplayError):
            self.store.promote(run_a['run_id'])
        self.assertEqual(self.store.query_active(), before)
        self.assertEqual(
            self.store.run_state(run_a['run_id'])['status'], 'cancelled')

    def test_unexpected_error_marks_run_failed_and_blocks_promotion(self):
        from collector.rules import RuleRegistry
        started = self.store.start_replay('measurement', 2)
        run_id = started['run_id']
        before = self.store.query_active()

        class ExplodingRegistry(RuleRegistry):
            def parse(self, name, version, page):
                raise RuntimeError('解析进程崩溃')

        self.store.registry = ExplodingRegistry()
        with self.assertRaises(RuntimeError):
            self.store.process_one(run_id, started['frozen_seqs'][0])
        self.assertEqual(self.store.run_state(run_id)['status'], 'failed')
        self.assertIn('解析进程崩溃',
                      self.store.run_state(run_id)['failure_reason'])
        with self.assertRaises(ReplayError):
            self.store.promote(run_id)
        # 当前可见版本一行未动
        self.assertEqual(self.store.query_active(), before)

    def test_shadow_is_invisible_while_replay_runs(self):
        started = self.store.start_replay('measurement', 2)
        self.store.run_all(started['run_id'])
        active = self.store.query_active()
        self.assertTrue(all('unit' in row for row in active))  # 仍是 v1
        shadow_id = f'shadow-{started["run_id"]}'
        shadow_rows = self.store.query_version(shadow_id)
        self.assertTrue(all('value_m' in row for row in shadow_rows))


class PromotionTests(RolloutTestBase):
    def _completed_run(self):
        started = self.store.start_replay('measurement', 2)
        self.store.run_all(started['run_id'])
        return started['run_id'], started['bundle']['frozen_high_seq']

    def test_promote_catches_up_arrivals_after_freeze(self):
        run_id, frozen = self._completed_run()
        # 重放冻结后到达的 r4，由正常采集流写在 v1
        r4 = self.store.live_ingest({'id': 'r4', 'value': 12, 'unit': 'm'})
        self.assertGreater(r4['seq'], frozen)
        result = self.store.promote(run_id)
        # 追赶把 r4 也按 v2 口径带进新版本
        self.assertEqual(result['switch_watermark'], r4['seq'])
        caught = {item['seq']: item for item in result['catchup']}
        self.assertEqual(caught[r4['seq']]['key'], 'r4')
        active = {row['key']: row for row in self.store.query_active()}
        self.assertEqual(active['r4'], {'key': 'r4', 'value_m': 12.0})
        self.assertEqual(active['r2']['value_m'], 2000.0)
        self.assertNotIn('unit', active['r1'])

    def test_critical_arrival_inside_promote_transaction(self):
        run_id, frozen = self._completed_run()
        # under_lock 在持锁临界区内归档 r5，模拟发布临界点到达的响应
        critical = [{'id': 'r5', 'value': 3, 'unit': 'km'}]
        result = self.store.promote(run_id, under_lock=lambda: critical)
        self.assertEqual(result['switch_watermark'],
                         self.store.current_high_seq())
        active = {row['key']: row for row in self.store.query_active()}
        self.assertEqual(active['r5']['value_m'], 3000.0)

    def test_catchup_quarantines_incompatible_critical_page(self):
        run_id, _ = self._completed_run()
        bad = [{'id': 'r6', 'value': 1, 'unit': 'ft'}]  # v2 不接受英尺
        self.store.promote(run_id, under_lock=lambda: bad)
        catchup_q = [q for q in self.store.quarantine_list(run_id)
                     if q['phase'] == 'catchup']
        self.assertEqual(len(catchup_q), 1)
        self.assertEqual(catchup_q[0]['page_id'], 'r6')
        self.assertEqual(catchup_q[0]['kind'], 'incompatible_field')

    def test_rollback_and_repromote(self):
        run_id, _ = self._completed_run()
        self.store.promote(run_id)
        v2_active = self.store.active_version()
        v2_rows = self.store.query_active()
        rolled = self.store.rollback()
        self.assertNotEqual(rolled['active_version_id'], v2_active)
        back = self.store.query_active()
        self.assertTrue(all('unit' in row for row in back))
        # 追溯仍可拿到原始证据与规则信息
        trace = self.store.trace('r2')
        self.assertEqual(trace['source']['raw_payload']['unit'], 'km')
        self.assertEqual(trace['rule']['version'], 1)
        # 重新发布
        again = self.store.promote(run_id)
        self.assertEqual(again['active_version_id'], v2_active)
        self.assertEqual(self.store.query_active(), v2_rows)

    def test_trace_after_promotion_records_rule_and_digest(self):
        run_id, _ = self._completed_run()
        self.store.promote(run_id)
        trace = self.store.trace('r1')
        self.assertEqual(trace['rule']['version'], 2)
        self.assertEqual(trace['rule']['code_digest'],
                         self.store.registry.release('measurement', 2).code_digest)
        self.assertEqual(trace['output']['value_m'], 10.0)
        self.assertEqual(trace['source']['page_id'], 'r1')

    def test_live_ingest_after_promotion_uses_new_rule(self):
        run_id, _ = self._completed_run()
        self.store.promote(run_id)
        result = self.store.live_ingest({'id': 'r7', 'value': 1, 'unit': 'km'})
        self.assertFalse(result['quarantined'])
        row = next(r for r in self.store.query_active() if r['key'] == 'r7')
        self.assertEqual(row['value_m'], 1000.0)

    def test_code_digest_tamper_is_rejected_on_reopen(self):
        self._completed_run()
        self.store.close()
        import sqlite3
        connection = sqlite3.connect(self.db_path)
        connection.execute(
            "UPDATE ro_rule_releases SET code_digest=? WHERE version=2",
            (digest_json({'tampered': True}),))
        connection.commit()
        connection.close()
        with self.assertRaises(RuntimeError):
            RolloutStore(self.db_path, build_default_registry())


class PreflightTests(RolloutTestBase):
    def test_preflight_quarantines_without_version_rows(self):
        before = len(self.store.query_active())
        report = self.store.preflight(
            'measurement', 2,
            [{'id': 'p1', 'value': 1, 'unit': 'm'},
             {'id': 'p2', 'value': 'oops', 'unit': 'm'},
             {'id': 'p3', 'value': 1, 'unit': 'm', 'extra': True}])
        self.assertEqual([r['kind'] for r in report],
                         ['compatible', 'bad_page', 'incompatible_field'])
        self.assertEqual(len(self.store.query_active()), before)
        pre = [q for q in self.store.quarantine_list() if q['phase'] == 'preflight']
        self.assertEqual(len(pre), 2)


if __name__ == '__main__':
    unittest.main()
