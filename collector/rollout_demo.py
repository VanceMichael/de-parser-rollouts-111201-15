"""端到端演示：受控规则发布与历史重放。

使用 fixtures/raw-responses.json 与 fixtures/contracts.json 中的合成响应，
完整展示：
  1. 规则兼容检查（坏页 / 字段不兼容进入隔离区）
  2. 重放中断后的断点续跑（关闭并重新打开数据库，模拟进程崩溃）
  3. 发布临界点到达的新响应（不停采，同事务追赶）
  4. 切换后回退，以及回退后的查询与逐条追溯

fixtures 路径以参数传入，脚本不依赖工作目录；仅使用标准库。
"""

import argparse
import copy
import json
import tempfile
import threading
from pathlib import Path

from .rollout import RolloutStore, ReplayError
from .rules import build_default_registry, check_compatibility, CompatibilityKind


def hr(title):
    print('\n' + '=' * 72)
    print(f' {title}')
    print('=' * 72)


def show(label, value):
    print(f'[{label}]')
    print(json.dumps(value, ensure_ascii=False, indent=2))


def load_fixtures(fixtures_dir):
    fixtures = Path(fixtures_dir)
    raw = json.loads((fixtures / 'raw-responses.json').read_text(encoding='utf-8'))
    contracts = json.loads((fixtures / 'contracts.json').read_text(encoding='utf-8'))
    return raw, contracts


def run_demo(fixtures_dir):
    raw_fixture, contracts = load_fixtures(fixtures_dir)
    registry = build_default_registry()

    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / 'rollout.sqlite3'
        store = RolloutStore(db_path, registry)

        # ------------------------------------------------------------- 0. 基线
        hr('0. 建立基线：v1 规则发布，历史响应经正常采集流进入可见版本')
        v1 = store.bootstrap_version('measurement', 1)
        print(f'初始可见版本: {v1}（measurement v1，契约字段 {contracts["v1"]["fields"]}）')
        for page in raw_fixture['responses']:
            result = store.live_ingest(copy.deepcopy(page))
            tag = ('隔离: ' + result['reason']) if result['quarantined'] \
                else f"入库 key={result['key']}"
            print(f"  采集 seq={result['seq']} id={page['id']!r}: {tag}")
        show('当前查询结果（v1 口径：有单位、value 保留原文）', store.query_active())
        baseline_high_seq = store.current_high_seq()
        print(f'当前归档水位 high_seq={baseline_high_seq}')

        # ------------------------------------------------- 1. 规则兼容检查
        hr('1. 规则兼容检查：v2 契约（归一为米 value_m）逐页预检')
        print('v2 声明输入字段: [id, value, unit]（规则只能读取自己声明的输入）')
        print(f"v2 输出契约字段: {contracts['v2']['fields']}，接受单位: {contracts['v2']['accepted_units']}")
        extra_page = {'id': 'rX', 'value': 5, 'unit': 'm', 'source': '改版后的额外字段'}
        preflight_pages = raw_fixture['responses'] + [extra_page]
        for page in preflight_pages:
            kind, detail = check_compatibility(registry, 'measurement', 2, page)
            mark = {
                CompatibilityKind.COMPATIBLE: '兼容',
                CompatibilityKind.BAD_PAGE: '坏页（字段口径匹配，数据本身坏）',
                CompatibilityKind.INCOMPATIBLE_FIELD: '字段不兼容（规则只读取声明输入）',
            }[kind]
            line = f"  id={page.get('id')!r}: {mark}"
            if detail:
                line += f' —— {detail}'
            print(line)
        report = store.preflight('measurement', 2, [extra_page], phase='preflight')
        show('预检落隔离区（不产生任何版本行）', report)

        # ----------------------------------------------- 2. 冻结重放 + 中断续跑
        hr('2. 启动 v2 历史重放：冻结响应集合/规则摘要/契约/参数，随后中断续跑')
        started = store.start_replay(
            'measurement', 2,
            run_params={'purpose': '单位口径统一为米', 'operator': 'demo'})
        run_id = started['run_id']
        show('冻结清单 manifest（含 bundle_digest）', {
            'run_id': run_id,
            'bundle_digest': started['bundle_digest'],
            'frozen_high_seq': started['bundle']['frozen_high_seq'],
            'frozen_seqs': started['frozen_seqs'],
            'contract': started['bundle']['contract'],
            'rule_code_digest': started['bundle']['rule']['code_digest'],
        })

        print('\n-- 中断点 1: after_shadow_write，只处理第一条后模拟进程崩溃 --')
        first = store.process_one(run_id, started['frozen_seqs'][0])
        show('第一条处理结果（写入影子版本，查询不可见）', first)
        print('当前可见版本查询依旧是 v1 结果:',
              [row['key'] for row in store.query_active()])
        store.close()

        print('\n-- 重新打开数据库（新进程），规则摘要被重新核对，从断点续跑 --')
        store = RolloutStore(db_path, build_default_registry())
        state_before = store.run_state(run_id)
        show('恢复后的运行状态', state_before)
        remaining = store.pending_items(run_id)
        print(f'待处理项（已完成的 r1 不在其中）: {remaining}')
        results = store.run_all(run_id)
        for item in results:
            print(f"  seq={item['seq']} -> {item['state']}"
                  + (f"（{item['kind']}: {item['reason']}）"
                     if item['state'] == 'quarantined' else f" key={item['key']}"))
        show('续跑完成后的运行状态', store.run_state(run_id))
        show('影子与当前版本的业务差异 diff', store.diff_shadow(run_id))

        # ----------------------------- 范围不扩大：重放期间到达的响应被排除
        hr('2b. 重放运行期间有新响应到达：范围不偷偷扩大，走正常采集流')
        arrival = raw_fixture['arrivals_during_replay'][0]
        live_result = store.live_ingest(copy.deepcopy(arrival))
        show('运行期间到达的 r4 经正常采集流写入当前 v1 版本', live_result)
        print(f'重放冻结上界仍为 {started["bundle"]["frozen_high_seq"]}，'
              f'当前归档水位 {store.current_high_seq()}；r4 不在重放清单内。')
        try:
            store.process_one(run_id, live_result['seq'])
        except ReplayError as error:
            print(f'  重放拒绝读取冻结点之外的 seq: {error}')

        # ----------------------------------------------- 3. 取消不污染当前版本
        hr('3. 主动取消一次重放：失败与取消都不污染当前可见版本')
        cancelled = store.start_replay('measurement', 2, run_params={'purpose': '演示取消'})
        store.cancel_run(cancelled['run_id'])
        show('被取消的任务状态', store.run_state(cancelled['run_id']))
        try:
            store.run_all(cancelled['run_id'])
        except ReplayError as error:
            print(f'  取消后续跑被拒绝: {error}')
        print('当前可见版本查询不变:',
              [row['key'] for row in store.query_active()])

        # ----------------------------------------- 4. 发布 + 临界点响应 + 追赶
        hr('4. 发布临界点到达的新响应：不停采，锁竞争后重新捕获水位并追赶')
        critical_page = {'id': 'r5', 'value': 3, 'unit': 'km'}
        # 用第二个连接模拟独立的采集进程/线程，与发布事务真实竞争写锁。
        collector_store = RolloutStore(db_path, build_default_registry())
        lock_acquired = threading.Event()
        release_lock = threading.Event()
        collector_done = threading.Event()
        collector_result = {}

        def concurrent_collector():
            # 在一个显式写锁事务内完成 r5 的归档与入库，模拟发布临界点
            # 正好有正常采集写入；promote 的 IMMEDIATE 事务在锁外排队。
            with collector_store.write_lock():
                lock_acquired.set()
                release_lock.wait(timeout=10)
                collector_result.update(
                    collector_store._live_ingest_locked(copy.deepcopy(critical_page)))
            collector_done.set()

        thread = threading.Thread(target=concurrent_collector)
        thread.start()
        lock_acquired.wait(timeout=10)
        print('  [采集线程] 已持有写锁，临界响应 r5 正在临界点上完成归档')
        release_lock.set()

        # promote 的 IMMEDIATE 事务先排队等锁，拿到锁后重新捕获水位，
        # 因此 r5 落入 (冻结上界, 临界水位]，被同一发布事务追赶，不丢不重。
        promotion = store.promote(run_id)
        thread.join(timeout=10)
        collector_done.wait(timeout=10)
        collector_store.close()
        print('  [采集线程] 采集未停止，r5 处理结果:',
              json.dumps(collector_result, ensure_ascii=False))
        show('切换结果（含 switch_watermark 与追赶明细）', promotion)
        show('切换后查询结果（v2 口径：统一 value_m，无单位列）', store.query_active())

        # 验证 r4/r5 的去重与追赶：r4 冻结点之后到达，r5 临界点到达
        show('隔离区（r3 坏页 + 预检的字段不兼容页）',
             [{'page_id': q['page_id'], 'phase': q['phase'],
               'kind': q['kind'], 'reason': q['reason']}
              for q in store.quarantine_list()])

        # ------------------------------------------------------- 5. 回退
        hr('5. 切换后发现问题：一键回退，证据与失败原因全部保留')
        rollback = store.rollback()
        show('回退结果', rollback)
        show('回退后的查询结果（恢复 v1 口径，单位列回来）', store.query_active())
        trace_r2 = store.trace('r2')
        show('追溯 r2：原始证据 + 使用的规则版本/代码摘要', trace_r2)
        show('版本清单（新版本数据仍保留，可再次发布）', [
            {'version_id': row['version_id'], 'rule_version': row['rule_version'],
             'status': row['status'], 'switch_watermark': row['switch_watermark']}
            for row in store.connection.execute(
                'SELECT version_id, rule_version, status, switch_watermark'
                ' FROM ro_versions ORDER BY created_at')])

        # 回退后再发布（指针翻转可逆）
        again = store.promote(run_id)
        print('\n回退后重新发布同一影子版本:', json.dumps(again, ensure_ascii=False))
        show('再次切换后的查询结果', store.query_active())

        hr('6. 事件审计链（归档/重放/隔离/切换/回退全程可追溯）')
        for event in store.events():
            print(f"  #{event['event_id']:>2} {event['kind']}: {event['payload']}")
        store.close()
        print('\n演示结束，临时数据库已清理。')


def main():
    parser = argparse.ArgumentParser(description='规则发布与历史重放端到端演示')
    parser.add_argument('--fixtures', default='fixtures',
                        help='fixtures 目录路径（默认 fixtures）')
    arguments = parser.parse_args()
    run_demo(arguments.fixtures)


if __name__ == '__main__':
    main()
