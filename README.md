# 采集解析规则·可回退发布

面向合作资料接收人员的 Python 采集组件。登记不可覆盖的规则版本，以固定测值契约解析响应并保存原始摘要；
在正常采集不停止的前提下，支持受控的规则发布、历史重放、影子校验、原子切换与一键回退。
输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。

## 安装与运行

使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供，无需安装第三方包。
从项目根目录执行；演示启动真实的回环 HTTP 来源、处理响应、事务入库后读取实际结果。
每次演示使用独立临时目录，结束后清理；通常两秒内结束。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
python3 -m collector rollout-demo
```

`rollout-demo` 使用 `fixtures/raw-responses.json` 与 `fixtures/contracts.json`，
依次展示：规则兼容检查（坏页/字段不兼容入隔离区）、重放中断后的断点续跑、
重放期间到达响应的范围隔离、取消不污染当前版本、发布临界点响应的锁竞争与追赶、
切换后回退的查询结果与逐条追溯。

常规导入保留在本地数据库中，可重复运行；默认路径已忽略，不会进入版本库。

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

查询服务监听回环地址，`GET /records` 返回数据库中的记录；按 Ctrl-C 停止。

## 两条写流的分界与追赶（发布期间不停采）

改版后两版字段口径（v1：`id/value/unit`；v2：`id/value_m`，单位归一为米）由
`collector/rules.py` 中两个不可覆盖的规则版本承载，注册时冻结源码摘要与字段契约。

* **正常采集流 `live_ingest`**：新响应先归档（`ro_raw_archive`，全局单调 `seq`），
  再按**当前可见版本**的规则解析并 upsert 到该版本自己的行表 `ro_version_rows`。
  它永远看不到影子版本，因此重放不可能覆盖正在被查询的行。
* **历史重放流 `start_replay/process_one/run_all`**：启动时冻结四项内容——
  原始响应集合（`frozen_high_seq` 与 seq 清单）、规则代码摘要、字段契约快照、运行参数，
  整体计算 `bundle_digest`。运行期间新到达的响应 `seq` 大于冻结上界，
  重放读取会被直接拒绝，范围不可能偷偷扩大；这些响应继续由正常采集流处理。
* **追赶与切换 `promote`**：业务方通过 `diff_shadow` 检查影子版本与当前版本的差异后，
  在一个 `BEGIN IMMEDIATE` 事务内重新捕获临界水位，把冻结点之后到达
  （含发布临界点上正在与写锁排队）的响应按新版本规则补齐处理，再原子翻转版本指针。
  采集全程不停止，只是在写锁上短暂排队；临界响应不丢、不重、不混口径。

## 隔离区、失败取消与回退

* 规则版本只能读取自己声明的输入字段；坏页（字段匹配但数据坏，如 `value="异常"`）
  与字段不兼容（缺字段、多字段、单位不在接受列表）分类进入 `ro_quarantine`，
  带原始载荷、摘要、失败原因、所处阶段（preflight/replay/catchup/live）。
* 每条重放项独立事务、状态为 pending/done/quarantined；进程崩溃后重开库即可断点续跑。
  意外错误把任务标记为 `failed` 并记录原因；`cancelled`/`failed` 的任务不可提升为版本，
  当前可见版本一行不动。
* 切换与回退都只是版本指针的原子更新：回退后旧口径查询立即恢复，
  新版本的影子行、隔离区与事件链全部保留，同一影子版本可再次发布。
* `trace(key)` 返回一条可见记录的完整证据链：原始响应载荷与摘要、来源 `seq`、
  所用规则版本与代码摘要、输出字段契约；`events()` 给出全程审计事件。
* 重开数据库时会重新核对规则代码摘要，摘要不一致（发布后偷改规则代码）直接拒绝打开。

## 组件与数据

`collector/domain.py` 负责来源格式和领域校验；`storage.py` 保留原始对象、导入凭据和当前记录。
单次导入在同一事务内提交，异常时整体回滚；相同来源和原文只登记一次凭据。
`collector/rules.py` 是不可覆盖的规则注册器与兼容检查器；
`collector/rollout.py` 是版本化重放/发布引擎（独立 `ro_*` 表，与既有导入表互不干扰）。
`transport.py` 提供请求客户端和本地响应服务；`api.py` 提供只读查询入口。
`fixtures/sample.json` 是当前导入器支持的输入；
`fixtures/raw-responses.json`、`fixtures/contracts.json` 是重放演示使用的合成页面与契约。
当前响应服务是顺序脚本工具，不自动推断认证、文件或表单业务；领域对象提供对应的本地交互。

## 可复现运行

宿主机或容器均不需要网络。镜像准备好以后可以关闭外部网络再运行上述命令。
仓库内不含真实账号、新闻正文或人员资料；不要将生产凭据写入样本或日志。
