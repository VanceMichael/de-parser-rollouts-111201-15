"""受控规则发布：不可覆盖的规则版本、代码摘要与字段兼容检查。

每个规则版本显式声明自己能读取的输入字段和产出的字段契约；
规则注册后不可覆盖，代码摘要（digest）在注册时冻结并写入发布记录，
使重放可以锁定"当时的规则代码"，而不是引用一个可能被修改的活对象。
"""

import hashlib
import inspect
import json
from dataclasses import dataclass, field
from enum import Enum


class RuleError(ValueError):
    """规则解析阶段的可预期错误，对应隔离区中的 bad_page / incompatible_field。"""


class CompatibilityKind(str, Enum):
    COMPATIBLE = 'compatible'
    BAD_PAGE = 'bad_page'
    INCOMPATIBLE_FIELD = 'incompatible_field'


def canonical_json(obj):
    """排序键、无空白的确定性 JSON，用于摘要与冻结清单。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def digest_bytes(raw):
    return hashlib.sha256(raw).hexdigest()


def digest_json(obj):
    return digest_bytes(canonical_json(obj).encode('utf-8'))


# ---------------------------------------------------------------------------
# 两版规则：对应 fixtures/contracts.json 中的 v1 / v2 字段口径
# ---------------------------------------------------------------------------

def parse_measurement_v1(page):
    """v1 口径：保留原始 value 字符串与单位，字段集合必须恰好为 id/value/unit。"""
    if not isinstance(page, dict):
        raise RuleError('页面不是 JSON 对象')
    if set(page) != {'id', 'value', 'unit'}:
        missing = {'id', 'value', 'unit'} - set(page)
        extra = set(page) - {'id', 'value', 'unit'}
        detail = []
        if missing:
            detail.append(f"缺少字段 {sorted(missing)}")
        if extra:
            detail.append(f"存在未声明字段 {sorted(extra)}")
        raise RuleError('；'.join(detail) or '字段不匹配')
    key = page['id']
    if not isinstance(key, str) or not key:
        raise RuleError('id 必须是非空字符串')
    if not isinstance(page['unit'], str) or not page['unit']:
        raise RuleError('unit 必须是非空字符串')
    return {
        'key': key,
        'value': str(page['value']),
        'unit': page['unit'],
    }


_UNIT_FACTOR = {'m': 1.0, 'km': 1000.0}


def parse_measurement_v2(page):
    """v2 口径：统一归一到米，输出 value_m，只读取自己声明的输入字段。

    坏页（value 非数值）与字段不兼容（缺字段/多字段/单位不在接受列表）
    走不同的错误分类，便于隔离区分别呈现。
    """
    if not isinstance(page, dict):
        raise RuleError('页面不是 JSON 对象')
    declared = {'id', 'value', 'unit'}
    if set(page) != declared:
        missing = declared - set(page)
        extra = set(page) - declared
        detail = []
        if missing:
            detail.append(f"缺少字段 {sorted(missing)}")
        if extra:
            detail.append(f"存在未声明字段 {sorted(extra)}")
        raise RuleError('；'.join(detail))
    key = page['id']
    if not isinstance(key, str) or not key:
        raise RuleError('id 必须是非空字符串')
    unit = page['unit']
    if unit not in _UNIT_FACTOR:
        raise RuleError(f"单位 {unit!r} 不在接受列表 {sorted(_UNIT_FACTOR)} 内")
    try:
        number = float(page['value'])
    except (TypeError, ValueError):
        raise RuleError(f"value {page['value']!r} 无法解析为数值")
    return {
        'key': key,
        'value_m': number * _UNIT_FACTOR[unit],
    }


@dataclass(frozen=True)
class RuleRelease:
    """一次不可变的规则发布。"""

    name: str
    version: int
    input_fields: frozenset
    output_fields: frozenset
    accepted_units: frozenset = frozenset()
    code_digest: str = field(default='')
    source_hint: str = field(default='')

    def to_row(self):
        return {
            'name': self.name,
            'version': self.version,
            'input_fields': sorted(self.input_fields),
            'output_fields': sorted(self.output_fields),
            'accepted_units': sorted(self.accepted_units),
            'code_digest': self.code_digest,
            'source_hint': self.source_hint,
        }


class RuleRegistry:
    """不可覆盖的规则注册器：同名同版本重复登记直接拒绝。"""

    def __init__(self):
        self._rules = {}
        self._releases = {}

    def register(self, release, func):
        key = (release.name, release.version)
        if key in self._rules:
            raise ValueError(f'规则版本已登记: {release.name} v{release.version}')
        # 注册时做一次静态自检：函数对样例输出的字段必须与声明契约一致。
        self._lint_contract(release, func)
        self._rules[key] = func
        self._releases[key] = release

    @staticmethod
    def _lint_contract(release, func):
        samples = [
            {'id': 'lint', 'value': 1, 'unit': 'm'},
            {'id': 'lint', 'value': 2, 'unit': 'km'},
        ]
        for sample in samples:
            try:
                output = func(sample)
            except RuleError:
                continue
            produced = set(output) - {'key'}
            if produced != set(release.output_fields):
                raise ValueError(
                    f"规则 {release.name} v{release.version} 产出 {sorted(produced)} "
                    f"与声明契约 {sorted(release.output_fields)} 不一致"
                )

    def release(self, name, version):
        try:
            return self._releases[(name, version)]
        except KeyError:
            raise KeyError(f'未登记的规则版本: {name} v{version}')

    def func(self, name, version):
        try:
            return self._rules[(name, version)]
        except KeyError:
            raise KeyError(f'未登记的规则版本: {name} v{version}')

    def parse(self, name, version, page):
        return self.func(name, version)(page)

    def versions(self):
        return sorted(self._releases.values(), key=lambda item: (item.name, item.version))


def make_release(name, version, func, input_fields, output_fields, accepted_units=()):
    """根据函数源码构造带代码摘要的不可变发布。"""
    source = inspect.getsource(func)
    return RuleRelease(
        name=name,
        version=version,
        input_fields=frozenset(input_fields),
        output_fields=frozenset(output_fields),
        accepted_units=frozenset(accepted_units),
        code_digest=digest_bytes(source.encode('utf-8')),
        source_hint=func.__name__,
    )


def check_compatibility(registry, name, version, page):
    """对单页做规则兼容检查，不依赖数据库、不产生副作用。

    返回 (kind, detail)：
      compatible          —— 页面能被该规则版本解析；
      bad_page            —— 字段口径匹配但页内数据本身坏（如 value 非数值）；
      incompatible_field  —— 页面字段集合与规则声明输入不一致（单位不接受也归入此类）。
    """
    release = registry.release(name, version)
    if not isinstance(page, dict):
        return CompatibilityKind.INCOMPATIBLE_FIELD, '页面不是 JSON 对象'
    fields = set(page)
    if fields != set(release.input_fields):
        missing = set(release.input_fields) - fields
        extra = fields - set(release.input_fields)
        detail = []
        if missing:
            detail.append(f"缺少声明输入 {sorted(missing)}")
        if extra:
            detail.append(f"出现规则未声明的输入 {sorted(extra)}")
        return CompatibilityKind.INCOMPATIBLE_FIELD, '；'.join(detail)
    unit = page.get('unit')
    if release.accepted_units and unit not in release.accepted_units:
        return CompatibilityKind.INCOMPATIBLE_FIELD, (
            f"单位 {unit!r} 不在规则接受列表 {sorted(release.accepted_units)} 内"
        )
    try:
        registry.parse(name, version, page)
    except RuleError as error:
        return classify_parse_error(release, error)
    return CompatibilityKind.COMPATIBLE, ''


def classify_parse_error(release, error):
    """把规则抛出的 RuleError 细分为坏页或字段不兼容。"""
    message = str(error)
    if '字段' in message or '单位' in message or '不是 JSON' in message:
        return CompatibilityKind.INCOMPATIBLE_FIELD, message
    return CompatibilityKind.BAD_PAGE, message


def build_default_registry():
    """构造与 fixtures/contracts.json 对齐的两版规则注册表。"""
    registry = RuleRegistry()
    registry.register(
        make_release('measurement', 1, parse_measurement_v1,
                     input_fields=('id', 'value', 'unit'),
                     output_fields=('value', 'unit')),
        parse_measurement_v1,
    )
    registry.register(
        make_release('measurement', 2, parse_measurement_v2,
                     input_fields=('id', 'value', 'unit'),
                     output_fields=('value_m',),
                     accepted_units=('m', 'km')),
        parse_measurement_v2,
    )
    return registry
