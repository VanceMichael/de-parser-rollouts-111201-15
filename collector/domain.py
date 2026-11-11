import hashlib
import json


class Registry:
    def __init__(self):
        self.rules = {}

    def register(self, name, version, rule):
        key = name, version
        if key in self.rules:
            raise ValueError('规则版本已登记')
        self.rules[key] = rule

    def parse(self, name, version, payload):
        return self.rules[name, version](payload)


def parse_measurement(value):
    if set(value) != {'id', 'value', 'unit'}:
        raise ValueError('测值字段不匹配')
    return dict(key=value['id'], value=str(value['value']), unit=value['unit'])


def collect(payload):
    registry = Registry()
    registry.register('measurement', 1, parse_measurement)
    return [dict(registry.parse('measurement', 1, row), raw_digest=hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest(), rule_version=1) for row in payload['responses']]
