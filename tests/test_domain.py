import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        registry = Registry()
        registry.register('measurement', 1, parse_measurement)
        with self.assertRaises(ValueError):
            registry.register('measurement', 1, parse_measurement)
        self.assertEqual(collect(self.payload)[0]['unit'], 'm')
        with self.assertRaises(ValueError):
            parse_measurement({'id': 'x'})

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
