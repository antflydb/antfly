# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Hermetic production-soak evidence tests: no model, server, or GPU execution."""
import copy
import io
import json
import signal
import unittest
from unittest import mock
import urllib.error

import qualify_soak as soak

PREFIX = 'antfly_inference_admission_'


def exposition(**overrides):
    values = {'host_scratch_bytes': '0', 'backend_scratch_bytes': '0',
              'host_weight_bytes': '1234', 'backend_weight_bytes': '5678'}
    values.update(overrides)
    return '\n'.join(PREFIX + key + ' ' + value for key, value in values.items()) + '\n'


def unicode_fixture(unit):
    case = {'id': 'unicode', 'text': 'Aé😀e\u0301中', 'expected_classifications': [],
            'expected_entities': [{'label': 'name', 'text': 'é😀e\u0301', 'start': 1, 'end': 5, 'score': .9}]}
    endpoints = {'unicode_codepoints': (1, 5), 'utf8_bytes': (1, 10), 'utf16_codeunits': (1, 6)}
    entity = copy.deepcopy(case['expected_entities'][0])
    entity['start'], entity['end'] = endpoints[unit]
    response = {'schema_version': 2, 'data': [{'id': case['id'], 'offset_unit': unit, 'entities': [entity]}]}
    return case, response


class CoordinateEvidenceTests(unittest.TestCase):
    def test_accent_astral_combining_and_cjk_coordinates_in_all_public_units(self):
        text = 'Aé😀e\u0301中'
        expected = {'unicode_codepoints': [0, 1, 2, 3, 4, 5, 6],
                    'utf8_bytes': [0, 1, 3, 7, 8, 10, 13],
                    'utf16_codeunits': [0, 1, 2, 4, 5, 6, 7]}
        for unit, coordinates in expected.items():
            with self.subTest(unit=unit):
                self.assertEqual(coordinates, soak.coordinate_map(text, unit))
                case, response = unicode_fixture(unit)
                original = copy.deepcopy(response)
                self.assertEqual(0, soak.check_units('pii', case, response, 1, unit))
                self.assertEqual(original, response, 'checking must not rewrite the retained HTTP receipt')

    def test_rejects_utf8_continuation_and_utf16_surrogate_interior(self):
        for unit, start in [('utf8_bytes', 2), ('utf16_codeunits', 3)]:
            with self.subTest(unit=unit):
                case, response = unicode_fixture(unit)
                response['data'][0]['entities'][0]['start'] = start
                with self.assertRaises(ValueError):
                    soak.check_units('pii', case, response, 1, unit)

    def test_rejects_noninteger_json_offsets_even_if_python_keys_compare_equal(self):
        for unit in ['unicode_codepoints', 'utf8_bytes', 'utf16_codeunits']:
            for value in [True, 1.0, '1', None, -1, 100]:
                with self.subTest(unit=unit, value=value):
                    case, response = unicode_fixture(unit)
                    response['data'][0]['entities'][0]['start'] = value
                    with self.assertRaises(ValueError):
                        soak.check_units('pii', case, response, 1, unit)

    def test_rejects_wrong_units_identity_order_or_confidence(self):
        case, response = unicode_fixture('utf8_bytes')
        for field, value in [('offset_unit', 'unicode_codepoints'), ('id', 'wrong')]:
            changed = copy.deepcopy(response)
            changed['data'][0][field] = value
            with self.assertRaises(ValueError):
                soak.check_units('pii', case, changed, 1, 'utf8_bytes')
        response['data'][0]['entities'][0]['score'] += .001
        with self.assertRaises(ValueError):
            soak.check_units('pii', case, response, 1, 'utf8_bytes')


class AdmissionEvidenceTests(unittest.TestCase):
    def test_exact_large_integer_and_integer_scientific_notation(self):
        for encoded in ['9007199254740993', '9.007199254740993e15']:
            values = soak.parse_metrics(exposition(host_weight_bytes=encoded))
            self.assertEqual(9007199254740993, values[PREFIX + 'host_weight_bytes'])
            soak.assert_idle(values)
        self.assertEqual((1 << 64) - 1, soak.parse_metrics(exposition(
            backend_weight_bytes=str((1 << 64) - 1)))[PREFIX + 'backend_weight_bytes'])

    def test_fractional_negative_nonfinite_and_overflow_byte_counts_fail_closed(self):
        for value in ['0.5', '-0.01', '-1', '1e-6', 'NaN', 'sNaN', 'Infinity', '-Infinity',
                      '1e309', str(1 << 64), 'False']:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    soak.parse_metrics(exposition(host_scratch_bytes=value))

    def test_rejects_duplicate_missing_nontext_and_malformed_metrics(self):
        samples = [exposition() + PREFIX + 'host_scratch_bytes 0\n',
                   exposition().replace(PREFIX + 'host_scratch_bytes 0\n', ''),
                   exposition().replace(PREFIX + 'host_scratch_bytes 0', PREFIX + 'host_scratch_bytes 0 extra'),
                   {'error': 'not a text metrics response'}]
        for value in samples:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    soak.parse_metrics(value)

    def test_nonzero_scratch_lease_is_not_treated_as_idle(self):
        for field in ['host_scratch_bytes', 'backend_scratch_bytes']:
            with self.assertRaises(ValueError):
                soak.assert_idle(soak.parse_metrics(exposition(**{field: '1'})))


class StatusAndLifecycleEvidenceTests(unittest.TestCase):
    def test_ready_means_installed_inventory_including_two_extractors(self):
        soak.assert_ready({'status': 'ready', 'models': {'extractors': 2}})
        for response in [{'status': 'not_ready', 'models': {'extractors': 2}},
                         {'status': 'ready', 'models': {'extractors': 1}},
                         {'status': 'ready', 'models': {'extractors': 2.0}},
                         {'status': 'ready', 'models': {'extractors': True}},
                         {'status': 'ready', 'models': None}, {}]:
            with self.subTest(response=response):
                with self.assertRaises(ValueError):
                    soak.assert_ready(response)

    def test_checked_calls_reject_non200_and_report_the_actual_post_method(self):
        for status in [201, 400, 413, 429, 503]:
            with mock.patch.object(soak, 'call', return_value=(status, {'retryable': True}, 1)):
                with self.assertRaisesRegex(ValueError, f'POST /ai/v1/extract: HTTP {status}'):
                    soak.checked_call('http://unused', '/ai/v1/extract', {'inputs': []})
        with mock.patch.object(soak, 'call', return_value=(200, {'ok': True}, 1)):
            self.assertEqual(({'ok': True}, 1), soak.checked_call('http://unused', '/healthz'))

    def test_http_errors_preserve_status_and_json_body_for_rejection_and_retry_gates(self):
        payload = {'error': 'CAPACITY', 'retryable': True, 'retry_after_ms': 1000}
        error = urllib.error.HTTPError('http://unused', 503, 'capacity', {},
                                       io.BytesIO(json.dumps(payload).encode()))
        with mock.patch.object(soak.urllib.request, 'urlopen', side_effect=error):
            status, response, elapsed = soak.call('http://unused', '/ai/v1/extract', {'text': 'é'})
        self.assertEqual(503, status)
        self.assertEqual(payload, response)
        self.assertGreaterEqual(elapsed, 0)

    def test_retry_honors_public_backoff_and_retains_the_recovery_deadline(self):
        self.assertEqual(1, soak.capacity_retry_delay({'retry_after_ms': 1000}, 0))
        self.assertEqual(1, soak.capacity_retry_delay({}, 0))
        self.assertEqual(.25, soak.capacity_retry_delay({'retry_after_ms': 250}, 0))
        for response, elapsed in [({'retry_after_ms': 1000}, 59.5), ({'retry_after_ms': True}, 0),
                                  ({'retry_after_ms': -1}, 0), ({'retry_after_ms': 1.5}, 0)]:
            with self.assertRaises(ValueError):
                soak.capacity_retry_delay(response, elapsed)

    def test_shutdown_rejects_crashes_forced_kills_and_premature_exit(self):
        soak.assert_server_shutdown(True, 0)
        soak.assert_server_shutdown(True, -signal.SIGTERM)
        for running, code in [(False, 0), (False, -signal.SIGTERM), (True, -signal.SIGSEGV),
                               (True, -signal.SIGKILL), (True, 1)]:
            with self.subTest(running=running, code=code):
                with self.assertRaises(ValueError):
                    soak.assert_server_shutdown(running, code)


if __name__ == '__main__':
    unittest.main()
