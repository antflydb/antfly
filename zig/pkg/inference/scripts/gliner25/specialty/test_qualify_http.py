# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Hermetic HTTP qualification policy tests; never starts a server or GPU."""
import copy
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import qualify_http as gate


def marker(profile):
    return f'info(session): gliner_specialty_cuda_policy profile={profile} weights=f32 attention=f32 diagnostic_overrides=ignored\n'


POLICY = marker('pii') + marker('gliguard')


class PolicyEvidenceTests(unittest.TestCase):
    def test_requires_both_fp32_policy_receipts_independently_of_surrounding_logs(self):
        receipts = gate.parse_cuda_policy('unrelated startup log\n' + POLICY + 'listening\n')
        self.assertEqual({'pii', 'gliguard'}, set(receipts))
        for profile, receipt in receipts.items():
            self.assertEqual('f32', receipt['weights'])
            self.assertEqual('f32', receipt['attention'])
            self.assertEqual('ignored', receipt['diagnostic_overrides'])
            self.assertEqual([marker(profile).strip()], receipt['log_lines'])
        for text in ['', marker('pii'), marker('gliguard'), 'weights=f32 attention=f32']:
            with self.assertRaises(ValueError):
                gate.parse_cuda_policy(text)

    def test_production_structured_json_logs_use_the_actual_message_field(self):
        log = "\n".join(json.dumps({"ts": "2026-10-30", "level": "info", "scope": "default",
            "msg": marker(profile).strip()}) for profile in gate.PROFILES)
        receipts = gate.parse_cuda_policy(log)
        self.assertEqual(set(gate.PROFILES), set(receipts))
        self.assertTrue(receipts["pii"]["log_lines"][0].startswith("{"))
        for bad in ['{"msg":"gliner_specialty_cuda_policy',
                    json.dumps({"other": marker("pii")}),
                    json.dumps({"msg": "unrelated", "other": marker("pii")})]:
            with self.assertRaises(ValueError):
                gate.parse_cuda_policy(POLICY + bad)

    def test_conflicting_duplicate_or_malformed_markers_cannot_hide_behind_a_valid_pair(self):
        for bad in [marker('pii').replace('weights=f32', 'weights=f16'),
                    marker('gliguard').replace('attention=f32', 'attention=streaming-f16'),
                    marker('pii').replace('diagnostic_overrides=ignored', 'diagnostic_overrides=enabled'),
                    marker('unexpected'), marker('pii').replace('weights=f32 ', ''),
                    marker('pii').strip() + ' extra=unreviewed\n']:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    gate.parse_cuda_policy(POLICY + bad)
        self.assertEqual(2, len(gate.parse_cuda_policy(POLICY + marker('pii'))['pii']['log_lines']))

    def test_all_explicit_conflicting_modes_reach_the_child_environment(self):
        inherited = {'ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE': 'inherited',
                     'ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO': 'inherited', 'CUDA_VISIBLE_DEVICES': '0'}
        original = copy.deepcopy(inherited)
        for mode in gate.ATTENTION_MODES:
            for generated in ('0', '1'):
                with self.subTest(mode=mode, generated=generated):
                    env = gate.backend_environment(mode, generated, inherited)
                    self.assertEqual(mode, env['ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE'])
                    self.assertEqual(generated, env['ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO'])
                    self.assertEqual('cuda', env['ANTFLY_INFERENCE_REQUIRED_BACKEND'])
                    self.assertEqual('0', env['CUDA_VISIBLE_DEVICES'])
        self.assertEqual(original, inherited)
        for mode, generated in [('fp16', '0'), ('f32', 'true'), ('auto', 1)]:
            with self.assertRaises(ValueError):
                gate.backend_environment(mode, generated, inherited)

    def test_records_visibility_math_and_cuda_controls_without_secrets(self):
        controls = {'CUDA_VISIBLE_DEVICES': '0', 'CUDA_DEVICE_ORDER': 'PCI_BUS_ID',
                    'NVIDIA_TF32_OVERRIDE': '0', 'CUBLAS_WORKSPACE_CONFIG': ':4096:8',
                    'CUDA_MODULE_LOADING': 'LAZY', 'ANTFLY_CUDA_TEST_CONTROL': '1',
                    'TERMITE_CUDA_TEST_CONTROL': '2', 'ANTFLY_INFERENCE_CUDA_TEST_CONTROL': '3'}
        env = {**controls, 'ANTFLY_CUDA_API_KEY': 'secret', 'TERMITE_CUDA_TOKEN': 'secret',
               'ANTFLY_INFERENCE_CUDA_CREDENTIAL': 'secret', 'UNRELATED_PASSWORD': 'secret', 'PATH': 'unrelated'}
        self.assertEqual(controls, gate.backend_environment_receipt(env))

    def test_cleanup_cannot_turn_a_premature_exit_or_crash_into_pass(self):
        gate.assert_server_shutdown(True, 0)
        gate.assert_server_shutdown(True, -signal.SIGTERM)
        for running, code in [(False, 0), (False, -signal.SIGTERM), (True, -signal.SIGKILL),
                               (True, -signal.SIGSEGV), (True, 1)]:
            with self.assertRaises(ValueError):
                gate.assert_server_shutdown(running, code)


class RunningImageEvidenceTests(unittest.TestCase):
    def test_rejects_different_image_or_process_group(self):
        process = mock.Mock(pid=123, returncode=None)
        process.poll.return_value = None
        with mock.patch.object(gate.os, 'readlink', return_value='/candidate'), \
             mock.patch.object(gate, 'digest_file', return_value='actual'), \
             mock.patch.object(gate.os, 'getpgid', return_value=123):
            self.assertEqual('actual', gate.running_binary_receipt(process, 'actual')['sha256'])
            with self.assertRaisesRegex(ValueError, 'SHA differs'):
                gate.running_binary_receipt(process, 'different')
        with mock.patch.object(gate.os, 'readlink', return_value='/candidate'), \
             mock.patch.object(gate, 'digest_file', return_value='actual'), \
             mock.patch.object(gate.os, 'getpgid', return_value=999):
            with self.assertRaisesRegex(ValueError, 'escaped'):
                gate.running_binary_receipt(process, 'actual')

    def test_premature_exit_and_image_disappearance_fail_closed(self):
        process = mock.Mock(pid=123, returncode=0)
        process.poll.return_value = 0
        with self.assertRaisesRegex(ValueError, 'exited before'):
            gate.running_binary_receipt(process, 'sha')
        process.poll.return_value = None
        with mock.patch.object(gate.os, 'readlink', side_effect=FileNotFoundError):
            with self.assertRaisesRegex(ValueError, 'exited during'):
                gate.running_binary_receipt(process, 'sha')


class ProcessGroupCleanupTests(unittest.TestCase):
    def test_exited_leader_does_not_hide_a_child_ignoring_sigterm(self):
        process = mock.Mock(pid=9001, returncode=0)
        process.poll.return_value = 0
        process.wait.return_value = 0
        forced = []
        def kill_group(group, sig):
            self.assertEqual(9001, group)
            if sig == signal.SIGKILL:
                forced.append(True)
        with mock.patch.object(gate.os, 'killpg', side_effect=kill_group), \
             mock.patch.object(gate, 'process_group_members', side_effect=lambda _: [] if forced else [9002]):
            with self.assertRaises(gate.ProcessGroupCleanupError) as failure:
                gate.terminate_server(process, grace_seconds=0)
        self.assertTrue(forced)
        self.assertEqual([], failure.exception.remaining_pids)
        self.assertEqual(0, failure.exception.returncode)

    @unittest.skipUnless(sys.platform == 'linux', 'GB10 cleanup uses Linux /proc')
    def test_normal_owned_process_is_terminated_waited_and_group_drained(self):
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=True)
        try:
            image = gate.running_binary_receipt(process, gate.digest_file(Path(sys.executable)))
            self.assertEqual(process.pid, image['pid'])
            self.assertEqual(process.pid, image['process_group'])
            gate.terminate_server(process, grace_seconds=1)
            self.assertIsNotNone(process.poll())
            self.assertEqual([], gate.process_group_members(process.pid))
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)

    @unittest.skipUnless(sys.platform == 'linux', 'GB10 cleanup uses Linux /proc')
    def test_forced_kill_drains_owned_process_but_fails_qualification(self):
        process = subprocess.Popen([sys.executable, '-c',
            "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); time.sleep(60)"],
            stdout=subprocess.PIPE, start_new_session=True)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 2)[0])
            self.assertEqual(b'ready\n', process.stdout.readline())
            with self.assertRaises(gate.ProcessGroupCleanupError) as failure:
                gate.terminate_server(process, grace_seconds=.05)
            self.assertEqual([], failure.exception.remaining_pids)
            self.assertEqual(-signal.SIGKILL, process.returncode)
            self.assertEqual([], gate.process_group_members(process.pid))
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)
            process.stdout.close()


class HarnessPolicyIntegrationTests(unittest.TestCase):
    def run_harness(self, initial_log=POLICY, late_log=''):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        output = Path(directory.name) / 'report.json'
        log_path = output.with_suffix('.server.log')
        case = {'id': 'example', 'text': 'hello', 'input_ids': [1], 'native_schema_json': '{"entities":["person"]}',
                'expected_entities': [], 'expected_classifications': []}
        references = {profile: {'cases': [copy.deepcopy(case)]} for profile in gate.PROFILES}
        instances = {'object': 'list', 'data': [{'model': profile, 'id': profile + '-id', 'backend': 'cuda',
            'state': 'loaded', 'tasks': ['extractor'], 'startup_managed': True} for profile in gate.PROFILES]}
        process = SimpleNamespace(pid=12345, returncode=None, poll=lambda: None)
        args = SimpleNamespace(server=Path('/unused/server'), models_root=Path('/unused/models'), output=output,
            port=18984, startup_timeout=1, case=None, batch_size=1, warmups=0, iterations=1,
            attention_mode='generated-tc', generated_tc_auto='1')
        report = {}
        child_environments = []
        def fake_spawn(_command, **kwargs):
            child_environments.append(kwargs['env'])
            kwargs['stdout'].write(initial_log.encode())
            kwargs['stdout'].flush()
            return process
        def fake_http(_base, path, body=None, **_kwargs):
            if path == '/ai/v1/models/instances':
                if late_log:
                    with log_path.open('a') as log:
                        log.write(late_log)
                return copy.deepcopy(instances), 1
            request = json.loads(body)
            return {'schema_version': 2, 'data': [{'id': item['id'], 'offset_unit': 'unicode_codepoints',
                'entities': [], 'classifications': []} for item in request['inputs']]}, 1
        def fake_terminate(target):
            target.returncode = -signal.SIGTERM
        with mock.patch.object(gate, 'verify_inputs', return_value=({}, references)), \
             mock.patch.object(gate, 'hardware', return_value={'test': 'no hardware queried'}), \
             mock.patch.object(gate, 'digest_file', return_value='synthetic-sha'), \
             mock.patch.object(gate.socket, 'socket'), \
             mock.patch.object(gate.subprocess, 'Popen', side_effect=fake_spawn), \
             mock.patch.object(gate, 'wait_ready', return_value=copy.deepcopy(instances)), \
             mock.patch.object(gate, 'running_binary_receipt', return_value={'pid': 12345, 'sha256': 'synthetic-sha'}), \
             mock.patch.object(gate, 'http_json', side_effect=fake_http) as http, \
             mock.patch.object(gate, 'terminate_server', side_effect=fake_terminate) as terminate, \
             mock.patch('builtins.print'):
            error = None
            try:
                gate.qualify(args, report)
            except ValueError as failure:
                error = failure
        return report, child_environments, http.call_count, terminate.call_count, error

    def test_conflicting_globals_are_set_while_actual_profile_policy_is_checked_before_and_after(self):
        report, envs, calls, cleanups, error = self.run_harness()
        self.assertIsNone(error)
        self.assertEqual('generated-tc', envs[0]['ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE'])
        self.assertEqual('1', envs[0]['ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO'])
        self.assertEqual({'pii', 'gliguard'}, set(report['cuda_policy_before']))
        self.assertEqual({'pii', 'gliguard'}, set(report['cuda_policy_after']))
        self.assertEqual('synthetic-sha', report['running_binary_before']['sha256'])
        self.assertEqual('synthetic-sha', report['running_binary_after']['sha256'])
        self.assertGreater(calls, 0)
        self.assertEqual(1, cleanups)

    def test_missing_profile_policy_fails_before_extractions_and_still_cleans_up(self):
        _report, _envs, calls, cleanups, error = self.run_harness(initial_log=marker('pii'))
        self.assertIsInstance(error, ValueError)
        self.assertIn('missing specialty CUDA policy', str(error))
        self.assertEqual(0, calls)
        self.assertEqual(1, cleanups)

    def test_conflicting_late_policy_fails_even_after_all_outputs_match(self):
        _report, _envs, calls, cleanups, error = self.run_harness(
            late_log=marker('gliguard').replace('attention=f32', 'attention=f16'))
        self.assertIsInstance(error, ValueError)
        self.assertIn('violates the FP32 contract', str(error))
        self.assertGreater(calls, 0)
        self.assertEqual(1, cleanups)


if __name__ == '__main__':
    unittest.main()
