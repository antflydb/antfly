"""No-model continuation contract checks and read-only paused-state validation."""
import copy
from pathlib import Path
import tempfile
import unittest

import continuation as driver

CPU = Path('/private/tmp/antfly-gliner25-training-inactive-published-small-v1')


class ContinuationTests(unittest.TestCase):
    def test_exact_resource_and_semantic_configuration_preserved(self):
        prep = driver.preparation()
        for mode in ('lora', 'dora'):
            original = driver.reference.load(driver.PAUSE_ROOT / f'{mode}-paused.json')
            for phase in driver.PHASES:
                expected = copy.deepcopy(original)
                expected['output_dir'] = str(driver.ROOT / f'{mode}-{phase}')
                if phase == 'resumed':
                    expected['resume_from'] = str(driver.PAUSE_ROOT / f'{mode}-paused/latest.safetensors')
                    expected['expected_restore_state_sha256'] = prep['paused'][mode]['state_sha256']
                actual = driver.reference.load(driver.ROOT / f'{mode}-{phase}.json')
                self.assertEqual(expected, actual)
                self.assertEqual(actual['memory'], original['memory'])
                self.assertEqual(actual['train_file'], original['train_file'])

    def test_both_original_paused_states_reconstruct_exactly_with_resident_domain(self):
        prep = driver.preparation()
        for mode in ('lora', 'dora'):
            output = driver.PAUSE_ROOT / f'{mode}-paused'
            manifest = driver.reference.read(output / 'run.json')
            fingerprint = driver.pause.controller_fingerprint(mode, manifest)
            state = driver.reference.validate_checkpoint(output / 'latest.safetensors', mode, True, fingerprint, prep['paused'][mode]['state_sha256'])
            self.assertEqual(state['global_microbatch'], 1)
            self.assertEqual(state['all_slot_updates'], 0)
            wrong = list(fingerprint)
            wrong[0] ^= 1
            with self.assertRaisesRegex(ValueError, 'run fingerprint'):
                driver.reference.validate_checkpoint(output / 'latest.safetensors', mode, True, wrong)

    def test_stitched_reports_and_results_require_exact_same_backend_semantics(self):
        # Reuse already validated CPU report bytes only as a small structural
        # test input. This does not claim new Metal update execution.
        reports = [driver.reference.loads(line) for line in driver.reference.read(CPU / 'lora-uninterrupted/progress.jsonl').splitlines()]
        result = driver.reference.load(CPU / 'lora-uninterrupted/result.json')
        whole = {'result': result, 'reports': reports}
        paused = {'result': {'run_fingerprint': result['run_fingerprint']}, 'reports': reports[:1]}
        resumed = {'result': copy.deepcopy(result), 'reports': copy.deepcopy(reports[1:])}
        driver.compare_continuity(whole, paused, resumed)
        for key in ('zero_loss_fallback', 'decision_fingerprint', 'terms', 'optimizer'):
            changed = copy.deepcopy(resumed)
            changed['reports'][0][key] = None
            with self.assertRaisesRegex(ValueError, 'semantic decisions'):
                driver.compare_continuity(whole, paused, changed)
        changed = copy.deepcopy(resumed)
        changed['result']['state_sha256'][0] ^= 1
        with self.assertRaisesRegex(ValueError, 'final result'):
            driver.compare_continuity(whole, paused, changed)
        changed = copy.deepcopy(resumed)
        changed['reports'].pop()
        with self.assertRaisesRegex(ValueError, 'missing/extra'):
            driver.compare_continuity(whole, paused, changed)

    def test_all_inactive_rows_and_final_partial_flush_are_mandatory(self):
        reports = [driver.reference.loads(line) for line in driver.reference.read(CPU / 'dora-uninterrupted/progress.jsonl').splitlines()]
        driver.reference.validate_reports(reports, 'uninterrupted')
        driver.reference.validate_reports(reports[1:], 'resumed')
        for index in (1, 3, 4):
            changed = copy.deepcopy(reports)
            changed[index]['optimizer']['loss'] = changed[index]['terms']['total']
            with self.assertRaisesRegex(ValueError, 'inactive objective'):
                driver.reference.validate_reports(changed, 'uninterrupted')
        changed = copy.deepcopy(reports)
        changed[-1]['optimizer']['grad_norm'] = 0.001
        with self.assertRaisesRegex(ValueError, 'wholly inactive'):
            driver.reference.validate_reports(changed, 'uninterrupted')

    def test_supervised_process_identity_reaping_and_resources_are_strict(self):
        prep = driver.preparation()
        config = driver.ROOT / 'lora-resumed.json'
        process = {'returncode': 0, 'failure': None, 'cleanup': {'complete': True, 'direct_child_reaped': True, 'known_children_gone': True}, 'psutil_version': '7.1.3', 'max_child_tree_rss_bytes': driver.pause.RSS_LIMIT, 'peak_child_tree_rss_bytes': 600 * driver.MIB, 'tracked_processes': [{'relation': 'observed_descendant'}], 'helpers': driver.helper_pins(), 'binary': {key: prep['binary'][key] for key in ('size_bytes', 'sha256')}, 'config': driver.reference.digest(config), 'command': driver.command_for(Path(prep['binary']['path']), config)}
        driver.validate_process(process, 'lora', 'resumed', prep)
        for key, value in (('returncode', False), ('peak_child_tree_rss_bytes', driver.pause.RSS_LIMIT + 1), ('command', process['command'] + ['--stop-after-microbatches', '1']), ('tracked_processes', [])):
            changed = copy.deepcopy(process)
            changed[key] = value
            with self.assertRaises(ValueError):
                driver.validate_process(changed, 'lora', 'resumed', prep)
        for key in process['cleanup']:
            changed = copy.deepcopy(process)
            changed['cleanup'][key] = False
            with self.assertRaisesRegex(ValueError, 'cleanup'):
                driver.validate_process(changed, 'lora', 'resumed', prep)

    def test_receipts_are_additive_and_cannot_overwrite_successful_pause(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / 'validation.json'
            driver.reference.write_new(output, {'status': 'pass'})
            original = output.read_bytes()
            with self.assertRaises(FileExistsError):
                driver.reference.write_new(output, {'status': 'changed'})
            self.assertEqual(output.read_bytes(), original)
        self.assertEqual(driver.reference.digest(Path(driver.supervision.__file__))['sha256'], 'a937237975be2ed879f62afd285494ccb51a7c1d7cc1dff7160621fb26b87332')


if __name__ == '__main__':
    unittest.main()
