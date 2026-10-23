"""Real process lifecycle tests; no model or GPU is used."""
import os
import signal
import subprocess
import sys
import unittest

from qualify_matched_llama import finish_campaign


class Sampler:
    def __init__(self, failure=False, samples=2):
        self.failure=failure
        self.samples=samples
    def finish(self):
        if self.failure:raise RuntimeError('injected sampling failure')
        return {'samples':self.samples,'errors':[],'gpu_uuids':['test-gpu'],
                'peak_host_rss_bytes':1,'peak_gpu_memory_bytes':1}


class MatchedLlamaCleanupTests(unittest.TestCase):
    def start(self, source):
        process=subprocess.Popen([sys.executable,'-u','-c',source],start_new_session=True,
                                 stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        self.addCleanup(self.fallback,process)
        self.assertEqual(process.stdout.readline().strip(),'READY')
        return process

    @staticmethod
    def fallback(process):
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
        process.wait(timeout=3)
        process.stdout.close();process.stderr.close()

    @staticmethod
    def evidence():
        return {'passed':True,'sampling_qualified':True,'hardware':[{'uuid':'test-gpu'}]}

    def test_normal_shutdown_verifies_group_and_keeps_success(self):
        process=self.start("import time; print('READY'); time.sleep(30)")
        evidence=self.evidence();finish_campaign(evidence,process,Sampler(),timeout=.3)
        self.assertTrue(evidence['passed'])
        self.assertTrue(evidence['process_group_stopped'])
        self.assertNotIn('cleanup_errors',evidence)

    def test_sampling_failure_preserves_primary_and_still_stops_process(self):
        process=self.start("import time; print('READY'); time.sleep(30)")
        evidence=self.evidence();evidence.update(passed=False,error='original token mismatch')
        finish_campaign(evidence,process,Sampler(failure=True),timeout=.3)
        self.assertEqual(evidence['error'],'original token mismatch')
        self.assertIn('injected sampling failure',' '.join(evidence['cleanup_errors']))
        self.assertTrue(evidence['process_group_stopped'])
        self.assertFalse(evidence['sampling_qualified'])

    def test_stubborn_leader_is_reaped_and_cannot_qualify(self):
        process=self.start("import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('READY'); time.sleep(30)")
        evidence=self.evidence();finish_campaign(evidence,process,Sampler(),timeout=.3)
        self.assertTrue(evidence['forced_kill'])
        self.assertTrue(evidence['process_group_stopped'])
        self.assertFalse(evidence['passed'])
        self.assertEqual(process.returncode,-signal.SIGKILL)

    def test_stubborn_descendant_cannot_outlive_completed_cleanup(self):
        child="import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('CHILD_READY',flush=True); time.sleep(30)"
        source=("import subprocess,sys,time\n"
                f"child=subprocess.Popen([sys.executable,'-u','-c',{child!r}],stdout=subprocess.PIPE,text=True)\n"
                "assert child.stdout.readline().strip()=='CHILD_READY'\n"
                "print('READY'); time.sleep(30)\n")
        process=self.start(source)
        evidence=self.evidence();finish_campaign(evidence,process,Sampler(),timeout=.3)
        self.assertTrue(evidence['forced_kill'])
        self.assertTrue(evidence['process_group_stopped'])
        self.assertFalse(evidence['passed'])
        self.assertIn('descendants',' '.join(evidence['cleanup_errors']))

    def test_insufficient_sampling_cannot_qualify(self):
        evidence=self.evidence();finish_campaign(evidence,None,Sampler(samples=1),timeout=.3)
        self.assertFalse(evidence['passed'])
        self.assertFalse(evidence['sampling_qualified'])
        self.assertIn('incomplete',evidence['error'])


if __name__=='__main__':unittest.main()
