import ast
import copy
import inspect
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import export_soak
import soak_policy_24h as policy

class Policy24HourTests(unittest.TestCase):
    def state(self,hours=24):
        start=1792833024587;end=start+int(hours*3600000)
        state=dict(continuous_start_ms=start,last_sample_ms=end,code_hash='frozen-candidate',samples=100,
            producer_samples=[dict(rss_bytes=100000000) for _ in range(12)],exports={})
        for interval,seconds in export_soak.PERIOD_SECONDS.items():
            step=seconds*1000
            rows=[dict(time=start+1000,boundary=start//step*step,reason='STARTUP',duration_ms=1000,
                peak_rss_bytes=100000000,coverage=dict(expected=522,completed=522))]
            for boundary in range((start//step+1)*step,end-180000+1,step):
                rows.append(dict(time=boundary+1000,boundary=boundary,reason='CANDLE_CLOSE',duration_ms=1000,
                    peak_rss_bytes=100000000,close_latency_ms=1000,coverage=dict(expected=522,completed=522)))
            state['exports'][interval]=rows
        return state

    def test_only_duration_differs_from_frozen_evaluator(self):
        tree=ast.parse(inspect.getsource(policy.summarize_soak))
        class RestoreDuration(ast.NodeTransformer):
            def visit_Constant(self,node):
                if type(node.value) is int and node.value==24:node.value=72
                return node
        actual=RestoreDuration().visit(tree)
        self.assertEqual(ast.dump(ast.parse(inspect.getsource(export_soak.summarize_soak))),ast.dump(actual))

    def test_24_hours_complete_evidence_passes_without_claiming_72(self):
        state=self.state()
        self.assertEqual('PASSED',policy.summarize_soak(state)['status'])
        self.assertEqual(24,policy.summarize_soak(state)['required_hours'])
        self.assertEqual('OBSERVING',export_soak.summarize_soak(state)['status'])

    def test_less_than_24_hours_never_passes(self):
        self.assertEqual('OBSERVING',policy.summarize_soak(self.state(23.99))['status'])

    def test_missing_close_cannot_pass_at_24_hours(self):
        state=self.state();state['exports']['15m'].pop()
        self.assertEqual('INSUFFICIENT_EXPORT_EVIDENCE',policy.summarize_soak(state)['status'])

    def test_memory_failure_remains_failure(self):
        state=self.state();state['failures']=[['15m',state['last_sample_ms'],'NODE_OOM']]
        self.assertEqual('FAILED',policy.summarize_soak(state)['status'])

    def test_24_hours_without_records_is_not_a_pass(self):
        state=self.state();state['exports']={}
        self.assertEqual('INSUFFICIENT_EXPORT_EVIDENCE',policy.summarize_soak(state)['status'])

if __name__=='__main__':unittest.main()
