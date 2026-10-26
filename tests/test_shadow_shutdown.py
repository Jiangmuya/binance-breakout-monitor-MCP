import hashlib
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
import run_shadow
import check_release

class FinalAcceptanceControlTests(unittest.TestCase):
    def test_stop_before_start_never_submits_export(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'source'/'monitor.sqlite3';source.parent.mkdir();source.touch()
            output=root/'shadow';output.mkdir();(output/'STOP').touch()
            config=Path(__file__).resolve().parents[1]/'config.json'
            args=SimpleNamespace(source_db=str(source),source_config=str(config),output=str(output),
                node=None,hours=0,poll_seconds=.001,memory_mib=512,once=False)
            with patch.object(run_shadow.ShadowRunner,'tick') as tick:
                run_shadow.run(args)
            tick.assert_not_called()
            health=json.loads((output/'runtime/health.json').read_text())
            self.assertFalse(health['running'])
            self.assertFalse(health['worker_busy'])

    def test_failed_health_publication_records_error_and_closes_database(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'source'/'monitor.sqlite3';source.parent.mkdir();source.touch()
            output=root/'shadow'
            config=Path(__file__).resolve().parents[1]/'config.json'
            args=SimpleNamespace(source_db=str(source),source_config=str(config),output=str(output),
                node=None,hours=0,poll_seconds=.001,memory_mib=512,once=False)
            real_write=run_shadow._write_json
            real_close=run_shadow.Store.close
            def fail_health(path,value):
                if Path(path).name=='health.json':
                    raise PermissionError('persistent health-file lock')
                return real_write(path,value)
            with patch.object(run_shadow.ShadowRunner,'tick',return_value=None), \
                 patch.object(run_shadow,'_write_json',side_effect=fail_health), \
                 patch.object(run_shadow.Store,'close',autospec=True,side_effect=real_close) as closed:
                with self.assertRaisesRegex(PermissionError,'persistent health-file lock'):
                    run_shadow.run(args)
            closed.assert_called_once()
            errors=[json.loads(line) for line in (output/'acceptance/shadow-errors.jsonl').read_text().splitlines()]
            self.assertEqual('PermissionError',errors[-1]['type'])
            self.assertIn('persistent health-file lock',errors[-1]['detail'])
            self.assertFalse((output/'acceptance/soak-state.json').exists())

    def test_writer_binding_accepts_exact_files_and_rejects_changed_or_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);hashes={}
            for name in ('excel_builder.mjs','streaming_xlsx.mjs','package.json','package-lock.json'):
                (root/name).write_text(name)
                hashes[name]=hashlib.sha256((root/name).read_bytes()).hexdigest()
            report=dict(runs=[dict(source_unchanged_during_run=True,provenance=dict(source_sha256=hashes))])
            self.assertTrue(check_release.writer_evidence_matches(report,root))
            (root/'streaming_xlsx.mjs').write_text('changed')
            self.assertFalse(check_release.writer_evidence_matches(report,root))
            self.assertFalse(check_release.writer_evidence_matches(dict(runs=[]),root))

if __name__=='__main__':unittest.main()
