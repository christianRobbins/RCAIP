"""Labelled Python substitutes only; no Spark/Delta execution or table writes."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
import threading
import time

SOURCE=Path(__file__).resolve().parents[1]/'notebooks/RCAIP_IP_001_Open_Port_Intelligence v2.0.py'
tree=ast.parse(SOURCE.read_bytes())
node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='ip001_write_with_metadata')
ns={};exec(compile(ast.Module(body=[node],type_ignores=[]),str(SOURCE),'exec'),ns)
write=ns['ip001_write_with_metadata']


class Conf:
    def __init__(self, previous=None, fault=None):
        self.value=previous;self.previous=previous;self.fault=fault
    def isModifiable(self,key):return self.fault!='unsupported'
    def get(self,key,default=None):
        if self.fault=='get':raise RuntimeError('synthetic get failure')
        return self.value
    def set(self,key,value):
        if self.fault=='set' and value=='TOKEN':raise RuntimeError('synthetic set failure')
        if self.fault=='restore' and value==self.previous:raise RuntimeError('synthetic restore failure')
        self.value='wrong' if self.fault=='mismatch' and value=='TOKEN' else value
    def unset(self,key):
        if self.fault=='restore':raise RuntimeError('synthetic unset failure')
        self.value=None


class Writer:
    def __init__(self,conf,fail=False,events=None):self.conf=conf;self.fail=fail;self.options={};self.calls=0;self.events=events
    def option(self,k,v):self.options[k]=v;return self
    def mode(self,v):self.mode_value=v;return self
    def format(self,v):self.format_value=v;return self
    def saveAsTable(self,name):
        self.calls+=1
        assert self.conf.value==self.options['userMetadata']
        if self.events is not None:
            self.events.append(('start',self.conf.value));time.sleep(.01);self.events.append(('end',self.conf.value))
        if self.fail:raise ValueError('synthetic write failure')


class Metadata(unittest.TestCase):
    def test_success_restores_absent_empty_and_nonempty(self):
        for previous in (None,'','prior'):
            conf=Conf(previous);writer=Writer(conf)
            write(SimpleNamespace(conf=conf),SimpleNamespace(write=writer),'synthetic.table','TOKEN','overwrite',True)
            self.assertEqual(conf.value,previous);self.assertEqual(writer.calls,1)
            self.assertEqual(writer.options,{'userMetadata':'TOKEN','overwriteSchema':'true'})
            self.assertEqual((writer.mode_value,writer.format_value),('overwrite','delta'))
    def test_failed_write_restores_all_states(self):
        for previous in (None,'','prior'):
            conf=Conf(previous);writer=Writer(conf,True)
            with self.assertRaises(ValueError):write(SimpleNamespace(conf=conf),SimpleNamespace(write=writer),'synthetic.table','TOKEN','errorifexists')
            self.assertEqual(conf.value,previous);self.assertEqual(writer.mode_value,'errorifexists')
    def test_unavailable_before_write(self):
        for fault in ('unsupported','get','set','mismatch'):
            conf=Conf('prior',fault);writer=Writer(conf)
            with self.assertRaises(RuntimeError):write(SimpleNamespace(conf=conf),SimpleNamespace(write=writer),'synthetic.table','TOKEN','append')
            self.assertEqual(writer.calls,0);self.assertEqual(conf.value,'prior')
    def test_restoration_failure_explicit_after_success_and_failure(self):
        for fail in (False,True):
            for previous in (None,'','prior'):
                conf=Conf(previous,'restore');writer=Writer(conf,fail)
                with self.assertRaisesRegex(RuntimeError,'RESTORATION_FAILED'):
                    write(SimpleNamespace(conf=conf),SimpleNamespace(write=writer),'synthetic.table','TOKEN','append')
                self.assertEqual(writer.calls,1)
    def test_shared_session_serializes_calls(self):
        conf=Conf('prior');spark=SimpleNamespace(conf=conf);events=[];errors=[]
        def worker(token):
            try:write(spark,SimpleNamespace(write=Writer(conf,events=events)),'synthetic.table',token,'append')
            except Exception as exc:errors.append(exc)
        threads=[threading.Thread(target=worker,args=(token,)) for token in ('A','B')]
        for t in threads:t.start()
        for t in threads:t.join()
        self.assertEqual(errors,[]);self.assertEqual(conf.value,'prior')
        self.assertEqual([x[0] for x in events],['start','end','start','end'])
        self.assertEqual(events[0][1],events[1][1]);self.assertEqual(events[2][1],events[3][1])
    def test_both_integrations_use_single_implementation(self):
        save=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='save_post_table')
        self.assertEqual(sum(isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='ip001_write_with_metadata' for n in ast.walk(save)),1)
        harness=ast.parse((SOURCE.parents[1]/'tests/ip001_real_spark_smoke.py').read_bytes())
        delta=next(n for n in harness.body if isinstance(n,ast.FunctionDef) and n.name=='run_delta')
        self.assertEqual(sum(isinstance(n,ast.Call) and isinstance(n.func,ast.Subscript) and isinstance(n.func.slice,ast.Constant) and n.func.slice.value=='ip001_write_with_metadata' for n in ast.walk(delta)),2)


if __name__=='__main__':unittest.main(verbosity=2)
