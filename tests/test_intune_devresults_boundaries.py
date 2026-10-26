"""Boundary regression over actual adapter and persistent synthetic transports.

Page generation/iteration ONLY is substituted for the capacity cases. No Spark,
Delta or Workspace service execution. Workers are independent Python processes.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from copy import deepcopy
import test_intune_devresults as t
import intune_devresults_spark_verification as launcher

HOST = t.inputs()['workspace_host']


class ManyPagesStore(t.Store):
    def pages(self, products):
        return t.sf.D([dict(product='missing_identity', page=i,
            html='<!doctype html><meta charset="utf-8"><a href="index.html">Time basis and field legend</a><p>SYNTHETIC PAGE '+str(i)+'</p>')
            for i in range(self.page_count)], ['product', 'page', 'html'])

    def page_rows(self, frame):
        for r in sorted(frame.rows, key=lambda r: r['page']):
            yield r['product']+'-'+str(r['page'])+'.html', r['html'].encode()


def large_worker(root, mode, fault, count):
    cfg=t.inputs(); plan,pid,_,_,_,writer,audit=t.dev.prepare(cfg,t.SOURCES,t.old.d)
    db=t.Catalog(root/'tables.sqlite')
    # WAL/NORMAL preserves committed transactions across PROCESS exit; this fixture
    # does not claim OS/power-loss durability or emulate Delta durability internals.
    db.db.execute('PRAGMA journal_mode=WAL'); db.db.execute('PRAGMA synchronous=NORMAL')
    store=ManyPagesStore(db,plan,pid,writer,audit,mode=='RESUME')
    store.page_count=count; ws=t.Workspace(db)
    if fault=='commit:receipts': db.fail_role='receipts'
    if fault=='upload':ws.interrupt=True
    anchorfile=root/'trusted-anchor.json'
    anchor=json.loads(anchorfile.read_text()) if mode=='RESUME' else None
    def progress(a):anchorfile.write_text(json.dumps(a))
    def inject(point):
        if point==fault:os._exit(77)
    with t.table_api(db):
        try:
            result=t.old.d.run(cfg,t.SOURCES,mode=mode,approval=t.approval(cfg,mode,anchor),anchor=anchor,
                backend=store,publisher=t.dev.WorkspacePublisher(ws,plan),progress=progress,fault=inject)
            receipt,ref=store.get('publication')
            rows=store.read('receipts',receipt['receipts']).rows
            # An independent expected path/ordering/size check, not just encoder self-comparison.
            expected_names=['missing_identity-'+str(i)+'.html' for i in range(count)]+['index.html']
            assert [r['path'].rsplit('/',1)[1] for r in rows]==expected_names
            assert [r['ordinal'] for r in rows]==list(range(count+1))
            assert receipt['artifact_count']==count+1
            for r in rows:
                body=db.db.execute('select body from files where path=?',(r['path'],)).fetchone()[0]
                assert r['sha256']==hashlib.sha256(body).hexdigest() and r['bytes']==len(body)
            record={'status':result['status'],'receipt_rows':len(rows),'completion_bytes':len(t.dev.blob(receipt)),
                    'index_bytes':rows[-1]['bytes'],'all_output_hashes_verified':True}
        except Exception as e:record={'failure':str(e)}
    record.update(raw_calls=store.analytics.raw_calls,profile_calls=store.analytics.profile_calls,uploads=ws.uploads)
    db.db.close();return record


class Boundaries(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)

    def manifest(self):
        p=self.root/'manifest.json'
        p.write_text(json.dumps({'notebooks/'+n:{'lf_sha256':hashlib.sha256(
            (t.ROOT/'notebooks'/n).read_bytes().replace(b'\r\n',b'\n')).hexdigest()} for n in launcher.FILES.values()}))
        return p

    def test_launcher_supported_timezones_and_exact_approved_change(self):
        class PassiveSpark:
            def __init__(self,zone):
                self.conf=types.SimpleNamespace(get=lambda key:zone);self.catalog=self;self.calls=0
            def currentCatalog(self):self.calls+=1;raise RuntimeError('SYNTHETIC_PREFLIGHT')
        path=self.manifest();db=t.Catalog(self.root/'ws.sqlite');ws=t.Workspace(db)
        try:
            for zone in ('UTC','Etc/UTC','+00:00'):
                args=dict(catalog='synthetic_catalog',principal=t.inputs()['principal'],workflow_id='synthetic_tz_01',
                          session_timezone=zone,workspace_host=HOST)
                p=launcher.verify(t.ROOT/'notebooks',path,**args)
                self.assertEqual(p['private_plan']['inputs']['metadata']['session_timezone'],zone)
                auth=dict(plan_id=p['plan_id'],reference='SYNTHETIC',operations=p['operations'],allow_values=True,
                          allow_writes=True,allow_recomputation=True,anchor=None)
                spark=PassiveSpark(zone)
                with self.assertRaises(Exception):launcher.verify(t.ROOT/'notebooks',path,**args,mode='FRESH',
                    spark=spark,client=ws,approval=auth,progress=lambda _:None)
                self.assertEqual(spark.calls,1)
                changed=PassiveSpark('Etc/UTC' if zone=='UTC' else 'UTC')
                with self.assertRaisesRegex(Exception,'SESSION_TIMEZONE_CHANGED'):
                    launcher.verify(t.ROOT/'notebooks',path,**args,mode='FRESH',spark=changed,client=ws,approval=auth,progress=lambda _:None)
                self.assertEqual(changed.calls,0)
            args['session_timezone']='Europe/London'
            with self.assertRaises(Exception):launcher.verify(t.ROOT/'notebooks',path,**args)
            self.assertEqual((db.writes,ws.uploads),(0,0))
        finally:db.db.close()

    def test_workspace_missing_wrong_and_mutated_before_values(self):
        cfg=t.inputs();auth=t.approval(cfg,'FRESH',None)
        for host in (None,'https://other-workspace.example.invalid'):
            with self.subTest(host=host):
                plan,pid,_,_,_,writer,audit=t.dev.prepare(cfg,t.SOURCES,t.old.d)
                db=t.Catalog(self.root/('absent.sqlite' if host is None else 'wrong.sqlite'))
                store=t.Store(db,plan,pid,writer,audit);ws=t.Workspace(db);ws.config.host=host
                with self.assertRaisesRegex(t.dev.Stop,'WORKSPACE'):
                    t.dev.WorkspacePublisher(ws,plan).verify_principal()
                # Injected backend/publisher use a separately loaded Stop class; the public
                # entry point safely wraps it. Retain both exact boundary and zero-action checks.
                with t.table_api(db),self.assertRaisesRegex(Exception,'STAGE_MANIFEST_FAILED_NO_FALLBACK'):
                    t.old.d.run(cfg,t.SOURCES,mode='FRESH',approval=auth,backend=store,
                        publisher=t.dev.WorkspacePublisher(ws,plan),progress=lambda _:None)
                self.assertEqual((db.writes,ws.uploads,store.analytics.raw_calls),(0,0,0));db.db.close()
        for host in (None,'','http://not-https.invalid','https://user:secret@example.invalid',HOST+'/path',HOST+'?x=1'):
            changed=deepcopy(cfg);changed['workspace_host']=host
            with self.assertRaisesRegex(Exception,'WORKSPACE_IDENTITY_REQUIRED'):t.old.d.run(changed,t.SOURCES)
        changed=deepcopy(cfg);changed['workspace_host']='https://other-workspace.example.invalid'
        with self.assertRaisesRegex(Exception,'EXACT_WORKFLOW_APPROVAL_REQUIRED'):
            t.old.d.run(changed,t.SOURCES,mode='FRESH',approval=auth,progress=lambda _:None)

    def test_resume_wrong_workspace_never_reads_values(self):
        t.invoke(self.root,'FRESH','after_checkpoint')
        cfg=t.inputs();plan,pid,_,_,_,writer,audit=t.dev.prepare(cfg,t.SOURCES,t.old.d)
        db=t.Catalog(self.root/'tables.sqlite');store=t.Store(db,plan,pid,writer,audit,True)
        store.read=lambda *a: (_ for _ in ()).throw(AssertionError('VALUE_READ_BEFORE_DESTINATION_CHECK'))
        ws=t.Workspace(db);ws.config.host='https://other-workspace.example.invalid'
        anchor=json.loads((self.root/'trusted-anchor.json').read_text())
        with self.assertRaisesRegex(t.dev.Stop,'AUTHENTICATED_WORKSPACE_CHANGED'):
            t.dev.WorkspacePublisher(ws,plan).verify_principal()
        with t.table_api(db),self.assertRaisesRegex(Exception,'STAGE_MANIFEST_FAILED_NO_FALLBACK'):
            t.old.d.run(cfg,t.SOURCES,mode='RESUME',approval=t.approval(cfg,'RESUME',anchor),anchor=anchor,
                backend=store,publisher=t.dev.WorkspacePublisher(ws,plan))
        self.assertEqual((db.writes,ws.uploads,store.analytics.raw_calls),(0,0,0));db.db.close()

    def worker(self,mode,fault,count):
        r=subprocess.run([sys.executable,'-B',__file__,'--worker',str(self.root),mode,fault,str(count)],capture_output=True,text=True)
        if fault in ('after_uploads','commit:receipts'):
            self.assertEqual(r.returncode,77,r.stderr);print('LARGE_HARD_STOP',count,fault,77,flush=True);return
        self.assertEqual(r.returncode,0,r.stderr);value=json.loads(r.stdout)
        print('LARGE_NEW_PROCESS',count,mode,fault,json.dumps(value),flush=True);return value

    def test_declared_10000_page_capacity_complete(self):
        r=self.worker('FRESH','none',10000)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual(r['receipt_rows'],10001)
        self.assertLessEqual(r['completion_bytes'],t.dev.MAX_BYTES);self.assertLessEqual(r['index_bytes'],t.dev.MAX_BYTES)
        s=self.worker('RESUME','none',10000)
        self.assertEqual(s['status'],'COMPLETE');self.assertEqual((s['raw_calls'],s['profile_calls'],s['uploads']),(0,0,0))

    def test_6000_pages_after_upload_hard_stop_fresh_process_resume(self):
        self.worker('FRESH','after_uploads',6000)
        r=self.worker('RESUME','none',6000)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual((r['raw_calls'],r['profile_calls'],r['uploads']),(0,0,0))

    def test_6000_pages_receipt_commit_hard_stop_fresh_process_resume(self):
        self.worker('FRESH','commit:receipts',6000)
        r=self.worker('RESUME','none',6000)
        self.assertEqual(r['status'],'COMPLETE');self.assertEqual((r['raw_calls'],r['profile_calls'],r['uploads']),(0,0,0))

    def test_receipt_missing_extra_duplicate_or_changed_cannot_complete(self):
        for mutation in ('missing','extra','changed','duplicate'):
            with tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);self.assertEqual(t.invoke(root,'FRESH')['receipt']['status'],'COMPLETE')
                db=t.Catalog(root/'tables.sqlite')
                name,payload=db.db.execute("select name,payload from commits where name like '%_receipts'").fetchone()
                value=t.old.decode(json.loads(payload));rows=value['rows']
                if mutation=='missing':rows.pop()
                if mutation=='extra':rows.append(dict(rows[0],ordinal=999))
                if mutation=='duplicate':rows.append(rows[0])
                if mutation=='changed':rows[0]['sha256']='0'*64
                db.db.execute('update commits set payload=? where name=?',(json.dumps(t.old.encode(value)),name));db.db.commit();db.db.close()
                result=t.invoke(root,'RESUME');self.assertIn('failure',result)
                self.assertEqual((result['raw_calls'],result['profile_calls']),(0,0))

    def test_explicit_receipt_and_index_bounds(self):
        cfg=t.inputs();plan,pid,_,_,_,writer,audit=t.dev.prepare(cfg,t.SOURCES,t.old.d)
        db=t.Catalog(self.root/'bounds.sqlite');s=t.Store(db,plan,pid,writer,audit)
        good=dict(path='/synthetic/a',sha256='a'*64,bytes=1)
        for rows in ([good,good],[dict(good,path='a'*t.dev.MAX_RECEIPT_BYTES)], [dict(good,bytes=t.dev.MAX_BYTES+1)]):
            with self.assertRaises(Exception):s.receipt_frame(rows)
        with self.assertRaisesRegex(t.dev.Stop,'METADATA_RECORD_BOUND'):s.record('publication',{'x':'a'*t.dev.MAX_BYTES})
        # Oversize index is rejected before upload/receipt completion; no lowered page cap.
        from unittest.mock import patch
        with patch.object(t.old.d,'render_summary',return_value='x'*(t.dev.MAX_BYTES+1)):
            r=t.invoke(self.root,'FRESH')
        self.assertIn('REPORT_INDEX_TOO_LARGE',r['failure'])
        check=t.Catalog(self.root/'tables.sqlite')
        self.assertFalse(any(n.endswith('_publication') for n, in check.db.execute('select name from objects')))
        check.db.close();db.db.close()


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--worker':
        print(json.dumps(large_worker(Path(sys.argv[2]),sys.argv[3],sys.argv[4],int(sys.argv[5]))))
    else:unittest.main(verbosity=2)
