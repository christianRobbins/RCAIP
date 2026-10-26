"""Synthetic-only dataframe substitute and AST extraction. NOT a Spark runtime."""
import ast
from collections import Counter
import copy
import json
import re
from types import SimpleNamespace


def frozen(x): return json.dumps(x, sort_keys=True, default=str)
class C:
    def __init__(self,f,name=None): self.f,self.name=f,name
    def v(self,r): return self.f(r)
    def alias(self,n): return C(self.f,n)
    def cast(self,t):
        return C(lambda r: None if self.v(r) is None else int(self.v(r)) if t=='int' else str(self.v(r)))
    def isNull(self): return C(lambda r:self.v(r) is None)
    def isNotNull(self): return C(lambda r:self.v(r) is not None)
    def __eq__(self,x): return C(lambda r: None if self.v(r) is None or val(x).v(r) is None else self.v(r)==val(x).v(r))
    def __ne__(self,x): return C(lambda r: None if self.v(r) is None or val(x).v(r) is None else self.v(r)!=val(x).v(r))
    def __invert__(self): return C(lambda r:not self.v(r))
    def __ge__(self,x): return C(lambda r:self.v(r) is not None and self.v(r)>=val(x).v(r))
    def __gt__(self,x): return C(lambda r: self.v(r) is not None and self.v(r)>x)
    def __and__(self,x): return C(lambda r:bool(self.v(r)) and bool(x.v(r)))
    def __or__(self,x): return C(lambda r:bool(self.v(r)) or bool(x.v(r)))
    def eqNullSafe(self,x): return C(lambda r:self.v(r)==x.v(r))
    def isin(self,*xs): return C(lambda r:self.v(r) in xs)
    def contains(self,s): return C(lambda r:s in (self.v(r) or ''))
    def rlike(self,s): return C(lambda r:bool(re.search(s,self.v(r) or '')))
    def when(self,c,x): return C(lambda r:self.v(r) if self.v(r) is not None else val(x).v(r) if c.v(r) else None)
    def otherwise(self,x): return C(lambda r:self.v(r) if self.v(r) is not None else val(x).v(r))
class Agg(C):
    def alias(self,n): return Agg(self.f,n)
class Explode(C):
    def alias(self,n): return Explode(self.f,n)
def val(x): return x if isinstance(x,C) else C(lambda r:x)
def col(x): return x if isinstance(x,C) else C(lambda r:r.get(x.replace('`','')),x)
def rowfn(f,x): return C(lambda r:None if col(x).v(r) is None else f(col(x).v(r)))
def sort_array(x):
    if isinstance(x,Agg): return Agg(lambda rows:sorted(x.v(rows)))
    return rowfn(sorted,x)
def concat_ws(sep,*xs):
    if any(isinstance(x,Agg) for x in xs):
        return Agg(lambda rows:sep.join(str(v) for x in xs for v in (x.v(rows) if isinstance(x,Agg) else [col(x).v(rows[0])]) if v is not None))
    return C(lambda r:sep.join(str(v) for x in xs for v in (col(x).v(r) if isinstance(col(x).v(r),list) else [col(x).v(r)]) if v is not None))
F=SimpleNamespace(col=col,lit=val,trim=lambda x:rowfn(str.strip,x),lower=lambda x:rowfn(str.lower,x),
    when=lambda c,x:C(lambda r:val(x).v(r) if c.v(r) else None),
    coalesce=lambda *xs:C(lambda r:next((col(x).v(r) for x in xs if col(x).v(r) is not None),None)),
    concat_ws=concat_ws,
    concat=lambda *xs:C(lambda r:None if any(col(x).v(r) is None for x in xs) else "".join(str(col(x).v(r)) for x in xs)),
    collect_set=lambda x:Agg(lambda rows:list({col(x).v(r) for r in rows if col(x).v(r) is not None})),
    struct=lambda *xs:C(lambda r:{col(x).name:col(x).v(r) for x in xs}),
    to_json=lambda x,*opts:C(lambda r:json.dumps(col(x).v(r),separators=(',',':'))),
    collect_list=lambda x:Agg(lambda rows:[col(x).v(r) for r in rows]),
    max=lambda x:Agg(lambda rows:max((col(x).v(r) for r in rows if col(x).v(r) is not None),default=None)),sort_array=sort_array,
    explode=lambda x:Explode(lambda r:col(x).v(r)),asc_nulls_last=lambda x:x,
    count=lambda x:Agg(lambda rows:len(rows)),countDistinct=lambda x:Agg(lambda rows:len({col(x).v(r) for r in rows})))
class D:
    def __init__(self,rows,columns=None):
        self.rows=copy.deepcopy(rows);self.columns=columns or (list(rows[0]) if rows else []);self.tag=None
    def filter(self,c): return D([r for r in self.rows if c.v(r)],self.columns)
    def limit(self,n): return D(self.rows[:n],self.columns)
    def count(self): return len(self.rows)
    def withColumn(self,n,c): return D([dict(r,**{n:c.v(r)}) for r in self.rows],list(dict.fromkeys(self.columns+[n])))
    def drop(self,n): return D([{k:v for k,v in r.items() if k!=n} for r in self.rows],[k for k in self.columns if k!=n])
    def dropDuplicates(self): return D(list({frozen(r):r for r in self.rows}.values()),self.columns)
    def alias(self,n):
        d=D(self.rows,self.columns);d.tag=n;return d
    def select(self,*xs):
        names=[]
        for x in xs:
            names.extend(self.columns if isinstance(x,str) and x.endswith('.*') else [x if isinstance(x,str) else (x.name or 'value').split('.')[-1]])
        result=[]
        for r in self.rows:
            out={}; exploded=None
            for x in xs:
                if isinstance(x,str) and x.endswith('.*'):
                    prefix=x[:-1];out.update({k[len(prefix):]:v for k,v in r.items() if k.startswith(prefix)})
                else:
                    c=col(x);n=c.name or 'value';n=n.split('.')[-1]
                    if isinstance(c,Explode): exploded=(n,c.v(r))
                    else: out[n]=c.v(r)
            if exploded: result.extend(dict(out,**{exploded[0]:v}) for v in exploded[1])
            else: result.append(out)
        return D(result,list(result[0]) if result else names)
    def exceptAll(self,other):
        counts=Counter(frozen(r) for r in other.rows);out=[]
        for r in self.rows:
            k=frozen(r)
            if counts[k]: counts[k]-=1
            else:out.append(r)
        return D(out,self.columns)
    def join(self,other,on,how):
        out=[]
        for l in self.rows:
            matched=False
            for r in other.rows:
                if isinstance(on,C):
                    merged={**{self.tag+'.'+k:v for k,v in l.items()},**{other.tag+'.'+k:v for k,v in r.items()}}
                    yes=on.v(merged)
                else:
                    keys=[on] if isinstance(on,str) else on
                    yes=all(l.get(k) is not None and l.get(k)==r.get(k) for k in keys)
                    merged={**l,**r}
                if yes:
                    matched=True;out.append(l if how=='left_semi' else merged)
                    if how=='left_semi':break
            if not matched and how=='left':out.append({**{k:None for k in other.columns},**l})
        return D(out,list(dict.fromkeys(self.columns+other.columns)))
    def groupBy(self,*keys):
        parent=self
        class G:
            def agg(self,*aggs):
                groups={}
                for r in parent.rows:groups.setdefault(tuple(r[k] for k in keys),[]).append(r)
                return D([{**dict(zip(keys,k)),**{a.name:a.v(rs) for a in aggs}} for k,rs in groups.items()],list(keys)+[a.name for a in aggs])
            def count(self):return self.agg(Agg(lambda rs:len(rs),'count'))
        return G()
    def orderBy(self,*args):return self
    def collect(self):
        return [type("Row",(dict,),{"asDict":lambda r,recursive=False:dict(r)})(r) for r in self.rows]
    def toPandas(self):
        import pandas as pd
        return pd.DataFrame(self.rows,columns=self.columns,dtype=object)


def extract(path,names,namespace):
    tree=ast.parse(path.read_text(encoding='utf-8-sig'))
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names or isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in names for t in n.targets)]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),namespace)
    return tree
