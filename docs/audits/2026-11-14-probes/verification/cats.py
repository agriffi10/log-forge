import csv,sys,collections
S=sys.argv[1]
coll={}
for line in open(S+'/perfile.txt'):
    n,f=line.split(); coll[f.replace('tests/','')]=int(n)
rows=list(csv.DictReader(open(S+'/classify.tsv'),delimiter='\t'))
rows=[r for r in rows if r['file'] not in('TOTAL','FILESWITH')]
for r in rows:
    for k in r:
        if k!='file': r[k]=int(r[k])
    r['coll']=coll.get(r['file'],0)
tf=[r for r in rows if r['tests']>0]
print('files with test defs',len(tf),'unit files',len([r for r in tf if not r['file'].startswith('integration')]),'integration files',len([r for r in tf if r['file'].startswith('integration')]))
print('test defs total',sum(r['tests'] for r in tf),'integration defs',sum(r['tests'] for r in tf if r['file'].startswith('integration')),'collected',sum(r['coll'] for r in tf))
def cat(name,pred):
    sel=[r for r in tf if pred(r)]
    print(f"{name}: files={len(sel)} defs={sum(r['tests'] for r in sel)} collected={sum(r['coll'] for r in sel)}")
    return sel
cat('uses Event/Barrier/Condition (park)',lambda r:r['park']>0)
cat('uses threads',lambda r:r['thread']>0)
cat('fork (run_in_child/os.fork)',lambda r:r['fork']>0 and ('fork' in r['file'] or r['fork']>=3))
cat('fork any mention',lambda r:r['fork']>0)
cat('elapsed asserts',lambda r:r['elapsed']>0)
cat('monotonic',lambda r:r['mono']>0)
cat('sleep',lambda r:r['sleep']>0)
cat('cpu_window',lambda r:r['cpuwin']>0)
cat('ast',lambda r:r['ast']>0)
cat('roster/inventory named',lambda r:'roster' in r['file'] or 'inventory' in r['file'])
cat('subprocess',lambda r:r['subproc']>0)
cat('async tests',lambda r:r['asyncio']>0)
cat('random',lambda r:r['rand']>0)
cat('none of park/thread/fork/mono/sleep/ast/subproc (pure example)',lambda r:not any(r[k] for k in('park','thread','fork','mono','sleep','ast','subproc','cpuwin')) and not r['file'].startswith('integration'))
cat('integration',lambda r:r['file'].startswith('integration'))
print('missing from collection:',[ (r['file'],r['tests']) for r in tf if r['coll']==0])
print('--- 10 largest test files by lines')
for r in sorted(rows,key=lambda r:-r['lines'])[:12]: print(r['lines'],r['tests'],r['coll'],r['file'])
