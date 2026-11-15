import ast,sys,pathlib,re,collections
wt=pathlib.Path(sys.argv[1]); src=wt/'src'/'log_foundry'; tests=wt/'tests'
mods={}
for p in sorted(src.rglob('*.py')):
    rel=p.relative_to(src).with_suffix('')
    parts=list(rel.parts)
    if parts[-1]=='__init__': parts=parts[:-1]
    name='.'.join(['log_foundry']+parts)
    mods[name]=p.read_text().count('\n')
print('src modules',len(mods),'src lines',sum(mods.values()))
tfiles=[p for p in tests.rglob('*.py')]
refs=collections.defaultdict(set)  # mod -> test files
fileMods=collections.defaultdict(set)
for p in tfiles:
    s=p.read_text()
    try: t=ast.parse(s)
    except Exception: continue
    found=set()
    for n in ast.walk(t):
        if isinstance(n,ast.Import):
            for a in n.names:
                if a.name in mods: found.add(a.name)
        elif isinstance(n,ast.ImportFrom) and n.module and n.module.startswith('log_foundry'):
            if n.module in mods: 
                # from pkg import submodule?
                hit=False
                for a in n.names:
                    if n.module+'.'+a.name in mods: found.add(n.module+'.'+a.name); hit=True
                if not hit or n.module!='log_foundry': 
                    if n.module not in('log_foundry','log_foundry.sinks'): found.add(n.module)
        elif isinstance(n,ast.Attribute):
            pass
    # dotted textual references e.g. log_foundry._lifecycle. or "log_foundry.sinks._retry.wait"
    for m in mods:
        if m in ('log_foundry','log_foundry.sinks'): continue
        if re.search(re.escape(m)+r'\b',s): found.add(m)
        # file path refs like sinks/http.py or _lifecycle.py
        tail='/'.join(m.split('.')[1:])+'.py'
        if re.search(r'(^|[^\w])'+re.escape(tail),s): found.add(m)
    for m in found: refs[m].add(p)
    fileMods[p]=found
lines={p:p.read_text().count('\n') for p in tfiles}
rows=[]
for m,sl in mods.items():
    if m in('log_foundry','log_foundry.sinks'): pass
    fs=refs.get(m,set())
    share=sum(lines[p]/max(1,len(fileMods[p])) for p in fs)
    rows.append((share/sl if sl else 0, m, sl, len(fs), int(share), sum(lines[p] for p in fs)))
rows.sort()
print('ratio(shared)\tmodule\tsrc_lines\ttest_files_ref\tshared_test_lines\traw_test_lines')
for r in rows: print(f"{r[0]:.2f}\t{r[1]}\t{r[2]}\t{r[3]}\t{r[4]}\t{r[5]}")
