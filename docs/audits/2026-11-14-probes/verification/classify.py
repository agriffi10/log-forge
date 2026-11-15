import ast, re, sys, pathlib, collections
root = pathlib.Path(sys.argv[1])
pats = {
 'park': r'threading\.(Event|Barrier|Condition|Semaphore)\(|\bEvent\(\)|\bBarrier\(',
 'thread': r'threading\.Thread\(|\bThread\(target',
 'fork': r'os\.fork\(|run_in_child|register_at_fork|_fork\.',
 'sleep': r'time\.sleep\(',
 'mono': r'monotonic\(\)|perf_counter\(\)',
 'cpuwin': r'cpu_window|process_time|thread_time',
 'floor': r'timing_floor|assert_floor|measure_delay|TimingFloor',
 'ast': r'\bast\.(parse|walk|NodeVisitor)',
 'hyp': r'hypothesis',
 'rand': r'random\.Random\(|random\.seed|\brandom\.',
 'perm': r'itertools\.(permutations|product|combinations)|\bpermutations\(|\bproduct\(',
 'subproc': r'subprocess\.',
 'asyncio': r'async def test_',
 'param': r'parametrize',
 'elapsed': r'elapsed\s*[<>]=?|[<>]=?\s*elapsed|assert .*(took|duration|elapsed)',
 'gc': r'gc\.(collect|get_objects)',
 'faulth': r'faulthandler',
}
rows=[]
for p in sorted(root.rglob('*.py')):
    src=p.read_text()
    try: tree=ast.parse(src)
    except Exception as e: print('PARSEFAIL',p,e); continue
    ntests=sum(1 for n in ast.walk(tree) if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name.startswith('test_'))
    row={'file':str(p.relative_to(root)),'lines':src.count('\n'),'tests':ntests}
    for k,v in pats.items(): row[k]=len(re.findall(v,src))
    rows.append(row)
keys=['lines','tests']+list(pats)
print('file\t'+'\t'.join(keys))
for r in rows: print(r['file']+'\t'+'\t'.join(str(r[k]) for k in keys))
tot={k:sum(r[k] for r in rows) for k in keys}
print('TOTAL\t'+'\t'.join(str(tot[k]) for k in keys))
print('FILESWITH\t'+'\t'.join(str(sum(1 for r in rows if r[k])) for k in keys))
