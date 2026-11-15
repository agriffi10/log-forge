import ast,sys,pathlib,re
root=pathlib.Path(sys.argv[1])/'tests'
mode=sys.argv[2]
SRC_HINT=re.compile(r'log_foundry|_SRC|__file__|src')
for p in sorted(root.rglob('*.py')):
    s=p.read_text()
    try: t=ast.parse(s)
    except Exception: continue
    rel='tests/'+str(p.relative_to(root))
    if mode=='ast':
        if not re.search(r'\bast\.(parse|walk|NodeVisitor)',s): continue
        funcs={n.name:n for n in t.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef))}
        def uses_ast(n): return any(isinstance(x,ast.Attribute) and isinstance(x.value,ast.Name) and x.value.id=='ast' for x in ast.walk(n))
        def calls(n): return {x.func.id for x in ast.walk(n) if isinstance(x,ast.Call) and isinstance(x.func,ast.Name)} | {x.id for x in ast.walk(n) if isinstance(x,ast.Name)}
        astf={k for k,v in funcs.items() if uses_ast(v)}
        changed=True
        while changed:
            changed=False
            for k,v in funcs.items():
                if k not in astf and calls(v)&astf: astf.add(k); changed=True
        tests=[(v.lineno,k) for k,v in funcs.items() if k.startswith('test_') and k in astf]
        first=min((i+1 for i,l in enumerate(s.split('\n')) if re.search(r'\bast\.(parse|walk|NodeVisitor)',l)))
        print(f"{rel}:{first}\t{len(tests)} of {sum(1 for k in funcs if k.startswith('test_'))} tests reach ast\t"+'; '.join(f"{n}@{l}" for l,n in sorted(tests)[:60]))
    else:
        parents={}
        for n in ast.walk(t):
            for c in ast.iter_child_nodes(n): parents[c]=n
        lines=s.split('\n')
        for n in ast.walk(t):
            if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call):
                f=n.value.func
                if isinstance(f,ast.Attribute) and f.attr=='sleep' and isinstance(f.value,ast.Name) and f.value.id=='time' and n.value.args and isinstance(n.value.args[0],ast.Constant):
                    v=n.value.args[0].value
                    a=n; inwhile=incls=False; fn=None
                    while a in parents:
                        a=parents[a]
                        if isinstance(a,(ast.While,ast.For)): inwhile=True
                        if isinstance(a,ast.ClassDef): incls=True
                        if isinstance(a,(ast.FunctionDef,ast.AsyncFunctionDef)) and fn is None: fn=a.name
                    if inwhile or incls or v<0.05: continue
                    nxt=lines[n.end_lineno].strip() if n.end_lineno<len(lines) else ''
                    print(f"{rel}:{n.lineno}\tsleep({v})\tin {fn}\tnext: {nxt[:90]}")
