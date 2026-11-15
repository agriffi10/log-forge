import ast,sys,pathlib,re
root=pathlib.Path(sys.argv[1])
for p in sorted(root.glob('*.py')):
    s=p.read_text(); t=ast.parse(s)
    n=len(re.findall(r'\bast\.(parse|walk|NodeVisitor)',s))
    if not n and 'roster' not in p.name and 'inventory' not in p.name: continue
    d=(ast.get_docstring(t) or '').split('\n\n')[0].replace('\n',' ')
    nt=sum(1 for x in ast.walk(t) if isinstance(x,(ast.FunctionDef,ast.AsyncFunctionDef)) and x.name.startswith('test_'))
    print(f"{p.name}\tast={n}\ttests={nt}\tlines={s.count(chr(10))}\t{d[:230]}")
