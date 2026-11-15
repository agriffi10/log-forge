"""Print a python file with docstrings elided, original line numbers kept."""
import ast, sys
path = sys.argv[1]
src = open(path).read()
tree = ast.parse(src)
skip = set()
for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)):
        b = node.body
        if b and isinstance(b[0], ast.Expr) and isinstance(b[0].value, ast.Constant) and isinstance(b[0].value.value, str):
            d = b[0]
            if d.end_lineno - d.lineno >= 2:
                for i in range(d.lineno + 1, d.end_lineno + 1):
                    skip.add(i)
lo = int(sys.argv[2]) if len(sys.argv) > 2 else 1
hi = int(sys.argv[3]) if len(sys.argv) > 3 else 10**9
for i, line in enumerate(src.splitlines(), 1):
    if i in skip or i < lo or i > hi:
        continue
    print(f"{i:5d} {line}")
