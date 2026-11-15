"""Print a source file with every bare string statement blanked, keeping original line numbers."""
import ast, sys
path = sys.argv[1]
src = open(path).read()
lines = src.split("\n")
tree = ast.parse(src)
kill = set()
for node in ast.walk(tree):
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
        for i in range(node.lineno, node.end_lineno + 1):
            kill.add(i)
for i, l in enumerate(lines, 1):
    if i in kill or not l.strip():
        continue
    print(f"{i}\t{l}")
