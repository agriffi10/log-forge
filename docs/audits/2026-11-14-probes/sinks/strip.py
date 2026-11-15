"""Print named functions of a source file without their docstrings (read-only helper)."""
import ast, sys
path = sys.argv[1]; names = set(sys.argv[2:])
src = open(path).read(); tree = ast.parse(src); lines = src.splitlines()
for node in ast.walk(tree):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and (node.name in names or not names):
        body = node.body; start = node.lineno
        end_doc = node.lineno
        first = ""
        if isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant) and isinstance(body[0].value.value, str):
            end_doc = body[0].end_lineno
            first = body[0].value.value.strip().splitlines()[0]
        print(f"--- {path.split('/')[-1]}:{node.lineno} {node.name}  # {first}")
        for i in range(end_doc, node.end_lineno):
            if lines[i].strip():
                print(f"{i+1}: {lines[i]}")
