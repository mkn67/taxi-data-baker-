"""Script to test execution of notebook code cells in pilot mode."""

import ast
import json
import os
import sys

def run_notebook_cells(nb_path: str, mode: str = "pilot"):
    print(f"\n{'='*70}")
    print(f">>> TESTING EXECUTION OF: {nb_path} (mode={mode})")
    print(f"{'='*70}")

    os.environ["BENCHMARK_MODE"] = mode

    with open(nb_path) as f:
        nb = json.load(f)

    code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]

    # Provide display function
    def custom_display(obj):
        print(obj)

    global_ctx = {
        "__name__": "__main__",
        "display": custom_display,
    }

    # Change working directory to notebooks/ directory while executing
    old_cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(os.path.abspath(nb_path)))
        for i, cell in enumerate(code_cells):
            source = "".join(cell["source"])
            # Remove ipython magics if any
            lines = [l for l in source.splitlines() if not l.strip().startswith("%") and not l.strip().startswith("!")]
            code_to_exec = "\n".join(lines)
            print(f"--- Executing Cell {i+1}/{len(code_cells)} ---")
            exec(code_to_exec, global_ctx)
        print(f"\n>>> NOTEBOOK {nb_path} EXECUTED TO COMPLETION SUCCESSFULLY!")
    finally:
        os.chdir(old_cwd)


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "notebooks/02_benchmark_aggregation_salting.ipynb"
    mode = sys.argv[2] if len(sys.argv) > 2 else "pilot"
    run_notebook_cells(target, mode)

