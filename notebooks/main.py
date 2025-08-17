from pathlib import Path

# 1) See where you're writing
print("CWD:", Path.cwd())

# 2) Desired content
content = """# Data
data/*.xlsx
data/*.csv

# Jupyter
*.ipynb_checkpoints
.ipynb_checkpoints/

# Python cache
__pycache__/
*.py[cod]
*$py.class

# Envs
.env
.venv
venv/
env/

# Models & artifacts
models/*
*.pkl
*.joblib

# OS / editor
.DS_Store
Thumbs.db
.idea/
.vscode/

# Logs
logs/
*.log
"""

# 3) Write atomically and verify
p = Path(".gitignore")
p.write_text(content, encoding="utf-8")
print("Wrote .gitignore with", len(content), "chars")

# 4) Read back to confirm
print("---- .gitignore preview ----")
print(p.read_text(encoding="utf-8"))
