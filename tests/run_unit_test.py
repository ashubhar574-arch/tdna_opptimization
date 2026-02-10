
# Databricks-safe pytest runner (coverage target fixed to match import name)
import os
import sys
import pytest
import traceback

# 1) Prevent .pyc / __pycache__ creation
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

# 2) Paths
workspace_dir = "/Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_preference/"
tests_dir = os.path.join(workspace_dir, "tests")

# Validate directories
if not os.path.isdir(workspace_dir):
    raise SystemExit(f"❌ Workspace dir missing: {workspace_dir}")
if not os.path.isdir(tests_dir):
    raise SystemExit(f"❌ Tests dir missing: {tests_dir}")

# 3) Add workspace root to sys.path
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

# 4) Writable coverage directories
custom_temp_dir = os.path.join(tests_dir, "tests_coverage")
os.makedirs(custom_temp_dir, exist_ok=True)
htmlcov_dir = os.path.join(custom_temp_dir, "htmlcov")
xml_report = os.path.join(custom_temp_dir, "coverage.xml")

# 5) Pytest arguments (NO --cache-dir). IMPORTANT: --cov target matches import name.
pytest_args = [
    tests_dir,
    "-v",
    "--disable-warnings",
    "--assert=plain",
    "--maxfail=1",
    "--cov=tdna_guest_preference_nb",  # <-- fix: match import name used by tests
    f"--cov-report=html:{htmlcov_dir}",
    f"--cov-report=xml:{xml_report}",
    "--cov-report=term",
]

# 6) Run pytest safely
old_argv = sys.argv.copy()
try:
    sys.argv = ["pytest"] + pytest_args
    print("Running pytest with args:", pytest_args)
    ret_code = pytest.main(pytest_args)
except Exception:
    print("❌ Exception while running pytest:")
    traceback.print_exc()
    ret_code = 2
finally:
    sys.argv = old_argv

# 7) Handle result
print("✅ All tests passed successfully!" if ret_code == 0 else f"❌ Pytest finished with exit code: {ret_code}")
print("Coverage HTML report:", htmlcov_dir)
print("Coverage XML report:", xml_report)
