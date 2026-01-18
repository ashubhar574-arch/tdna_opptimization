# Databricks notebook source
# Databricks-safe pytest runner (final fixed version)
import os
import sys
import pytest
import traceback


# 1) Prevent .pyc / __pycache__ creation
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

# 2) Paths
# workspace_dir = "/Workspace/Users/eyakankshar@etihadppe.ae/avtura_turnaround/test/"

workspace_dir="/Workspace/Users/akanksharana321997@gmail.com/turnaround_info (5)/"
tests_dir = os.path.join(workspace_dir, "tests")

# Validate directories
if not os.path.isdir(workspace_dir):
    raise SystemExit(f"❌ Workspace dir missing: {workspace_dir}")
if not os.path.isdir(tests_dir):
    raise SystemExit(f"❌ Tests dir missing: {tests_dir}")

# 3) Add workspace root to sys.path - this makes avtura_business_logic.py importable
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

# 4) Writable coverage directories
custom_temp_dir = "/Workspace/Users/akanksharana321997@gmail.com/turnaround_info (5)/tests/"
os.makedirs(custom_temp_dir, exist_ok=True)

htmlcov_dir = os.path.join(custom_temp_dir, "htmlcov")
xml_report = os.path.join(custom_temp_dir, "coverage.xml")

# 5) Pytest arguments (NO --cache-dir) - target only our specific test file
pytest_args = [
    tests_dir,
    # os.path.join(tests_dir, "test_avtura_mock_data.py"),  # Full path to our test file
    "-v",
    "--disable-warnings",
    "--assert=plain",  # avoid rewriting assertions (__pycache__)
    "--maxfail=1",     # optional: stop on first failure
    "--cov=avtura_data_processing",  # correct module name in test/ directory
    f"--cov-report=html:{htmlcov_dir}",
    f"--cov-report=xml:{xml_report}",
    "--cov-report=term"
]

# 6) Run pytest safely (override sys.argv to avoid Databricks args)
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
    sys.argv = old_argv  # restore args

# 7) Handle result
if ret_code == 0:
    print("✅ All tests passed successfully!")
else:
    print(f"❌ Pytest finished with exit code: {ret_code}")

print("Coverage HTML report:", htmlcov_dir)
print("Coverage XML report:", xml_report)


# COMMAND ----------

# MAGIC %pip install pytest-cov

# COMMAND ----------

# MAGIC %restart_python