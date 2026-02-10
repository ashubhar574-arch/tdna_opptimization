# Databricks notebook source

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

# COMMAND ----------

Running pytest with args: ['/Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_preference/tests', '-v', '--disable-warnings', '--assert=plain', '--maxfail=1', '--cov=tdna_guest_preference_nb', '--cov-report=html:/Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_preference/tests/tests_coverage/htmlcov', '--cov-report=xml:/Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_preference/tests/tests_coverage/coverage.xml', '--cov-report=term']
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.0.2, pluggy-1.6.0 -- /local_disk0/.ephemeral_nfs/envs/pythonEnv-282a6cf9-479d-46f8-8053-05aa2ca2c371/bin/python
cachedir: .pytest_cache
rootdir: /Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_preference/tests
plugins: cov-7.0.0, typeguard-4.3.0
collecting ... collected 7 items

test_guest_preference.py::test_business_logic_transformation PASSED      [ 14%]
test_guest_preference.py::test_null_ffp_number_filtered PASSED           [ 28%]
test_guest_preference.py::test_multiple_records_processing PASSED        [ 42%]
test_guest_preference.py::test_duplicate_records_distinct PASSED         [ 57%]
test_guest_preference.py::test_empty_inputs PASSED                       [ 71%]
test_guest_preference.py::test_ingestion_fields_types PASSED             [ 85%]
test_guest_preference.py::test_empty_expectations_array PASSED           [100%]/local_disk0/.ephemeral_nfs/envs/pythonEnv-282a6cf9-479d-46f8-8053-05aa2ca2c371/lib/python3.12/site-packages/coverage/inorout.py:561: CoverageWarning: Module tdna_guest_preference_nb was never imported. (module-not-imported); see https://coverage.readthedocs.io/en/7.13.4/messages.html#warning-module-not-imported
  self.warn(f"Module {pkg} was never imported.", slug="module-not-imported")
/local_disk0/.ephemeral_nfs/envs/pythonEnv-282a6cf9-479d-46f8-8053-05aa2ca2c371/lib/python3.12/site-packages/coverage/control.py:958: CoverageWarning: No data was collected. (no-data-collected); see https://coverage.readthedocs.io/en/7.13.4/messages.html#warning-no-data-collected
  self._warn("No data was collected.", slug="no-data-collected")

WARNING: Failed to generate report: No data to report.

/local_disk0/.ephemeral_nfs/envs/pythonEnv-282a6cf9-479d-46f8-8053-05aa2ca2c371/lib/python3.12/site-packages/pytest_cov/plugin.py:363: CovReportWarning: Failed to generate report: No data to report.

  warnings.warn(CovReportWarning(message), stacklevel=1)


================================ tests coverage ================================
_______________ coverage: platform linux, python 3.12.3-final-0 ________________

============================== 7 passed in 7.01s ===============================
✅ All tests passed successfully!
Coverage HTML report: /Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_preference/tests/tests_coverage/htmlcov
Coverage XML report: /Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_preference/tests/tests_coverage/coverage.xml