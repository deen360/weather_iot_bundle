addiing library
##uv add yfinance --no-sync

then
->uv sync --group remote --env venv_db

->uv sync --group local --env venv_spark

# Check Remote
.\venv_db\Scripts\python.exe -m pip show yfinance

# Check Local
.\venv_spark\Scripts\python.exe -m pip show yfinance


---> the fix when you make error by putting wrong dependencies without flag
# Force venv_db to ONLY have remote tools + base dependencies
uv sync --group remote --clean --env venv_db

# Force venv_spark to ONLY have local tools + base dependencies
uv sync --group local --clean --env venv_spark

##verification
# This should show databricks-connect and NOT pyspark
.\venv_db\Scripts\python.exe -m pip list | Select-String "databricks-connect", "pyspark"

# This should show pyspark and NOT databricks-connect
.\venv_spark\Scripts\python.exe -m pip list | Select-String "databricks-connect", "pyspark"

#connect databricks to external S3 with untiy catalog