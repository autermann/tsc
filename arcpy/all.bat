
SETLOCAL

SET "PYTHON=c:\Python27\ArcGIS10.5\python.exe"
SET "BASE=c:\tsc\arcpy"

"%PYTHON%" "%BASE%\preprocess_axes.py" || goto :error
"%PYTHON%" "%BASE%\create_subsets.py" || goto :error
"%PYTHON%" "%BASE%\split_into_weeks.py" || goto :error
"%PYTHON%" "%BASE%\calculate_statistics.py" || goto :error
"%PYTHON%" "%BASE%\to_cest.py" || goto :error

goto :EOF

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%
