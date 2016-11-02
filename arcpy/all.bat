
c:\Python27\ArcGIS10.3\python.exe c:\tsc\arcpy/preprocess_axes.py || goto :error
c:\Python27\ArcGIS10.3\python.exe c:\tsc\arcpy/create_subsets.py || goto :error
c:\Python27\ArcGIS10.3\python.exe c:\tsc\arcpy/split_into_weeks.py || goto :error
c:\Python27\ArcGIS10.3\python.exe c:\tsc\arcpy/calculate_statistics.py || goto :error
c:\Python27\ArcGIS10.3\python.exe c:\tsc\arcpy/to_cest.py || goto :error

goto :EOF

:error
echo Failed with error #%errorlevel%.
exit /b %errorlevel%