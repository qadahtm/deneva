find_package( PythonInterp 3.5 REQUIRED )
find_package( PythonLibs 3.5 REQUIRED )

configure_file(test_local.py test_local.py COPYONLY)