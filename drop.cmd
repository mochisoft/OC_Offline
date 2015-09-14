set pgbin_path=C:\Program Files\PostgreSQL\8.4\bin
set study_db_name=enter_site_study_database_name_here
REM 2. drop oc database using
REM user clinica password is set in the %APPDATA%\postgresql\pgpass.conf (where %APPDATA% refers to the Application Data subdirectory in the user's profile).
REM --------------------------------------------------
"%pgbin_path%\dropdb.exe" -U clinica -h localhost %study_db_name%

