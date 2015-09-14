set pgbin_path=C:\Program Files\PostgreSQL\8.4\bin
set study_db_name=enter_oc_site_database_name_here
REM 3. Create OpenClinica postgres database using
REM user clinica password is set in the %APPDATA%\postgresql\pgpass.conf (where %APPDATA% refers to the Application Data subdirectory in the user's profile).
REM ---------------------------------------------------
"%pgbin_path%\createdb.exe" -U clinica  -h localhost %study_db_name%
