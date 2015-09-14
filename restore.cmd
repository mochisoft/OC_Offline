set pgbin_path=C:\Program Files\PostgreSQL\8.4\bin
set study_db_name=site_study_database_name_goes_here
set zip_source=pathe_to_the_directory_where_extracted_database_dump_is_stored\extracted
REM 4. restore db using
REM user clinica password is set in the %APPDATA%\postgresql\pgpass.conf (where %APPDATA% refers to the Application Data subdirectory in the user's profile).
REM ---------------------------------------------------
"%pgbin_path%\pg_restore.exe" -i -h localhost -p 5432 -U clinica -d %study_db_name% -v "%zip_source%\%study_db_name%.backup"
