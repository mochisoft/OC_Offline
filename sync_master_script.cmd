REM define  path to the folder where synchronized data will be pushed...this could be Filr, dropbox, google drive e.t.c
set shared_folder_path=C:\shared_folder_path
set pgbin_path=C:\Program Files\PostgreSQL\8.4\bin
set zip_path=C:\Program Files\7-Zip
set oc_data=zipped_study_data_file_containing_db_dump.zip
set zip_source="\\path_on_your_server_where_the_zipped_file_is_moved_to_because_file_on_the_shared_folder_can_be_removed_by_the_owner"
set extracted_zip_source="\\path_on_the_server_where_zipped_folder_is_extracted\extracted"

set study_db_name=study_name

xcopy %shared_folder_path%\*.zip %zip_source% /Q /Y

call extract.cmd

call rename_db.cmd

call drop.cmd

call create.cmd

call restore.cmd


REM 5.execute python scheduler using
REM ---------------------------------------------------
call schedule.cmd


REM 6. Execute python import using
REM ---------------------------------------------------
call import.cmd
