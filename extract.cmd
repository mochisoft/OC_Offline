if exist %extracted_zip_source% (del %extracted_zip_source%\*.* /Q)
rd %extracted_zip_source% /S /Q 
mkdir %extracted_zip_source%

REM 1. Unzip the sync_back_up file using
REM -Remember to add -p when defining the password
REM --------------------------------------------------
"%zip_path%\7z.exe" e %zip_source%\%oc_data% -o%extracted_zip_source% -pPassword_to_extract_the_zipped_file_from_site_goes_here
