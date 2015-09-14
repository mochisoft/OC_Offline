import glob, os

def rename(dir, pattern):
    for pathAndFilename in glob.iglob(os.path.join(dir, pattern)):
        print pathAndFilename
        title, ext = os.path.splitext(os.path.basename(pathAndFilename))
        print title
        print ext
        os.rename(pathAndFilename,os.path.join(dir, 'study_name_to_be_renamed_to' + ext))

rename(r'C:\Path_to_where_the_extracted_zipped_file_is_stored\extracted', r'*.backup')
