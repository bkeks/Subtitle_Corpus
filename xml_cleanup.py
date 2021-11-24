import re
import os, os.path
from pathlib import Path

directory_tokenized = "untokenized corpus files/OpenSubtitles/raw/de/"


pathlist = Path(directory_tokenized).glob('**/*.xml')
for xml_path in pathlist:
    path_in_str = str(xml_path)

    # regex the path
    id_movie = re.search(r'(de\\\d+\\)(\d+)', path_in_str).group(2)
    movie_year = re.search(r'(de\\\d+\\)(\d+)', path_in_str).group(1).lstrip("de").strip("\\")

    path_movie = "untokenized corpus files/OpenSubtitles/raw/de/" + movie_year + "/" + id_movie
    pathlist2 = Path(path_movie).glob('**/*.xml')

    # count files in dirs
    total_files = 0
    for base, dirs, files in os.walk(path_movie):
        #print('Searching in : ',base)
        for Files in files:
            total_files += 1

    # in case more than one file is there
    if total_files > 1:

        # find first xml file and keep it
        for xml_keep in pathlist2:
            print("keep " + str(xml_keep))
            break
        # rename the other files
        for xml_path2 in pathlist2:
            if not xml_path2 == xml_keep:
                print("rename " + str(xml_path2))
                new_xml_path2 = str(xml_path2) + ".old"
                os.rename(xml_path2,new_xml_path2)
