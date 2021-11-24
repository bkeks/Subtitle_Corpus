import xml.etree.ElementTree as ET
import re
import mysql.connector
import spacy
nlp = spacy.load("de_core_news_sm") # load language model

from pathlib import Path

import time

# statistics
time_start_total = time.process_time()
data_counter = 0
time_total = 0

########
def SelectMaxID(id, table):
    # Prepared statement
    stmt_select = "SELECT MAX(" + id + ") FROM " + table + ";"
    mycursor.execute(stmt_select)
    id_str = mycursor.fetchone()

    # convert tupel to string
    id_str = id_str[0]
    id_str = str(id_str)

    # if no entry is found, then return None with type NoneType
    # then id_word will return with the value 0
    if id_str == "None":
        id = 0
    else:
        # string to int
        id = int(id_str)
    return id
#######

# Connetion to database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database="subtitle_corpus"
)

# Initiation of db connection
mycursor = mydb.cursor()

# set max_allowed_packet
mycursor.execute("SET GLOBAL max_allowed_packet=10000000000000")

directory_tokenized = "untokenized corpus files/OpenSubtitles/raw/de"

pathlist = Path(directory_tokenized).glob('**/*.xml')
for xml_path in pathlist:
    # timer for single file
    time_start_file = time.process_time()

    # because path is object not string
    path_in_str = str(xml_path)

    # import xml structure
    try:
        xml_root = ET.parse(xml_path).getroot()
    except Exception as e:
        print("Error while parsing")
        print("File skipped!")
        continue

    #############################################
    # collect meta informations

    # filename -> id_subtitle
    id_subtitle = xml_root.attrib # won't be in use

    # release year annoted by opensubtitle.org. is faulty, do not use
    movie_year = re.search(r'(de\\\d+\\)(\d+)', path_in_str).group(1).lstrip("de").strip("\\")

    # foldername -> id_movie
    id_movie = re.search(r'(de\\\d+\\)(\d+)', path_in_str).group(2)

    try:
        duration = xml_root.find('meta/subtitle/duration').text
    except:
        duration = ""
    try:
        genre = xml_root.find('meta/source/genre').text
        genres = genre.split(',')
    except:
        genres = ""
    try:
        translated_language = xml_root.find('meta/subtitle/language').text
    except:
        translated_language = ""
    try:
        original_language = xml_root.find('meta/source/original').text
    except:
        original_language = ""
    try:
        country = xml_root.find('meta/source/country').text
    except:
        country = ""
    try:
        year = xml_root.find('meta/source/year').text
    except:
        year = ""

    #####################################
    # user feedback
    print("Current file: " + movie_year + "/" + id_movie + "/" + id_subtitle['id'] + ".xml" )

    #####################################
    # insert meta
    # check currently highets id_meta
    id_meta = SelectMaxID("id_meta", "subtitle_meta")
    id_meta = id_meta + 1

    # process multiple genres and write meta infos to db
    if len(genres) < 1:
        stmt_insert = "INSERT INTO subtitle_meta (id_meta, id_movie, duration, translated_language, original_language, country, year, genre1) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val_insert = (id_meta, id_movie, duration, translated_language, original_language, country, year, "")
        mycursor.execute(stmt_insert, val_insert)
        mydb.commit()
    elif len(genres) == 1:
        stmt_insert = "INSERT INTO subtitle_meta (id_meta, id_movie, duration, translated_language, original_language, country, year, genre1) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val_insert = (id_meta, id_movie, duration, translated_language, original_language, country, year, genres[0])
        mycursor.execute(stmt_insert, val_insert)
        mydb.commit()
    elif len(genres) == 2:
        stmt_insert = "INSERT INTO subtitle_meta (id_meta, id_movie, duration, translated_language, original_language, country, year, genre1, genre2) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val_insert = (id_meta, id_movie, duration, translated_language, original_language, country, year, genres[0], genres[1])
        mycursor.execute(stmt_insert, val_insert)
        mydb.commit()
    elif len(genres) == 3:
        stmt_insert = "INSERT INTO subtitle_meta (id_meta, id_movie, duration, translated_language, original_language, country, year, genre1, genre2, genre3) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val_insert = (id_meta, id_movie, duration, translated_language, original_language, country, year, genres[0], genres[1], genres[2])
        mycursor.execute(stmt_insert, val_insert)
        mydb.commit()
    else:
        print("Invalid genre information, skipping file: " + movie_year + "/" + id_movie + "/" + id_subtitle['id'] + ".xml")

    #############################################
    # gathering subtitle data and annotation with spacy
    # check currently highest id_word
    id_token = SelectMaxID("id_token", "subtitle_token")

    # same with id_join
    id_join = SelectMaxID("id_join", "subtitle_join")

    # todo: change SelectMaxID into mycursor.lastrowid for better performance

    # emtpy dictionarys. Needed to cache the data before the db commit
    token_dict = []
    join_dict = []


    text_list = []

    # iterate through the xml structure
    for s in xml_root.findall('s'):
        # finds text right after <s>
        text_list.append(str(s.text).strip())

        for tag_time in s.findall('time'):
            # finds remaining text behind <time> elements
            text_list.append(str(tag_time.tail).strip())

    # remove empty elements from list
    text_list_clean = list(filter(None, text_list))

    # list to string
    text_string = ""
    for i in text_list_clean:
        text_string = text_string + i + " "


    # annotation with spaCy
    # create spaCy object
    doc = nlp(text_string)

    for token in doc:

        token_text = token.text
        lemma = token.lemma_
        pos = token.pos_
        if not token.morph.get("Tense") == []:
            tense_list = token.morph.get("Tense")
            tense = str(tense_list).replace('[','').replace(']','').strip("'")

        else:
            tense = ""
        if not token.morph.get("VerbForm") == []:
            verb_form_list = token.morph.get("VerbForm")
            verb_form = str(verb_form_list).replace('[','').replace(']','').strip("'")
        else:
            verb_form = ""


        # dict for insert subtitle_words
        id_token = id_token + 1
        token_dict.append({ 'id_token' : id_token, 'token' : token_text, 'lemma' : lemma, 'pos' : pos, 'verb_form' : verb_form, 'tense' : tense})

        # example:
         # {'id_token': 5607, 'token': '?', 'lemma': '?', 'pos': 'PUNCT', 'verb_form': '', 'tense': ''},
         # {'id_token': 5608, 'token': 'Ja', 'lemma': 'Ja', 'pos': 'PART', 'verb_form': '', 'tense': ''},
         # {'id_token': 5609, 'token': '.', 'lemma': '.', 'pos': 'PUNCT', 'verb_form': '', 'tense': ''},
         # {'id_token': 5610, 'token': 'Die', 'lemma': 'der', 'pos': 'DET', 'verb_form': '', 'tense': ''},
         # {'id_token': 5611, 'token': 'Kinder', 'lemma': 'Kind', 'pos': 'NOUN', 'verb_form': '', 'tense': ''},
         # {'id_token': 5612, 'token': 'freuen', 'lemma': 'freuen', 'pos': 'VERB', 'verb_form': ['Fin'], 'tense': ['Pres']},
         # {'id_token': 5613, 'token': 'sich', 'lemma': 'sich', 'pos': 'PRON', 'verb_form': '', 'tense': ''},
         # {'id_token': 5614, 'token': ',', 'lemma': ',', 'pos': 'PUNCT', 'verb_form': '', 'tense': ''},
         # {'id_token': 5615, 'token': 'wenn', 'lemma': 'wenn', 'pos': 'SCONJ', 'verb_form': '', 'tense': ''},
         # {'id_token': 5616, 'token': 'er', 'lemma': 'ich', 'pos': 'PRON', 'verb_form': '', 'tense': ''},


        # dict for insert subtitle_join
        id_join = id_join + 1
        join_dict.append({ 'id_join' : id_join, 'id_token' : id_token, 'id_meta' : id_meta })

    ##############################
    # insert gathered data into db
    # subtitle_token
    sql = "INSERT INTO subtitle_token ( id_token, token, lemma, pos, verb_form, tense ) VALUES ( %(id_token)s, %(token)s, %(lemma)s, %(pos)s, %(verb_form)s, %(tense)s )"

    try:
        mycursor.executemany(sql, token_dict)
        mydb.commit()
    except Exception as e:
         print("Token Error:", e )

    # subtitle_join
    sql = "INSERT INTO subtitle_join ( id_join, id_token, id_meta ) VALUES ( %(id_join)s, %(id_token)s, %(id_meta)s )"
    try:
        mycursor.executemany(sql, join_dict)
        mydb.commit()
    except Exception as e:
         print("Join Error:", e )

    ##############################
    # user feedback and statistics
    elapsed_time_file = time.process_time() - time_start_file
    elapsed_time_total = time.process_time() - time_start_total

    time_total = time_total + elapsed_time_total

    print("File #", data_counter, "completed \nProcess time (file):  ", elapsed_time_file , "\nProcess time (total): ", elapsed_time_total, "\n")
    data_counter = data_counter + 1
    ##############################

print("\n\n", data_counter, " files imported.\nOverall process time: ", time_total, "\n\nDone!")
