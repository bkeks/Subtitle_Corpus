import re
import mysql.connector
import spacy
nlp = spacy.load("de_core_news_sm") # load language model
import time
import csv

from pathlib import Path

# Connetion to database
mydb = mysql.connector.connect(
host="localhost",
user="root",
passwd="",
database="subtitle_corpus"
)

# Initiation of db connection
cursor = mydb.cursor()

# set max_allowed_packet
cursor.execute("SET GLOBAL max_allowed_packet=10000000000000")


# open csv file
file = open('past_vs_perfect-different_country.csv', 'w', newline='', encoding='utf-8') # comparison of different original languages

writer = csv.writer(file)

csv_header = ['Präteritum', 'Perfekt', 'USA', 'England', 'Deutschland', 'Andere Länder'] # comparison of different original languages


writer.writerow(csv_header)

# (34462, 7022034, '01:30:08,570', 'German', '', '', 2017, '', None, None),
# (34463, 7028366, '00:40:33,681', 'German', '', '', 2017, 'Drama', None, None),
# (34464, 7028368, '00:44:30,132', 'German', '', '', 2017, 'Drama', None, None),
# (34465, 7028374, '00:42:22,753', 'German', '', '', 2017, 'Drama', None, None),
# (34466, 7028380, '00:39:05,343', 'German', '', '', 2017, 'Drama', None, None),



############################
# get ids and build a new restricted corpus
# only get movies, not shows; skip movies with no original_language annotated
cursor.execute("SELECT id_meta FROM subtitle_meta WHERE duration > '01:00:00' and not original_language = '' and not original_language = 'N/A' and not country = '' and not genre1 = '' and not year = '' and not year = 0")

print("\nFetching metadata of all entries...\n")
db_result = cursor.fetchall()

############################
# get the tokens that belong to the metadata from the new corpus
print("\nFetching tokens...\n")
total_files = len(db_result)

# statistics
time_start_total = time.process_time()
time_total = 0
counter_files = 0

past_tense_usa = 0
perfect_tense_usa = 0

past_tense_uk = 0
perfect_tense_uk = 0

past_tense_de = 0
perfect_tense_de = 0

past_tense_other = 0
perfect_tense_other = 0

# iterate throug movies
for id in db_result:
    sentences = []
    # timer for single file
    time_start_file = time.process_time()
    counter_files += 1
    total_files = total_files - 1


    sql = "SELECT DISTINCT m.id_meta, m.id_movie, m.duration, m.translated_language, m.original_language, m.country, m.year, m.genre1, m.genre2, m.genre3, t.id_token, t.token, t.lemma, t.pos, t.verb_form, t.tense FROM subtitle_meta m LEFT JOIN subtitle_join j ON m.id_meta = j.id_meta LEFT JOIN subtitle_token t ON j.id_token = t.id_token WHERE m.id_meta = %s"

    cursor.execute(sql, id)
    joined_tables = cursor.fetchall()

    # group tokens to sentences
    word = []
    prev_pos = ""
    prev_token = ""

    for item in joined_tables:
        # item: (1, 1147427, '01:54:55,754', 'German', 'Korean', 'South Korea', 2000, 'Action', 'Drama', 'Fantasy', 3967, '...', '...', 'PUNCT', '', '')
        # ids:   0, 1,        2,              3,        4,        5,            6,     7,        8,       9,        10,    11,    12,    13,     14, 15
        if (re.match(r'^[A-Z]', item[11]) and prev_pos == "PUNCT" and item[13] != "NOUN" and item[13] != "PROPN") or ((item[13] == "NOUN" or item[13] == "PROPN") and (prev_token == "." or prev_token == "!" or prev_token == "?")):
            # sentence limit
            sentences.append(word)

            word = []
            #word.append(item[11]) # only tokens
        else:
            #word.append(item[11]) # only tokens
            word.append(item)

        prev_pos = item[13]
        prev_token = item[11]

    ############################
    # count tenses

    # word[14] -> verb_form
    # word[15] -> tense

    ############################
    # filter languages
    for sentence in sentences:
        #print(sentence)
        for word in sentence:

            if re.match(r'^USA', item[5]):
                #### English movie
                if word[15] == "Past" and word[14] == "Fin":
                    past_tense_usa += 1
                    writer.writerow(['1','0','1','0','0','0'])

                elif word[14] == "Part":
                    for word in sentence:
                        if word[15] == "Pres":
                            perfect_tense_usa += 1
                            writer.writerow(['0','1','1','0','0','0'])

            if re.match(r'^UK', item[5]):
                #### English movie
                if word[15] == "Past" and word[14] == "Fin":
                    past_tense_uk += 1
                    writer.writerow(['1','0','0','1','0','0'])

                elif word[14] == "Part":
                    for word in sentence:
                        if word[15] == "Pres":
                            perfect_tense_uk += 1
                            writer.writerow(['0','1','0','1','0','0'])

            elif re.match(r'^German', item[5]):
                #### German movie
                if word[15] == "Past" and word[14] == "Fin":
                    past_tense_de += 1
                    writer.writerow(['1','0','0','0','1','0'])

                elif word[14] == "Part":
                    for word in sentence:
                        if word[15] == "Pres":
                            perfect_tense_de += 1
                            writer.writerow(['0','1','0','0','1','0'])

            else:
                #### other languages
                if word[15] == "Past" and word[14] == "Fin":
                    past_tense_other += 1
                    writer.writerow(['1','0','0','0','0','1'])

                elif word[14] == "Part":
                    for word in sentence:
                        if word[15] == "Pres":
                            perfect_tense_other += 1
                            writer.writerow(['0','1','0','0','0','1'])
    ############################

    ############################
    # # base routine for filtering the tenses into past tense and past perfect
    # for sentence in sentences:
    #     #print(sentence)
    #     for word in sentence:
    #         if word[15] == "Past" and word[14] == "Fin":
    #             past_tense += 1
    #             writer.writerow(['1','0'])
    #
    #         elif word[14] == "Part":
    #             #writer.writerow(['0','0','0'])
    #             #particips += 1
    #             for word in sentence:
    #                 if word[15] == "Past":
    #                     perfect_tense += 1
    #                     writer.writerow(['0','1'])
    #
    #                     # for i in sentence:
    #                     #     print(i[11])
    ############################

    ##############################
    # user feedback and statistics
    elapsed_time_file = time.process_time() - time_start_file
    elapsed_time_total = time.process_time() - time_start_total

    time_total = time_total + elapsed_time_total

    print("File #", counter_files, "completed \nProcess time (file):  ", elapsed_time_file , "\nProcess time (total): ", elapsed_time_total)
    print("Files left: " + str(total_files) + "\n")

    #if counter_files >= 100:
print("Präteritum USA:           " + str(past_tense_usa))
print("Perfekt USA:              " + str(perfect_tense_usa))
print("Präteritum England:       " + str(past_tense_uk))
print("Perfekt England:          " + str(perfect_tense_uk))
print("Präteritum Deutschland:   " + str(past_tense_de))
print("Perfekt Deutschland:      " + str(perfect_tense_de))
print("Präteritum Andere Länder: " + str(past_tense_other))
print("Perfekt Andere Länder:    " + str(perfect_tense_other))
        #exit()









        ############################
        ## clean up meta data
        # split languages

        # original_languages = item[4]
        #
        # languages_split = original_languages.split(",")
        #
        # if len(languages_split) > 1:
        #
        #
        #     new_lang = str(languages_split[0] + ", " + languages_split[1])
        #
        #     print(languages_split[0] + languages_split[1]) # only note the first 2 languages
        #
            ############################
