from dotenv import load_dotenv
from nltk.corpus import words
from nostril import nonsense
from io import BytesIO
from PIL import Image
import pytesseract
import datetime
import psycopg2
import requests
import time
import nltk
import os
import re


# obtain worker id
def generate_id(url):
    connect = psycopg2.connect(url)
    cursor = connect.cursor()

    # set idle status - 0
    status = 0
    heart_beat = datetime.datetime.now().minute

    cursor.execute('''
                    SELECT * FROM vitals
                    ''')
    result = cursor.fetchall()


    # generate new unique id
    if result:
        cursor.execute('SELECT worker_id FROM vitals ORDER BY worker_id DESC LIMIT 1;')
        last_id = int(cursor.fetchall()[0][0])
        new_id = last_id + 1
        formatted_id = f"{new_id:09d}"
    else:
        id = 0
        formatted_id = f"{id:09d}"

    # update vitals table
    cursor.execute(f'''
                    INSERT INTO vitals (worker_id, status, heartbeat) VALUES ('{formatted_id}', {status}, {heart_beat})
                    ''')
    connect.commit()

    print(f'Worker: {formatted_id} successfully created')

    return formatted_id


# update own vitals
def heartbeat(url, id):
    print('Updating vitals ///')
    # get current time for heartbeat
    heart_beat = datetime.datetime.now().minute

    # updates vitals table
    connect = psycopg2.connect(url)
    cursor = connect.cursor()
    cursor.execute(f'''
                    UPDATE vitals SET heartbeat = {heart_beat} WHERE worker_id = '{id}'
                    ''')
    connect.commit()


# check for task assignment - returns chapter id to work on
def check_task(url, id):
    print('Checking for task...')
    connect = psycopg2.connect(url)
    cursor = connect.cursor()

    # check for a task with the same worker id
    cursor.execute(f'''
                    SELECT manga FROM dispatcher WHERE worker_id = '{id}' AND progress_status = '0' LIMIT 1;
                    ''')
    manga_id = cursor.fetchall()

    # if task is available, return chapter id
    if manga_id:
        return manga_id[0][0]
    else:
        return('empty')


# extract text from image
def extract_text_from_image(image_url):
    try:
        response = requests.get(image_url)
        response.raise_for_status()
        image = Image.open(BytesIO(response.content))
        text = pytesseract.image_to_string(image)

        return text

    except Exception as e:
        return f"Error: {str(e)}"


# splits and formats the extracted text
def text_formatter(text: str):
    text = text.split('\n\n')
    new_text = []
    for i in text:
        new_text.append(i.replace('\n', ' '))
    return new_text


# remove unwanted characters
def char_remover(text):
    symbols_to_remove = ['@', '#', '$', 
                    '&', '€', '=', '°',
                    ':', '(', ')', 
                    '{', '}', '|', 
                    '+', '~', '/', 
                    '`', '‘', '\\', 
                    '>', '<', '™', 
                    '©', '®', '[', 
                    ']', '¥', '*', 
                    '»', '%', '¢']
    
    pattern = "[" + re.escape("".join(symbols_to_remove)) + "]"

    for idx_1, k in enumerate(text):
            text[idx_1] = re.sub(pattern, '', text[idx_1])
            text[idx_1] = re.sub(r'\d', '', text[idx_1])

    return text


# check if the strings make sense in english
def clean_text(txt):
    unwanted_chars = [
    ' ', '  ', '?', '!', '...', '.', ':', ',', '-', '_', '™', '“', '”', '—', "'", '"', '¢'
    ]

    for idx_1, sentence in enumerate(txt):
        for char in unwanted_chars:
            sentence = sentence.replace(char, '')
        
        # handle short sentences
        if len(sentence) < 7:
            chars = len(sentence)
            difference = 8 - chars
            new_text = sentence + ('a' * difference)
            if nonsense(new_text):
                txt.pop(idx_1)
        else:
            if nonsense(sentence):
                txt.pop(idx_1)

    return txt


# double check if the strings make sense
def is_sensible_string(s):
    english_words = set(words.words())
    tokens = nltk.word_tokenize(s)
    valid_word_count = sum(1 for token in tokens if token.lower() in english_words)

    return valid_word_count / max(len(tokens), 1) >= 0.4


# filter strings 
def filter_sensible_strings(strings):
    return [s for s in strings if is_sensible_string(s)]


# complete text extraction function
def extraction_engine(url, id):
    connect = psycopg2.connect(url)
    cursor = connect.cursor()

    cursor.execute(f'''
                    SELECT chapters FROM chapters WHERE id = '{id}';
                    ''')
    
    web_pages = cursor.fetchall()[0][0]

    text = []
    temp = ''
    final = ''

    for page in web_pages:
        text.append(filter_sensible_strings(clean_text(char_remover(text_formatter(extract_text_from_image(page))))))

    # format extracted text before inserting it in the table
    for page in text:
        if page:
            for string in page:
                temp += 'new page - '
                temp += string
            final += temp
            final += ' | '
            temp = ''
    
    final = final.replace("'", '')

    return final


# add text to table - set worker status to idle
def add_extracted_text(url, manga_id, text, worker_id):
    connect = psycopg2.connect(url)
    cursor = connect.cursor()

    # update manga chapter status to complete, and insert extracted text
    cursor.execute(f'''
                    UPDATE chapter_text SET status = '2', text = '{text}' WHERE id = '{manga_id}';
                    ''')
    connect.commit()

    # update worker status to idle once text has been added
    cursor.execute(f'''
                    UPDATE vitals SET status = '0' WHERE worker_id = '{worker_id}';
                    ''')
    connect.commit()

    # update dispatcher task status to complete once text has been added
    cursor.execute(f'''
                    UPDATE dispatcher SET progress_status = '1' WHERE worker_id = '{worker_id}' AND manga = '{manga_id}';
                    ''')
    connect.commit()


def main():
    load_dotenv()
    url = os.environ.get('URL')

    worker_id = generate_id(url)

    while True:
        ''' Update every 20 seconds, except when working... '''
        heartbeat(url, worker_id)
        # manga id
        task = check_task(url, worker_id)

        if task != 'empty':
            print('Executing task...')
            # complete task
            extract = extraction_engine(url, task)
            # insert text
            add_extracted_text(url, task, extract, worker_id)
        else:
            print('     No task found +')

        time.sleep(20)


if __name__ == '__main__':
    main()
