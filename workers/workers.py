import platform
from dotenv import load_dotenv
from nltk.corpus import words
from nostril import nonsense
from io import BytesIO
from PIL import Image
import pytesseract
import datetime
import psycopg2
import requests
import logging
import time
import nltk
import os
import re
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import List, Set
import aiohttp
import asyncio
import contextlib


if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('worker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def db_connect(url):
    """Establish database connection with error handling"""
    try:
        connect = psycopg2.connect(url)
        return connect
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

# Cache frequently used data
@lru_cache(maxsize=1000)
def get_english_words() -> Set[str]:
    """Cache English words for repeated use"""
    return set(words.words())

@lru_cache(maxsize=100)
def is_sensible_string(s: str) -> bool:
    """Cached check for sensible strings"""
    english_words = get_english_words()
    tokens = nltk.word_tokenize(s)
    valid_word_count = sum(1 for token in tokens if token.lower() in english_words)
    return valid_word_count / max(len(tokens), 1) >= 0.4

# Compile regex patterns once
SYMBOLS_PATTERN = re.compile(r'[@#$&€=°:()\{\}|+~/`\'\\><™©®\[\]¥*»%¢]')
DIGITS_PATTERN = re.compile(r'\d+')

async def extract_text_from_image(session: aiohttp.ClientSession, image_url: str) -> str:
    """Asynchronously extract text from image"""
    try:
        async with session.get(image_url) as response:
            if response.status != 200:
                return f"Error: HTTP {response.status}"
            
            image_data = await response.read()
            image = Image.open(BytesIO(image_data))
            return pytesseract.image_to_string(image)
    except Exception as e:
        return f"Error: {str(e)}"

def db_connect(url):
    """Establish database connection with error handling"""
    try:
        connect = psycopg2.connect(url)
        return connect
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

# Compile regex patterns once
SYMBOLS_PATTERN = re.compile(r'[@#$&€=°:()\{\}|+~/`\'\\><™©®\[\]¥*»%¢]')
DIGITS_PATTERN = re.compile(r'\d+')

async def extract_text_from_image(session: aiohttp.ClientSession, image_url: str) -> str:
    """Asynchronously extract text from image"""
    try:
        async with session.get(image_url) as response:
            if response.status != 200:
                return f"Error: HTTP {response.status}"
            
            image_data = await response.read()
            image = Image.open(BytesIO(image_data))
            return pytesseract.image_to_string(image)
    except Exception as e:
        return f"Error: {str(e)}"

async def process_page(session: aiohttp.ClientSession, page: str, idx: int) -> tuple:
    """Process a single page asynchronously"""
    try:
        # Extract text
        extracted = await extract_text_from_image(session, page)
        if extracted.startswith("Error:"):
            return idx, None, f"PAGE {idx+1} ERROR: {extracted}"

        # Format and clean text
        formatted = [i.replace('\n', ' ') for i in extracted.split('\n\n')]
        if not formatted:
            return idx, None, f"PAGE {idx+1} ERROR: Empty text after formatting"

        # Clean text
        cleaned = [DIGITS_PATTERN.sub('', SYMBOLS_PATTERN.sub('', text)) for text in formatted]
        cleaned = [txt.strip() for txt in cleaned if txt.strip()]  # Remove empty strings

        if not cleaned:
            return idx, None, f"PAGE {idx+1} ERROR: No text after cleaning"

        return idx, cleaned, None

    except Exception as e:
        return idx, None, f"PAGE {idx+1} ERROR: {str(e)}"

async def extraction_engine(url: str, manga_id: str) -> str:
    """Optimized extraction engine using async IO"""
    try:
        # Use a single connection for the database query
        conn = db_connect(url)
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT content FROM chapters WHERE id = '{manga_id}'")
                web_pages = cursor.fetchall()[0][0]
        finally:
            conn.close()

        async with aiohttp.ClientSession() as session:
            # Process pages concurrently
            tasks = [process_page(session, page, idx) for idx, page in enumerate(web_pages)]
            results = await asyncio.gather(*tasks)

        # Combine results
        text_pieces = []
        errors = []
        
        for idx, text, error in sorted(results, key=lambda x: x[0]):
            if error:
                errors.append(error)
            elif text:
                text_pieces.extend(text)

        if not text_pieces and errors:
            return f"ERROR: {' | '.join(errors)}"
        
        if not text_pieces:
            return "ERROR: All pages empty or invalid"

        final_text = ' | '.join(f"new page - {piece}" for piece in text_pieces)
        
        if errors:
            final_text += f" | PARTIAL SUCCESS - Some pages failed: {' | '.join(errors)}"

        return final_text.replace("'", '')

    except Exception as e:
        return f"ERROR: Critical failure - {str(e)}"

def generate_id(url):
    logger.info("Generating new worker ID...")
    connect = None
    try:
        connect = db_connect(url)
        cursor = connect.cursor()

        status = 0
        heart_beat = datetime.datetime.now().minute

        cursor.execute('SELECT * FROM vitals')
        result = cursor.fetchall()

        if result:
            cursor.execute('SELECT worker_id FROM vitals ORDER BY worker_id DESC LIMIT 1;')
            last_id = int(cursor.fetchall()[0][0])
            new_id = last_id + 1
            formatted_id = f"{new_id:09d}"
            logger.info(f"Generated new worker ID: {formatted_id} (incremented from {last_id})")
        else:
            formatted_id = f"{0:09d}"
            logger.info("Generated first worker ID: 000000000")

        cursor.execute(
            "INSERT INTO vitals (worker_id, status, heartbeat) "
            f"VALUES ('{formatted_id}', {status}, {heart_beat})"
        )
        connect.commit()
        logger.info(f"Successfully registered worker {formatted_id} in vitals table")

        return formatted_id

    except Exception as e:
        logger.error(f"Error generating worker ID: {str(e)}", exc_info=True)
        raise
    finally:
        if connect:
            connect.close()

def heartbeat(url, id):
    logger.debug(f"Updating heartbeat for worker {id}")
    connect = None
    try:
        heart_beat = datetime.datetime.now().minute
        connect = db_connect(url)
        cursor = connect.cursor()
        
        cursor.execute(f"UPDATE vitals SET heartbeat = {heart_beat} WHERE worker_id = '{id}'")
        connect.commit()
        logger.debug(f"Heartbeat updated to {heart_beat} for worker {id}")

    except Exception as e:
        logger.error(f"Failed to update heartbeat for worker {id}: {str(e)}")
    finally:
        if connect:
            connect.close()

def check_task(url, id):
    logger.info(f"Checking for tasks assigned to worker {id}")
    connect = None
    try:
        connect = db_connect(url)
        cursor = connect.cursor()

        cursor.execute(
            "SELECT manga FROM dispatcher "
            f"WHERE worker_id = '{id}' AND progress_status = '0' LIMIT 1"
        )
        manga_id = cursor.fetchall()

        if manga_id:
            logger.info(f"Found task: manga_id {manga_id[0][0]} for worker {id}")
            return manga_id[0][0]
        else:
            logger.debug(f"No tasks found for worker {id}")
            return 'empty'

    except Exception as e:
        logger.error(f"Error checking tasks: {str(e)}")
        return 'empty'
    finally:
        if connect:
            connect.close()

def add_extracted_text(url, manga_id, text, worker_id):
    logger.info(f"Adding extracted text for manga {manga_id}")
    connect = None
    try:
        connect = db_connect(url)
        cursor = connect.cursor()

        cursor.execute(
            f"UPDATE chapter_text SET status = '2', text = '{text}' "
            f"WHERE id = '{manga_id}'"
        )
        cursor.execute(
            f"UPDATE vitals SET status = '0' WHERE worker_id = '{worker_id}'"
        )
        cursor.execute(
            "UPDATE dispatcher SET progress_status = '1' "
            f"WHERE worker_id = '{worker_id}' AND manga = '{manga_id}'"
        )
        connect.commit()
        logger.info(f"Successfully updated database for manga {manga_id}")

    except Exception as e:
        logger.error(f"Error adding extracted text: {str(e)}", exc_info=True)
        raise
    finally:
        if connect:
            connect.close()

async def main():
    logger.info("Starting worker process")
    try:
        load_dotenv()
        url = os.getenv('URL')
        
        if not url:
            raise ValueError("Missing DATABASE_URL environment variable")
        
        worker_id = generate_id(url)
        logger.info(f"Worker {worker_id} initialized and ready")

        while True:
            try:
                heartbeat(url, worker_id)
                task = check_task(url, worker_id)

                if task != 'empty':
                    logger.info(f"Processing task for manga {task}")
                    extract = await extraction_engine(url, task)
                    add_extracted_text(url, task, extract, worker_id)
                    logger.info(f"Successfully completed task for manga {task}")
                else:
                    await asyncio.sleep(20)

            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}", exc_info=True)
                await asyncio.sleep(60)

    except Exception as e:
        logger.critical(f"Critical error in main: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())