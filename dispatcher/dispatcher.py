from email.mime.text import MIMEText
from dotenv import load_dotenv
import datetime
import psycopg2
import smtplib
import logging
import time
import os

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dispatcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
SENDER_EMAIL = os.environ.get('EMAIL')
RECEIVER_EMAIL = os.environ.get('EMAIL')

def db_connect(url):
    """Establish database connection with error handling"""
    try:
        connect = psycopg2.connect(url)
        return connect
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def check_vitals(url):
    logger.info('Starting vitals check...')
    try:
        connect = db_connect(url)
        cursor = connect.cursor()

        cursor.execute(f'SELECT worker_id, heartbeat FROM vitals WHERE status != \'-1\'')
        all_workers_details = cursor.fetchall()
        logger.info(f"Found {len(all_workers_details)} active workers")

        dead_idx = []
        dead_workers = []
        current_time = int(datetime.datetime.now().minute)
        
        for idx, (worker_id, heartbeat) in enumerate(all_workers_details):
            old_time = int(heartbeat)
            logger.debug(f"Checking worker {worker_id} - Last heartbeat: {old_time}, Current time: {current_time}")

            # Offline worker logic with detailed logging
            if current_time == 0:
                if 58 <= old_time <= 59 or old_time == 0:
                    logger.debug(f"Worker {worker_id} alive (edge case at minute 0)")
                else:
                    dead_idx.append(idx)
                    logger.warning(f"Worker {worker_id} appears dead at minute 0")
            elif current_time == 1:
                if old_time == 59 or 0 <= old_time <= 1:
                    logger.debug(f"Worker {worker_id} alive (edge case at minute 1)")
                else:
                    dead_idx.append(idx)
                    logger.warning(f"Worker {worker_id} appears dead at minute 1")
            else:
                if (current_time - 2) <= old_time <= current_time:
                    logger.debug(f"Worker {worker_id} alive (normal case)")
                else:
                    dead_idx.append(idx)
                    logger.warning(f"Worker {worker_id} appears dead (normal case)")

        if dead_idx:
            for index in dead_idx:
                worker_id = all_workers_details[index][0]
                dead_workers.append(worker_id)
                try:
                    # Update vitals
                    cursor.execute(f"UPDATE vitals SET status = '-1' WHERE worker_id = '{worker_id}'")
                    # Update dispatcher
                    cursor.execute(f"UPDATE dispatcher SET worker_status = 'offline' WHERE worker_id = '{worker_id}'")
                    connect.commit()
                    logger.info(f"Successfully marked worker {worker_id} as offline")
                except psycopg2.Error as e:
                    logger.error(f"Database error while updating dead worker {worker_id}: {str(e)}")
                    connect.rollback()

            return dead_workers
        else:
            logger.info("All workers are alive")
            return 'no casualties'

    except Exception as e:
        logger.error(f"Error in check_vitals: {str(e)}", exc_info=True)
        raise
    finally:
        if 'connect' in locals():
            connect.close()

def assignments(url):
    logger.info('Starting task assignment process...')
    try:
        connect = db_connect(url)
        cursor = connect.cursor()

        # Get uncompleted chapters
        cursor.execute('SELECT id FROM chapter_text WHERE status = \'0\'')
        incomplete = cursor.fetchall()
        logger.info(f"Found {len(incomplete)} incomplete chapters")

        # Get idle workers
        cursor.execute('SELECT worker_id FROM vitals WHERE status = \'0\'')
        available_workers = cursor.fetchall()
        logger.info(f"Found {len(available_workers)} available workers")

        for idx, worker in enumerate(available_workers):
            try:
                if idx >= len(incomplete):
                    logger.info("No more incomplete chapters to assign")
                    break

                worker_id = worker[0]
                chapter_id = incomplete[idx][0]
                
                # Insert into dispatcher
                cursor.execute(
                    "INSERT INTO dispatcher (worker_id, manga, progress_status, worker_status) "
                    f"VALUES ('{worker_id}', '{chapter_id}', '0', 'online')"
                )
                
                # Update chapter status
                cursor.execute(f"UPDATE chapter_text SET status = '1' WHERE id = '{chapter_id}'")
                
                # Update worker status
                cursor.execute(f"UPDATE vitals SET status = '1' WHERE worker_id = '{worker_id}'")
                
                connect.commit()
                logger.info(f"Successfully assigned chapter {chapter_id} to worker {worker_id}")
                
            except psycopg2.Error as e:
                logger.error(f"Database error while assigning task to worker {worker_id}: {str(e)}")
                connect.rollback()
                
    except Exception as e:
        logger.error(f"Error in assignments: {str(e)}", exc_info=True)
        raise
    finally:
        if 'connect' in locals():
            connect.close()

def death_notification(url, dead_workers, sender, receiver, password):
    logger.info('Preparing offline worker notification...')
    try:
        connect = db_connect(url)
        cursor = connect.cursor()

        cursor.execute('SELECT COUNT(*) FROM vitals WHERE status != \'-1\'')
        available_workers = cursor.fetchall()[0][0]
        logger.info(f"Current available workers: {available_workers}")

        if len(dead_workers) == 1:
            message = f'Worker {dead_workers} is out of service.\n\n'
        else:
            message = f'{len(dead_workers)} workers are offline 🚧🔧\n\n'
            for worker in dead_workers:
                message += f'worker {worker},\n'
            message += 'are out of service.\n\n'

        message += f'There are {available_workers} workers left online 🤖'

        msg = MIMEText(message)
        msg['Subject'] = '⚠️☠️🚨ONE OR MORE WORKERS ARE DOWN⚠️☠️🚨'
        msg['From'] = sender
        msg['To'] = receiver

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
            smtp_server.login(sender, password)
            smtp_server.sendmail(sender, receiver, msg.as_string())
            logger.info(f"Successfully sent notification email about {len(dead_workers)} dead workers")

    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email notification: {str(e)}")
    except Exception as e:
        logger.error(f"Error in death_notification: {str(e)}", exc_info=True)
    finally:
        if 'connect' in locals():
            connect.close()

def reassignment(url, dead_w):
    logger.info('Starting task reassignment for dead workers...')
    try:
        connect = db_connect(url)
        cursor = connect.cursor()
        reset_mangas = []

        for worker in dead_w:
            try:
                cursor.execute(
                    "SELECT manga FROM dispatcher WHERE progress_status = '0' "
                    f"AND worker_id = '{worker}'"
                )
                task = cursor.fetchall()
                if task:
                    reset_mangas.extend(task[0])
                    logger.info(f"Found incomplete task {task[0][0]} from dead worker {worker}")

            except psycopg2.Error as e:
                logger.error(f"Error checking tasks for dead worker {worker}: {str(e)}")
                continue

        if reset_mangas:
            for manga in reset_mangas:
                try:
                    cursor.execute(f"UPDATE chapter_text SET status = '0' WHERE id = '{manga}'")
                    connect.commit()
                    logger.info(f"Successfully reset manga {manga} for reassignment")
                except psycopg2.Error as e:
                    logger.error(f"Error resetting manga {manga}: {str(e)}")
                    connect.rollback()

    except Exception as e:
        logger.error(f"Error in reassignment: {str(e)}", exc_info=True)
        raise
    finally:
        if 'connect' in locals():
            connect.close()

def main():
    logger.info('Initiating Dispatcher...')
    try:
        load_dotenv()
        url = os.environ.get('URL')
        email_password = os.environ.get('PASSWORD')
        
        if not url:
            logger.error("Database URL not found in environment variables")
            raise ValueError("Missing DATABASE_URL environment variable")
            
        if not email_password:
            logger.warning("Email password not found in environment variables")

        rep = 0
        offline_history = []
        workers_in_email = []

        dead_workers = check_vitals(url)
        
        if isinstance(dead_workers, list):
            reassignment(url, dead_workers)
            new_dead_workers = [w for w in dead_workers if w not in offline_history]
            if new_dead_workers:
                workers_in_email.extend(new_dead_workers)
                # if email_password:
                    # death_notification(url, workers_in_email, SENDER_EMAIL, RECEIVER_EMAIL, email_password)
                workers_in_email = []

        while True:
            try:
                assignments(url)
                time.sleep(20)

                if rep == 3:
                    dead_workers = check_vitals(url)
                    if isinstance(dead_workers, list):
                        reassignment(url, dead_workers)
                        new_dead_workers = [w for w in dead_workers if w not in offline_history]
                        if new_dead_workers:
                            workers_in_email.extend(new_dead_workers)
                            # if email_password:
                                # death_notification(url, workers_in_email, SENDER_EMAIL, RECEIVER_EMAIL, email_password)
                            workers_in_email = []
                            offline_history.extend(new_dead_workers)
                    rep = 0
                rep += 1

            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}", exc_info=True)
                time.sleep(10)  # Wait before retrying

    except Exception as e:
        logger.critical(f"Critical error in main: {str(e)}", exc_info=True)
        raise

if __name__ == '__main__':
    main()