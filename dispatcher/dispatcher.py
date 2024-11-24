from email.mime.text import MIMEText
from dotenv import load_dotenv
import datetime
import psycopg2
import smtplib
import time
import os

load_dotenv()
SENDER_EMAIL = os.environ.get('EMAIL')
RECEIVER_EMAIL = os.environ.get('EMAIL')


# supervise workers vitals
def check_vitals(url):
    print('checking vitals...')
    ''' If heartbeat is over 2 minutes ago, consider worker dead '''
    connect = psycopg2.connect(url)
    cursor = connect.cursor()

    # get online workers
    cursor.execute(f'''
                    SELECT worker_id, heartbeat FROM vitals WHERE status != '-1'
                    ''')

    all_workers_details = cursor.fetchall()

    dead_idx = []
    dead_workers = []
    # exceptions = [0, 1]  # 0 -> 58 <= x <= 59 | 1 -> x = 59, 0 <= x <= 1  

    for idx, i in enumerate(all_workers_details):
        # gets worker last heartbeat
        old_time = int(i[1])
        # gets current time
        time = int(datetime.datetime.now().minute)

        # offline worker logic
        if time == 0:
            if 58 <= old_time <= 59 or old_time == 0:
                continue
            else:
                dead_idx.append(idx)
        elif time == 1:
            if old_time == 59 or 0 <= old_time <= 1:
                continue
            else:
                dead_idx.append(idx)
        else:
            if (time - 2) <= old_time <= time:
                continue
            else:
                dead_idx.append(idx)

    # if any workers are offline, execute these lines
    if dead_idx:
        for index in dead_idx:
            # append worker id
            dead_workers.append(all_workers_details[index][0])
            # updates database to reflect that the worker is offline
            cursor.execute(f'''
                            UPDATE vitals SET status = '-1' WHERE worker_id = '{all_workers_details[index][0]}'
                            ''')
            connect.commit()

            # update dispatcher task log
            cursor.execute(f'''
                            UPDATE dispatcher SET worker_status = 'offline' WHERE worker_id = '{all_workers_details[index][0]}'
                            ''')
            connect.commit()

        return dead_workers
    else:
        return 'no casualties'


# check for chapters to complete
def assignments(url):
    print('     Assigning tasks...')
    connect = psycopg2.connect(url)
    cursor = connect.cursor()

    # gets uncompleted chapters id 
    cursor.execute('''
                    SELECT id FROM chapter_text WHERE status = '0';
                    ''')
    incomplete = cursor.fetchall()

    # checks how many workers are idle
    cursor.execute('''
                    SELECT worker_id FROM vitals WHERE status = '0';
                    ''')
    available_workers = cursor.fetchall()

    # assigns tasks to workers and updates chapters status
    for idx, worker in enumerate(available_workers):
        cursor.execute(f'''
        INSERT INTO dispatcher (worker_id, manga, progress_status, worker_status) VALUES ('{worker[0]}', '{incomplete[idx][0]}', '0', 'online');
        ''')
        connect.commit()

        # updates chapter_text status to in progress
        cursor.execute(f'''
        UPDATE chapter_text SET status = '1' WHERE id = '{incomplete[idx][0]}';
        ''')
        connect.commit()

        # updates vitals status to working...
        cursor.execute(f'''
        UPDATE vitals SET status = '1' WHERE worker_id = '{worker[0]}';
        ''')
        connect.commit()


# offline workers notification
def death_notification(url, dead_workers, sender, receiver, password):
    print('--- Sending Offline notification ---')
    subject = '⚠️☠️🚨ONE OR MORE WORKERS ARE DOWN⚠️☠️🚨'

    connect = psycopg2.connect(url)
    cursor = connect.cursor()

    # gets number of offline workers
    cursor.execute('''
                SELECT COUNT(*) FROM vitals WHERE status != '-1';
                ''')
    available_workers = cursor.fetchall()[0][0]

    # switch message depending on how many workers are offline
    if len(dead_workers) == 1:
        message = f'Worker {dead_workers} is out of service.\n\n'
    else:
        message = f'{len(dead_workers)} workers are offline 🚧🔧\n\n'
        for worker in dead_workers:
            message += f'worker {worker},\n'
        message += 'are out of service.\n\n'

    message += f'There are {available_workers} workers left online 🤖'

    # compose the message
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver # ', '.join(recipients)

    # send email
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, receiver, msg.as_string())


# reassign task if worker goes offline
def reassignment(url, dead_w):
    print('         Reassigning tasks...')
    connect = psycopg2.connect(url)
    cursor = connect.cursor()

    # chapters to be reset to status 0
    reset_mangas = []

    # check for tasks assigned to offline workers
    for worker in dead_w:
        cursor.execute(f'''
                        SELECT manga FROM dispatcher WHERE progress_status = '0' and worker_id = '{worker}';
                        ''')
        task = cursor.fetchall()
        # if the any task was assigned to an offline worker, execute the next lines
        if task:
            reset_mangas.append(task[0][0])

    # reset manga status
    if reset_mangas:
        for manga in reset_mangas:
            cursor.execute(f'''
            UPDATE chapter_text SET status = '0' WHERE id = '{manga}';
            ''')
            connect.commit()


def main():
    print('Initiating Dispatcher...\n')
    load_dotenv()
    url = os.environ.get('URL')
    email_password = os.environ.get('PASSWORD')
    rep = 0
    offline_history = []
    workers_in_email = []

    dead_workers = check_vitals(url)

    # if any workers are offline, run reassignment and notification
    if type(dead_workers) == list:
        reassignment(url, dead_workers)
        for worker in dead_workers:
            if worker not in offline_history:
                workers_in_email.append(worker)
        death_notification(url, workers_in_email, SENDER_EMAIL, RECEIVER_EMAIL, email_password)
        workers_in_email = []

    # runs the dispatcher indefinitely
    while True:
        assignments(url)
        time.sleep(20)

        # check for worker vitals roughly every 60 seconds
        if rep == 3:
            dead_workers = check_vitals(url)

            # if any workers are offline, run reassignment and notification
            if type(dead_workers) == list:
                reassignment(url, dead_workers)
                for worker in dead_workers:
                    if worker not in offline_history:
                        workers_in_email.append(worker)
                death_notification(url, workers_in_email, SENDER_EMAIL, RECEIVER_EMAIL, email_password)
                workers_in_email = []
                for worker in dead_workers:
                    offline_history.append(worker)
            rep = 0
        rep += 1


if __name__ == '__main__':
    main()
