from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import requests
import telebot
import re
import os

def run_():
    logging.info("Функция run_ запущена")
    try:
        url = "https://quotes15.p.rapidapi.com/quotes/random/"
        querystring = {"language_code": "ru"}
        headers = {
            "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
            "x-rapidapi-host": "quotes15.p.rapidapi.com"
        }
        TOKEN = os.environ.get('BOT_TOKEN')
        CHANNEL_ID = '@tvoy_znakkk'
        bot = telebot.TeleBot(TOKEN)

        while True:
            try:
                response = requests.get(url, headers=headers, params=querystring)
                new_post = response.json()
                post_content = new_post['content']
                if len(post_content) < 100 or re.search(r'Россия|России', post_content, re.IGNORECASE):
                    break
            except Exception as e:
                logging.error(f"Ошибка при запросе: {e}")
                continue

        bot.send_message(CHANNEL_ID, post_content)
        logging.info(f"Сообщение отправлено в канал: {post_content}")

    except Exception as e:
        logging.error(f"Ошибка при выполнении функции: {e}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),
}

dag = DAG(
    dag_id='bot_dag',
    default_args=default_args,
    description='DAG for running Telegram bot',
    schedule_interval='16 8,12,17 * * *',  # Запуск в 11:16, 15:16 и 20:16
    catchup=False,
)

run_bot = PythonOperator(
    task_id='run_bot_notebook',
    python_callable=run_,
    dag=dag,
)

run_bot