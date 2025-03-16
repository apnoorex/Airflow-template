# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_failure_message(context):
    hook = TelegramHook(token='token_id', chat_id='run_id')
    dag = context['task_instance_key_str']
    run_id = context['run_id']
    
    message = f'DAG {dag} с id={run_id}: unsuccessful!'
    hook.send_message({
        'chat_id': 'run_id',
        'text': message
    })

def send_telegram_success_message(context):
    hook = TelegramHook(token='{enter your token_id}', chat_id='{enter your chat_id}')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'DAG {dag} с id={run_id}: successful!'
    hook.send_message({
        'chat_id': '{Enter your chat_id}',
        'text': message
    })