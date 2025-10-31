from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import time, os, boto3, requests

# ==============================
# AWS and project configuration
# ==============================
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ACCOUNT_ID = "440848399208"  # class account that owns the queues
EXPECTED = 21                # number of messages per assignment instructions
TIMEOUT = 20 * 60            # overall safety timeout (20 min)

# hardcoded UVA ID and queue URLs for this project
UVA_ID = "csg7su"
SUBMIT_Q = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/dp2-submit"

# helper function to get an SQS client with explicit region
def sqs():
    return boto3.client("sqs", region_name=REGION)

def populate_queue(**context):
    """Task 1: Populate the SQS queue with message fragments"""
    uvaid = context['dag_run'].conf.get('uvaid', UVA_ID) if context['dag_run'].conf else UVA_ID
    
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"
    print(f"Calling scatter API for {uvaid} ...")
    
    r = requests.post(url, timeout=30)
    r.raise_for_status()
    payload = r.json()
    qurl = payload["sqs_url"]
    
    print(f"Queue created for {uvaid}: {qurl}")
    
    # Store queue URL in XCom for next task
    return {
        'queue_url': qurl,
        'uvaid': uvaid
    }

def collect_messages(**context):
    """Task 2: Collect all message fragments from SQS"""
    # Get data from previous task
    ti = context['task_instance']
    populate_data = ti.xcom_pull(task_ids='populate_queue_task')
    qurl = populate_data['queue_url']
    uvaid = populate_data['uvaid']
    
    client = sqs()
    pairs = []                 # store (order_no, word)
    last_attr_check = 0        # track last attribute query time
    attr_every = 30            # seconds between queue attribute checks
    long_poll = 20             # long-poll duration (seconds)
    start = time.time()

    print(f"Starting to collect {EXPECTED} message fragments from {qurl}")

    while len(pairs) < EXPECTED:
        resp = client.receive_message(
            QueueUrl=qurl,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=long_poll,
            MessageAttributeNames=["All"],
        )
        msgs = resp.get("Messages", [])

        # Parse attributes and delete each message
        for m in msgs:
            a = m.get("MessageAttributes") or {}
            if "order_no" in a and "word" in a:
                order = int(a["order_no"]["StringValue"])
                word  = a["word"]["StringValue"]
                pairs.append((order, word))
                print(f"Collected {len(pairs)}/{EXPECTED} · latest='{word}' (#{order})")
            else:
                print("Message missing expected attributes; deleting anyway to avoid dangling visibility.")

            client.delete_message(QueueUrl=qurl, ReceiptHandle=m["ReceiptHandle"])

        # Precision monitoring every 30 seconds
        now = time.time()
        if now - last_attr_check >= attr_every:
            try:
                attrs = client.get_queue_attributes(
                    QueueUrl=qurl,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed",
                    ],
                )["Attributes"]
                vis  = int(attrs.get("ApproximateNumberOfMessages", "0"))
                infl = int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
                dly  = int(attrs.get("ApproximateNumberOfMessagesDelayed", "0"))
                print(f"Status · visible={vis} inflight={infl} delayed={dly} · collected={len(pairs)}/{EXPECTED}")
                last_attr_check = now
            except Exception as e:
                print(f"Error checking queue attributes: {e}")

        if now - start > TIMEOUT:
            raise Exception(f"Timeout: only collected {len(pairs)}/{EXPECTED} within {TIMEOUT}s")

    if not pairs:
        raise ValueError("No message fragments received.")

    # Return collected pairs for next task
    return {
        'pairs': pairs,
        'uvaid': uvaid
    }

def reassemble_and_submit(**context):
    """Task 3: Reassemble the phrase and submit to submission queue"""
    # Get data from previous task
    ti = context['task_instance']
    collect_data = ti.xcom_pull(task_ids='collect_messages_task')
    pairs = collect_data['pairs']
    uvaid = collect_data['uvaid']
    
    # Reassemble the phrase
    phrase = " ".join(w for _, w in sorted(pairs, key=lambda t: t[0]))
    print(f"Phrase reconstructed: {phrase}")

    # Submit to submission queue
    client = sqs()
    resp = client.send_message(
        QueueUrl=SUBMIT_Q,
        MessageBody="solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": uvaid},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": "airflow"},
        },
    )
    code = resp["ResponseMetadata"]["HTTPStatusCode"]
    print(f"Submitted. HTTP {code}")
    print("PHRASE:", phrase)
    print("SUBMIT_STATUS:", code)
    
    return {
        'phrase': phrase,
        'status_code': code
    }

# Define the DAG
default_args = {
    'owner': 'csg7su',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dp2_quote_assembler',
    default_args=default_args,
    description='DS3022 DP2 - SQS Message Fragment Assembler',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['ds3022', 'sqs', 'data-pipeline'],
)

# Define the tasks
populate_task = PythonOperator(
    task_id='populate_queue_task',
    python_callable=populate_queue,
    dag=dag,
)

collect_task = PythonOperator(
    task_id='collect_messages_task',
    python_callable=collect_messages,
    dag=dag,
)

submit_task = PythonOperator(
    task_id='reassemble_and_submit_task',
    python_callable=reassemble_and_submit,
    dag=dag,
)

# Set task dependencies
populate_task >> collect_task >> submit_task
