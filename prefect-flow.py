from prefect import flow, get_run_logger
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
DEFAULT_SOURCE_Q = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/{UVA_ID}"

# helper function to get an SQS client with explicit region
def sqs():
    return boto3.client("sqs", region_name=REGION)


@flow(name="dp2-quote-assembler", log_prints=True)
def dp2(uvaid: str = UVA_ID, populate: bool = False):
    """
    DS3022 DP2 Prefect flow.
    - If populate=True, calls scatter API once to repopulate the queue for `uvaid`.
    - Then polls SQS until all fragments are received, reassembles the phrase, and submits to dp2-submit.
    """
    log = get_run_logger()

    # -----------------
    # Task 1 — Populate (optional; OFF by default)
    # -----------------
    if populate:
        url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"
        log.info(f"Calling scatter API for {uvaid} ...")
        r = requests.post(url, timeout=30)
        r.raise_for_status()
        payload = r.json()
        qurl = payload["sqs_url"]
        log.info(f"Queue created for {uvaid}: {qurl}")
    else:
        # Use the known class queue URL pattern for this assignment
        qurl = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT_ID}/{uvaid}"
        log.info(f"Using existing queue for {uvaid}: {qurl}")

    # -----------------
    # Task 2 — Collect
    # -----------------
    client = sqs()
    pairs = []                 # store (order_no, word)
    last_attr_check = 0        # track last attribute query time
    attr_every = 30            # seconds between queue attribute checks
    long_poll = 20             # long-poll duration (seconds)
    start = time.time()

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
                log.info(f"Collected {len(pairs)}/{EXPECTED} · latest='{word}' (#{order})")
            else:
                log.info("Message missing expected attributes; deleting anyway to avoid dangling visibility.")

            client.delete_message(QueueUrl=qurl, ReceiptHandle=m["ReceiptHandle"])

        # Precision monitoring every 30 seconds
        now = time.time()
        if now - last_attr_check >= attr_every:
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
            log.info(f"Status · visible={vis} inflight={infl} delayed={dly} · collected={len(pairs)}/{EXPECTED}")
            last_attr_check = now

        if now - start > TIMEOUT:
            raise TimeoutError(f"Timeout: only collected {len(pairs)}/{EXPECTED} within {TIMEOUT}s")

    if not pairs:
        raise ValueError("No message fragments received.")

    # -----------------
    # Task 3 — Reassemble and submit
    # -----------------
    phrase = " ".join(w for _, w in sorted(pairs, key=lambda t: t[0]))
    log.info(f"Phrase reconstructed: {phrase}")

    resp = client.send_message(
        QueueUrl=SUBMIT_Q,
        MessageBody="solution",
        MessageAttributes={
            "uvaid":    {"DataType": "String", "StringValue": uvaid},
            "phrase":   {"DataType": "String", "StringValue": phrase},
            "platform": {"DataType": "String", "StringValue": "prefect"},
        },
    )
    code = resp["ResponseMetadata"]["HTTPStatusCode"]
    log.info(f"Submitted. HTTP {code}")
    print("PHRASE:", phrase)
    print("SUBMIT_STATUS:", code)


# entry point for local testing
if __name__ == "__main__":
    # First run: set populate=True once to seed the queue, then set it back to False
    dp2(uvaid="csg7su", populate=True)
