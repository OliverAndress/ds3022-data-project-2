import requests

def init_queue(uva_id: str = "csg7su"):
    url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uva_id}"
    response = requests.post(url)
    
    if response.status_code != 200:
        raise RuntimeError(f"Request failed: {response.status_code} {response.text}")
    
    payload = response.json()
    print("Payload from scatter API:")
    print(payload)
    
    sqs_url = payload["sqs_url"]
    print("Your SQS queue URL is:", sqs_url)
    return sqs_url

if __name__ == "__main__":
    init_queue("csg7su")