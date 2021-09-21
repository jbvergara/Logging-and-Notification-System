import json
import logging
import boto3
import requests

log = logging.getLogger()
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('jem_application_logs')
api_endpoint = 'https://l2jy5gomd9.execute-api.ap-southeast-1.amazonaws.com/default/jem-testfunc'
email_endpoint = 'https://iwgz4ywhd4.execute-api.ap-southeast-1.amazonaws.com/beta'
topic_arn = {
    'CRITICAL': 'arn:aws:sns:ap-southeast-1:845767040548:JEM-TOPIC-CRITICAL',
    'ERROR': 'arn:aws:sns:ap-southeast-1:845767040548:JEM-TOPIC-ERROR',
    'WARNING': 'arn:aws:sns:ap-southeast-1:845767040548:JEM-TOPIC-WARNING',
    'INFO': 'arn:aws:sns:ap-southeast-1:845767040548:JEM-TOPIC-INFO',
    'DEBUG': 'arn:aws:sns:ap-southeast-1:845767040548:JEM-TOPIC-DEBUG'
}


def lambda_handler(event, context):
    #Obtain and Check Input Parameters
    try:
        #Obtain Input Parameters
        log_level = event['body']['log_level'].upper()
        message = event['body']['message']
        details = event['body']['details']
        source_application = event['body']['source_application']
        unique_id = event['body']['id']
        #Check if Log Level is valid
        getattr(
            logging,
            log_level
        )
        
    #Error Handling
    except KeyError:
        log.error(f'Invalid Input Parameters')
        return {
            "statusCode": 400,
            "body": "Invalid Input Parameters",
            
        }
    except AttributeError:
        log.error('Invalid Log Level')
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Invalid Log Level"
            }),
        }
    
    
    #Critical Log
    if log_level == 'CRITICAL':
        #Send an Email
        requests.post(
            email_endpoint,
            json={
                'to': 'jemuel.vergara@globe.com.ph',
                'subject': f'[CRITICAL ERROR] - {source_application}',
                'body': f'{message} \n {details}'
            }
        )
        fan_out(
            topic_arn[log_level],
            source_application,
            unique_id,
            log_level,
            message,
            details
        )
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Alert email sent to DevOps team."
            }),
        }
    #Other Logs
    else:
        fan_out(
            topic_arn[log_level],
            source_application,
            unique_id,
            log_level,
            message,
            details
        )
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"{log_level} - Logged Successfully"
            }),
        }

def fan_out(topic_arn, source_application, unique_id, log_level, message, details):
    sns.publish(
        TopicArn = topic_arn,
        Message= message,
        Subject=f'{source_application}-{log_level}'
    )
    response = table.put_item(
        Item={
            "source_application": source_application,
            "id": unique_id,
            "log_level": log_level,
            "message": message,
            "details": details
        }
    )