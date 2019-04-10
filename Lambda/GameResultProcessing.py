from __future__ import print_function

import boto3
import json
        
sqs = boto3.client('sqs')
dynamodb = boto3.client('dynamodb')

ddb_table = 'GomokuPlayerInfo'
queue_url = 'https://sqs.**.amazonaws.com/**/game-result-queue'


def lambda_handler(event, context):
    if 'Records' not in event:
        return
            
    message = event['Records'][0]
    parsed = json.loads(message['body'])
        
    playername = parsed['PlayerName']
    scorediff = parsed['ScoreDiff']
    windiff = parsed['WinDiff']
    losediff = parsed['LoseDiff']
    
    dynamodb.update_item(
        TableName=ddb_table,
        Key={ 'PlayerName' : { 'S' : playername } }, 
        UpdateExpression="SET Score = Score + :score, Win = Win + :win, Lose = Lose + :lose",
        ExpressionAttributeValues={
            ':score': {
                'N': str(scorediff)
            },
            ':win': {
                'N': str(windiff)
            },
            ':lose': {
                'N': str(losediff)
            }
        }
    )
    
    receipt_handle = message['receiptHandle']
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    