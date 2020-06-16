import boto3
from ast import literal_eval
from botocore.exceptions import ClientError
from constants import (
    CACHE_TABLE_NAME, QUERY_CACHE_NAME,
    QUERY_CACHE_NAME_WRITE_THROUGHPUT,
    QUERY_CACHE_NAME_READ_THROUGHPUT,
    CACHE_TABLE_NAME_WRITE_THROUGHPUT,
    CACHE_TABLE_NAME_READ_THROUGHPUT
)

class DydbRequestManager:
    
    def __init__(self, aws_access_key, aws_secret_key, region='us-east-2'):
        self.client = boto3.client(
                        'dynamodb',
                        aws_access_key_id=aws_access_key, 
                        aws_secret_access_key=aws_secret_key, 
                        region_name=region
                    )
       # self.clear_cache()
        
    def load_table_key_mapping(self, objects, key):
        objects = list(set(objects))
        chunks = [objects[x:x+20] for x in range(0, len(objects), 20)] 
        for chunk in chunks:
            self.client.batch_write_item(RequestItems=generate_cache_table_request(chunk, key))

    def load_query_cache(self, key, messages):
        response = self.client.put_item(
            Item={
                'request_key': {
                    'S' : key
                    },
                'message' : {
                    'S' : str(messages)
                    }
            },
            TableName=QUERY_CACHE_NAME,
        )     
        return response
    
    def retrieve_from_query_cache(self, key):
        try:
            response = self.client.get_item(
                TableName=QUERY_CACHE_NAME,
                Key={"request_key":{
                    'S' : key
                    },
                },
                AttributesToGet=["message"]
            )
        except:
            return None
        return literal_eval(response["Item"]["message"]["S"])

    def clear_cache(self):
        self.rebuild_object_key_table()
        self.rebuild_query_cache_table()
        
    def rebuild_object_key_table(self):
        try:
            self.client.delete_table(TableName=CACHE_TABLE_NAME)
            waiter = self.client.get_waiter('table_not_exists')
            waiter.wait(TableName=CACHE_TABLE_NAME)
        except ClientError:
            pass
        response = self.client.create_table(
            TableName=CACHE_TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'object',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'cache_key',
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions= [
                {
                    'AttributeName': 'cache_key',
                    'AttributeType': 'S'
                },
                  {
                    'AttributeName': 'object',
                    'AttributeType': 'S'
                },
                
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': CACHE_TABLE_NAME_READ_THROUGHPUT,
                'WriteCapacityUnits': CACHE_TABLE_NAME_WRITE_THROUGHPUT
            }
        )
        return response

    def rebuild_query_cache_table(self):
        try:
            self.client.delete_table(TableName=QUERY_CACHE_NAME)
            waiter = self.client.get_waiter('table_not_exists')
            waiter.wait(TableName=QUERY_CACHE_NAME)
        except ClientError:
            pass
        response = self.client.create_table(
            TableName=QUERY_CACHE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'request_key',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions= [
                {
                    'AttributeName': 'request_key',
                    'AttributeType': 'S'
                },            
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': QUERY_CACHE_NAME_READ_THROUGHPUT,
                'WriteCapacityUnits': QUERY_CACHE_NAME_WRITE_THROUGHPUT
            }
        )
        return response
    

def cache_batch_template(obj, cache_key):
    return {'PutRequest': {'Item': {'object': {'S': obj,},'cache_key': {'S': cache_key,}}}}

def generate_cache_table_request(objects, key):
    put_requests = [cache_batch_template(obj, key)
                    for obj in objects
                    ]
    return {CACHE_TABLE_NAME: put_requests}
