"""
MIT License

Copyright (c) 2025 RCarrilloDev

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

easy_sqs.py

This script provides functionality to send messages to an AWS SQS queue. 
It reads the message body from a raw XML file and message attributes from a JSON file.
The script also allows specifying the AWS profile and queue URL as parameters.

Credits: RCarrillo Dev - https://github.com/rcarrillodev
"""

import json
import random
import sys
import configparser
import logging
import argparse
import uuid
import boto3
import sqs_extended_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# List of supported parameters in parameters.properties file
list_of_params = [
    'queue_url',
    'aws_profile',
    'message_body',
    'message_attributes',
    's3_bucket',
    'fifo_test',
    'fifo_num_of_messages']

def send_message_to_sqs(params, message_body, message_attributes):
    session = boto3.Session(profile_name=params.get('aws_profile'))
    sqs_extended_client = session.client("sqs")
    sqs_extended_client.large_payload_support = params.get('s3_bucket')
    sqs_extended_client.use_legacy_attribute = False
    sqs_extended_client.s3 = session.resource("s3")
    queue_url = params.get('queue_url')
    
    message_group_id = "msg-group-" + str(random.randint(1, 100))
    num_of_iterations = 1
    if params.get('fifo_test') == 'true':
        num_of_iterations = int(params.get('fifo_num_of_messages',1))
        logger.debug(f'num_of_iterations: {num_of_iterations}')
    for n in range(0, num_of_iterations):
        logger.info(f"sending messages in group {message_group_id}, message number: {n+1}")
        message_deduplication_id = str(uuid.uuid4())
        message_attributes["messageNum"] = {
            "DataType": "String",
            "StringValue": str(n+1)
        }
        response = None
        if queue_url.endswith('.fifo'):
            response = sqs_extended_client.send_message(QueueUrl=queue_url,MessageBody=message_body,MessageAttributes=message_attributes, MessageDeduplicationId=message_deduplication_id, MessageGroupId=message_group_id)
        else: response = sqs_extended_client.send_message(QueueUrl=queue_url,MessageBody=message_body,MessageAttributes=message_attributes)
        logger.info(f"Message sent with ID: {response['MessageId']}")
        

def load_message_body(file_path):
    """
    Load the message body from a raw XML file.

    :param file_path: Path to the XML file
    :return: Message body as a string
    """
    try:
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        sys.exit(1)

def load_message_attributes(file_path):
    """
    Load the message attributes from a JSON file.

    :param file_path: Path to the JSON file
    :return: Dictionary of message attributes
    """
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        sys.exit(1)
    except json.JSONDecodeError as error:
        logger.error(f"Error decoding JSON file: {error}")
        sys.exit(1)

def load_parameters_from_properties(file_path, section='DEFAULT'):
    """
    Load parameters from a .properties file.

    :param file_path: Path to the .properties file
    :return: Dictionary of parameters
    """
    config = configparser.ConfigParser()
    try:
        with open(file_path, 'r') as file:
            config.read_file(file)
            params = {}
            for param in list_of_params:
                params[param] = config.get(section, param, fallback=None)
            return params
    except Exception as error:
        logger.error(f"Error reading properties file: {error}")
        sys.exit(1)
        
def override_parameters_with_args(params, args):
    """
    Override parameters with command line arguments if provided.

    :param params: Dictionary of parameters from the properties file
    :param args: Command line arguments
    :return: Updated dictionary of parameters
    """
    for param in list_of_params:
        arg_value = getattr(args, param, None)
        if arg_value is not None:
            params[param] = arg_value
            logger.debug(f"Overriding parameter '{param}' with value from command line: {arg_value}")
    return params

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Send messages to AWS SQS queue')
    parser.add_argument('-p', '--properties', required=True, help='Properties file path')
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('-s', '--properties_section', default='DEFAULT', help='Section in properties file to read parameters from')
    
    for p in list_of_params:
        parser.add_argument(f'--{p}', help=f'Specify {p} parameter (overrides properties file if provided)')
    
    return parser.parse_args()

def main() -> None:
    """Main execution function."""
    try:
        args = parse_arguments()
        logger.info(f"Arguments: {args}")
        if args.debug:
            logger.setLevel(logging.DEBUG)
        
        params = load_parameters_from_properties(args.properties, section=args.properties_section)
        logger.debug(f"Loaded parameters: {params}")
        
        # Override any parameters from command line arguments
        params = override_parameters_with_args(params, args)

        message_body = load_message_body(params['message_body'])
        message_attributes = load_message_attributes(params['message_attributes'])
        
        send_message_to_sqs(params, message_body, message_attributes)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()