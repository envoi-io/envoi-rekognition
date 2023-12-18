#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
import time
from threading import Thread
from urllib.parse import urlparse, parse_qs

import boto3

logger = logging.Logger('envoi-rekognition')

REKOGNITION_JOB_TYPES = [
    "celebrity-recognition",
    "content-moderation",
    "face-detection",
    "label-detection",
    "person-tracking",
    "segment-detection",
    "text-detection",
]
REKOGNITION_JOB_TYPES_ARGUMENT_CHOICES = ['all'] + REKOGNITION_JOB_TYPES

WAIT_STRATEGIES = [
    'none',
    'poll',
    'sns'
]


class InputS3Object:
    Bucket: str
    Name: str
    Version: str


class NotificationChannel:
    RoleArn: str
    SNSTopicArn: str


class CommonRequestProperties:
    ClientRequestToken: str
    JobTag: str
    NotificationChannel: NotificationChannel
    Video: InputS3Object


class AwsRekognitionHelper:

    @classmethod
    def uri_to_input_s3_object(cls, uri):
        parsed_uri = urlparse(uri)
        bucket_name = parsed_uri.netloc
        object_key = parsed_uri.path[1:]
        args_from_uri = parse_qs(parsed_uri.query)

        s3_object = {
            "Bucket": bucket_name,
            "Name": object_key
        }
        if hasattr(args_from_uri, 'version'):
            s3_object["Version"] = args_from_uri['version'][0]
        return s3_object


class AwsRekognitionGetJobHelper:

    def __init__(self, rekognition_client=None):
        self.rekognition = rekognition_client or boto3.client('rekognition')

    def get_job_status(self, job_type, job_id):
        if job_type == 'celebrity-recognition':
            return self.get_celebrity_job_status(job_id)
        elif job_type == 'content-moderation':
            return self.get_content_moderation_job_status(job_id)
        elif job_type == 'face-detection':
            return self.get_face_detection_job_status(job_id)
        elif job_type == 'label-detection':
            return self.get_label_detection_job_status(job_id)
        elif job_type == 'person-tracking':
            return self.get_person_tracking_job_status(job_id)
        elif job_type == 'segment-detection':
            return self.get_segment_detection_job_status(job_id)
        elif job_type == 'text-detection':
            return self.get_text_detection_job_status(job_id)
        else:
            raise Exception(f"Invalid job type: {job_type}")

    def get_celebrity_job_status(self, job_id):
        return self.rekognition.get_celebrity_recognition_job(JobId=job_id)

    def get_content_moderation_job_status(self, job_id):
        return self.rekognition.get_content_moderation_job(JobId=job_id)

    def get_face_detection_job_status(self, job_id):
        return self.rekognition.get_face_detection_job(JobId=job_id)

    def get_label_detection_job_status(self, job_id):
        return self.rekognition.get_label_detection_job(JobId=job_id)

    def get_person_tracking_job_status(self, job_id):
        return self.rekognition.get_person_tracking_job(JobId=job_id)

    def get_segment_detection_job_status(self, job_id):
        return self.rekognition.get_segment_detection_job(JobId=job_id)

    def get_text_detection_job_status(self, job_id):
        return self.rekognition.get_text_detection_job(JobId=job_id)


class AwsRekognitionStartJobHelper:

    def __init__(self, rekognition_client=None, common_args=None):
        self.rekognition = rekognition_client or boto3.client('rekognition')
        self.common_args = self.__class__._process_common_args(common_args) if common_args is not None else {}

    def _process_s3_object_arg(self, arg):
        #   "S3Object": {
        #       "Bucket": "string",
        #       "Name": "string",
        #       "Version": "string"
        #   }
        pass

    @classmethod
    def _process_common_args(cls,
                             common_opts):
        # client_token = common_opts.client_token if common_opts.client_token is not None else str(uuid.uuid4)

        # --input-file-uri "s3://bucket/key"
        # --notification-channel-role-arn
        # --notification-channel-sns-topic-arn

        # {
        #    "ClientRequestToken": "string",
        #    "JobTag": "string",
        #    "NotificationChannel": {
        #       "RoleArn": "string",
        #       "SNSTopicArn": "string"
        #    },
        #    "Video": {
        #       "S3Object": {
        #          "Bucket": "string",
        #          "Name": "string",
        #          "Version": "string"
        #       }
        #    }
        # }
        return common_opts

    def start_celebrity_recognition(self, job_args):
        return self.rekognition.start_celebrity_recognition(**job_args)

    def start_content_moderation(self, job_args):
        return self.rekognition.start_content_moderation(**job_args)

    def start_face_detection(self, job_args):
        return self.rekognition.start_face_detection(**job_args)

    def start_face_search(self, job_args):
        return self.rekognition.start_face_search(**job_args)

    def start_label_detection(self, job_args):
        return self.rekognition.start_label_detection(**job_args)

    def start_media_analysis(self, job_args):
        job_args.pop('JobTag', None)
        job_args['OperationsConfig'] = {
            "DetectModerationLabels": {}
        }
        job_args['OutputConfig'] = {
            "S3Bucket": job_args['Input']['S3Object']['Bucket'],
            "S3KeyPrefix": f"{job_args['Input']['S3Object']['Name']}.json"
        }
        return self.rekognition.start_media_analysis_job(**job_args)

    def start_person_tracking(self, job_args):
        return self.rekognition.start_person_tracking(**job_args)

    def start_segment_detection(self, job_args):
        job_args['SegmentTypes'] = ['SHOT', 'TECHNICAL_CUE']
        return self.rekognition.start_segment_detection(**job_args)

    def start_text_detection(self, job_args):
        return self.rekognition.start_text_detection(**job_args)


class BaseWaitStrategy:

    def __init__(self, start_job_function, get_job_function, job_args, job_type_def, opts=None):
        self.start_job_function = start_job_function
        self.get_job_function = get_job_function
        self.job_args = job_args
        self.job_id = None
        self.job_type_def = job_type_def
        self.opts = opts

    def run(self):
        pass

    def get_results(self):
        request_args = {
            'JobId': self.job_id
        }
        response_key = self.job_type_def['response_key']
        results = PaginatedResponseRunner(request_args=request_args,
                                          request_function=self.get_job_function,
                                          response_key=response_key
                                          )
        return results


class PaginatedResponseRunner:

    def __init__(self, request_function, request_args, response_key, auto_exec=True):
        self.request_function = request_function
        self.request_args = request_args
        self.response_key = response_key

        if auto_exec:
            self.run()

    def run(self):
        finished = False
        pagination_token = None

        results = []
        response = None
        while not finished:
            response = self.request_function(**self.request_args, NextToken=pagination_token)
            finished = response['PaginationToken'] is None
            pagination_token = response['PaginationToken']

            results.extend(response[self.response_key])

            if 'NextToken' in response:
                pagination_token = response['NextToken']
            else:
                finished = True

        if self.response_key in response:
            response[self.response_key] = results

        return response


class WaitUsingPolling(BaseWaitStrategy):

    def __init__(self, start_job_function, get_job_function, job_args, job_type_def, opts):
        super().__init__(start_job_function, get_job_function, job_args, job_type_def, opts)

    def run(self):
        self.start_job()
        self.wait_for_job_to_complete()

    def get_job_status(self):
        get_job_response = self.get_job_function(self.job_id)
        return get_job_response['JobStatus']

    def start_job(self):
        self.job_id = self.start_job_function(self.job_args)
        return self.job_id
        # return job_id['JobId']
        # return job_id['JobStatus']

    def wait_for_job_to_complete(self):
        job_status = self.get_job_status()
        while job_status == 'IN_PROGRESS':
            time.sleep(5)
            job_status = self.get_job_status()
        return job_status


class WaitUsingSns(BaseWaitStrategy):

    def __init__(self, start_job_function, get_job_function, job_args, opts):
        super().__init__(start_job_function, get_job_function, job_args, opts)

        role_arn = opts.get('notification-channel-role-arn', None)
        if role_arn is None:
            raise ValueError('No notification-channel-role-arn specified')

        self.job_id = None
        self.role_arn = role_arn
        self.sns_topic_arn = None
        self.sqs_queue_url = None

        self.rek = boto3.client('rekognition')
        self.sqs = boto3.client('sqs')
        self.sns = boto3.client('sns')

    def get_results(self):
        pass

    def run(self, opts=None):
        response = None
        self.create_topic_and_queue()
        self.start_job()
        if self.wait_for_success():
            response = self.get_results()

        return response

    def create_topic_and_queue(self):
        millis = str(int(round(time.time() * 1000)))

        # Create SNS topic

        sns_topic_name = f"AmazonRekognition-{millis}"

        create_topic_response = self.sns.create_topic(Name=sns_topic_name)
        self.sns_topic_arn = create_topic_response['TopicArn']

        # create SQS queue
        sqs_queue_name = "AmazonRekognitionQueue" + millis
        self.sqs.create_queue(QueueName=sqs_queue_name)
        self.sqs_queue_url = self.sqs.get_queue_url(QueueName=sqs_queue_name)['QueueUrl']

        attribs = self.sqs.get_queue_attributes(QueueUrl=self.sqs_queue_url,
                                                AttributeNames=['QueueArn'])['Attributes']

        sqs_queue_arn = attribs['QueueArn']

        # Subscribe SQS queue to SNS topic
        self.sns.subscribe(
            TopicArn=self.sns_topic_arn,
            Protocol='sqs',
            Endpoint=sqs_queue_arn)

        # Authorize SNS to write SQS queue
        policy = """{{
          "Version":"2012-10-17",
          "Statement":[
            {{
              "Sid":"MyPolicy",
              "Effect":"Allow",
              "Principal" : {{"AWS" : "*"}},
              "Action":"SQS:SendMessage",
              "Resource": "{}",
              "Condition":{{
                "ArnEquals":{{
                  "aws:SourceArn": "{}"
                }}
              }}
            }}
          ]
        }}""".format(sqs_queue_arn, self.sns_topic_arn)

        _response = self.sqs.set_queue_attributes(
            QueueUrl=self.sqs_queue_url,
            Attributes={
                'Policy': policy
            })

    def start_job(self):
        job_args = self.job_args.copy()
        job_args['NotificationChannel'] = {'RoleArn': self.role_arn,
                                           'SNSTopicArn': self.sns_topic_arn}

        response = self.start_job_function(job_args)
        self.job_id = response['JobId']
        print(f'Job Started. {self.job_id}')

    def wait_for_success(self):
        job_found = False
        succeeded = False

        dot_line = 0
        while not job_found:
            receive_messages_response = self.sqs.receive_message(QueueUrl=self.sqs_queue_url,
                                                                 MessageAttributeNames=['ALL'],
                                                                 MaxNumberOfMessages=10)
            if receive_messages_response:
                if 'Messages' not in receive_messages_response:
                    if dot_line < 40:
                        dot_line += 1
                        print('.', end='')
                    else:
                        dot_line = 0
                        print()

                    for message in receive_messages_response['Messages']:
                        job_found, succeeded = self.process_sqs_message(message)
                        if job_found:
                            break

        return succeeded

    def process_sqs_message(self, message):
        job_found = False
        succeeded = False

        notification = json.loads(message['Body'])
        rekognition_message = json.loads(notification['Message'])

        if rekognition_message['JobId'] == self.job_id:
            if rekognition_message['Status'] == 'SUCCEEDED':
                succeeded = True
        else:
            logger.warn("Job ID didn't match.")

        self.sqs.delete_message(QueueUrl=self.sqs_queue_url,
                                ReceiptHandle=message['ReceiptHandle'])
        return job_found, succeeded

    def delete_topic_and_queue(self):
        self.sqs.delete_queue(QueueUrl=self.sqs_queue_url)
        self.sns.delete_topic(TopicArn=self.sns_topic_arn)


class EnvoiRekognitionGetJobsCommand:

    def __init__(self, opts=None, auto_exec=True):
        self.opts = opts
        if auto_exec:
            self.run(opts)

    def run(self, opts=None):

        jobs_to_check = [{
            "type": "", "id": ""
        }]

        get_job_helper = AwsRekognitionGetJobHelper()

        output = {}
        for job_to_check in jobs_to_check:
            job_type = job_to_check['type']
            job_id = job_to_check['id']

            status = get_job_helper.get_job_status(job_type, job_id)
            output[job_type] = status

        return output


class EnvoiRekognitionStartJobsCommand:

    def __init__(self, opts=None, auto_exec=True):
        self.helper = None
        self.input_file_uri = None
        self.job_type_to_function_map = None
        self.job_types_to_start = None
        self.opts = opts
        self.input_s3_object = None
        if auto_exec:
            self.run(opts)

    @classmethod
    def init_parser(cls, subparsers, command_name=None):

        if subparsers is None:
            parser = argparse.ArgumentParser()
        else:
            parser = subparsers.add_parser(command_name, help='Envoi Rekognition Start Jobs')

        parser.set_defaults(handler=cls)

        parser.add_argument('--input-file-uri',
                            required=True,
                            help='The URI of the input file for the job')
        parser.add_argument('--notification-channel-role-arn',
                            default=None,
                            help='The ARN of the IAM role for SNS notifications. Only used for jobs wait using the '
                            'sns wait strategy.')
        parser.add_argument('--job-tag',
                            default=None,
                            help='Tag for the job for identification purposes, defaults to the input URI.')
        parser.add_argument('--job-types',
                            nargs="+",
                            choices=REKOGNITION_JOB_TYPES_ARGUMENT_CHOICES,
                            help='The types of jobs to start')
        parser.add_argument('--wait-strategy',
                            default="poll",
                            choices=WAIT_STRATEGIES,
                            help='The strategy for waiting job results: "none", "poll" or "sns"')
        parser.add_argument('--create-output-file-per-job-type',
                            default=False,
                            action='store_true',
                            help='Flag to control if the program should create an output file per job type')

        return parser

    def init_job_type_to_function_map(self, start_execution_helper=None, get_job_status_helper=None,
                                      common_job_args=None):
        if start_execution_helper is None:
            start_execution_helper = AwsRekognitionStartJobHelper(common_args=common_job_args)

        if get_job_status_helper is None:
            get_job_status_helper = AwsRekognitionGetJobHelper()

        self.job_type_to_function_map = {
            'celebrity-recognition': {
                "start_execution_handler": start_execution_helper.start_celebrity_recognition,
                "get_job_status_handler": get_job_status_helper.get_celebrity_job_status,
                "response_key": 'Celebrities'
            },  # No Output
            'content-moderation': {
                "start_execution_handler": start_execution_helper.start_content_moderation,
                "get_job_status_handler": get_job_status_helper.get_content_moderation_job_status,
                "response_key": 'ModerationLabels'
            },
            'face-detection': {
                "start_execution_handler": start_execution_helper.start_face_detection,
                "get_job_status_handler": get_job_status_helper.get_face_detection_job_status,
                "response_key": 'Faces'
            },
            'label-detection': {
                "start_execution_handler": start_execution_helper.start_label_detection,
                "get_job_status_handler": get_job_status_helper.get_label_detection_job_status,
                "response_key": 'Labels'
            },
            # 'media-analysis': {"start_execution_handler": helper.start_media_analysis, "input_name": "Input"},
            'person-tracking': {
                "start_execution_handler": start_execution_helper.start_person_tracking,
                "get_job_status_handler": get_job_status_helper.get_person_tracking_job_status,
                "response_key": 'Persons'
            },
            # 'project-version': {"start_execution_handler": helper.start_project_version},
            "segment-detection": {
                "start_execution_handler": start_execution_helper.start_segment_detection,
                "get_job_status_handler": get_job_status_helper.get_segment_detection_job_status,
                "response_key": 'Segments'
            },
            'text-detection': {
                "start_execution_handler": start_execution_helper.start_text_detection,
                "get_job_status_handler": get_job_status_helper.get_text_detection_job_status,
                "response_key": 'TextDetections'
            },
        }

    def run(self, opts=None):
        if opts is None:
            opts = self.opts

        input_s3_object = self.input_s3_object

        # Build Common Job Arguments
        common_job_args = {
            'JobTag': self.input_file_uri
        }
        helper = AwsRekognitionStartJobHelper(common_args=common_job_args)
        self.init_job_type_to_function_map(start_execution_helper=helper)
        job_types_to_start = opts.job_types
        if job_types_to_start is None:
            raise ValueError('No job types specified')

        if 'all' in job_types_to_start:
            job_types_to_start = REKOGNITION_JOB_TYPES

        handler_responses = []

        wait_strategy = opts.get('wait_strategy', 'none')
        if wait_strategy == 'none':
            job_handler = self.run_and_output_response
        else:
            job_handler = self.run_and_wait_for_response

        handler_threads = []
        try:
            for job_type_to_start in job_types_to_start:
                logger.info(f'Starting job type: "{job_type_to_start}"')

                # Build Specific Job Arguments
                job_args = common_job_args.copy()
                job_type_def = self.job_type_to_function_map.get(job_type_to_start, None)
                job_type_def['type'] = job_type_to_start
                if job_type_def is None:
                    print(f'No handler for job type: "{job_type_to_start}", skipping')
                    continue
                input_param_name = job_type_def.get('input_name', 'Video')
                job_args[input_param_name] = {'S3Object': input_s3_object}

                handler_thread = Thread(target=job_handler,
                                        args=(job_args, handler_responses, job_type_def))
                handler_thread.start()
                handler_threads.append(handler_thread)

            for handler_thread in handler_threads:
                handler_thread.join()
        finally:
            logger.info("Handler Responses:", handler_responses)
            self.write_handler_responses(handler_responses)

            return handler_responses

    def run_and_output_response(self, handler_responses, job_args, job_type_def):
        job_type_to_start = job_type_def['type']
        start_execution_handler = job_type_def['start_execution_handler']

        response = start_execution_handler(job_args=job_args)
        logger.info("Response:", response)
        handler_responses.append({"job_type": job_type_to_start, "job_id": response.get('JobId')})
        if self.opts.get('create-output-file-per-job-type', False):
            file_name = f'{job_type_to_start}_{response.get("JobId")}.json'
            with open(file_name, 'w') as f:
                json.dump(response, f, indent=2)

    def run_and_wait_for_response(self, handler_responses, job_args, job_type_def):
        start_execution_handler = job_type_def['start_execution_handler']
        get_job_status_handler = job_type_def['get_job_status_handler']

        opts = self.opts
        wait_strategy = opts.get('wait-strategy', 'poll')
        if wait_strategy == 'poll':
            waiter_class = WaitUsingPolling
        elif wait_strategy == 'sns':
            waiter_class = WaitUsingSns
        else:
            raise ValueError(f'Unknown wait strategy: "{wait_strategy}"')

        waiter = waiter_class(start_job_function=start_execution_handler,
                              get_job_function=get_job_status_handler,
                              job_type_def=job_type_def,
                              job_args=job_args,
                              opts=opts)

        response = waiter.run()
        handler_responses.append(response)
        return response

    def write_handler_responses(self, handler_responses):
        opts = self.opts
        output_file_name = opts.get('output-file-name', 'handler_responses.json')
        try:
            with open(output_file_name, 'w') as f:
                json.dump(handler_responses, f, indent=2)
                logger.info(f"Wrote handler responses to file: {output_file_name}")
        except Exception as e:
            logger.error(f'Exception writing handler responses to file: {output_file_name}. Error: {e}')


class EnvoiCommandLineUtility:

    @classmethod
    def parse_command_line(cls, cli_args, env_vars, sub_commands=None):
        parser = argparse.ArgumentParser(
            description='Envoi MediaConvert Command Line Utility',
        )

        parser.add_argument("--log-level", dest="log_level",
                            default="WARNING",
                            help="Set the logging level (options: DEBUG, INFO, WARNING, ERROR, CRITICAL)")

        if sub_commands is not None:
            sub_command_parsers = {}
            sub_parsers = parser.add_subparsers(dest='command')
            sub_parsers.required = True

            for sub_command_name, sub_command_handler in sub_commands.items():
                sub_command_parser = sub_command_handler.init_parser(sub_parsers, command_name=sub_command_name)
                sub_command_parser.required = True
                sub_command_parsers[sub_command_name] = sub_command_parser

        (opts, args) = parser.parse_known_args(cli_args)
        return opts, args, env_vars, parser

    @classmethod
    def handle_cli_execution(cls):
        """
        Handles the execution of the command-line interface (CLI) for the application.

        :returns: Returns 0 if successful, 1 otherwise.
        """
        cli_args = sys.argv[1:]
        env_vars = os.environ.copy()

        sub_commands = {
            "start-jobs": EnvoiRekognitionStartJobsCommand,
            # "get-jobs": EnvoiRekognitionGetJobsCommand,
        }

        opts, _unhandled_args, env_vars, parser = cls.parse_command_line(cli_args, env_vars, sub_commands)

        ch = logging.StreamHandler()
        ch.setLevel(opts.log_level.upper())
        logger.addHandler(ch)

        try:
            # If 'handler' is in args, run the correct handler
            if hasattr(opts, 'handler'):
                opts.handler(opts)
            else:
                parser.print_help()
                return 1

            return 0
        except Exception as e:
            logger.exception(e)
            return 1


if __name__ == '__main__':
    EXIT_CODE = EnvoiCommandLineUtility.handle_cli_execution()
    sys.exit(EXIT_CODE)
