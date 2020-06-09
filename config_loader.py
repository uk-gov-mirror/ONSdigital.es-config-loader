import json
import logging
import os
import random
import time

import boto3
from es_aws_functions import aws_functions, exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class EnvironmentSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating environment params: {e}")
        raise ValueError(f"Error validating environment params: {e}")

    step_function_arn = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    file_path = fields.Str(required=True)
    payload_reference_name = fields.Str(required=True)
    config_suffix = fields.Str(required=True)
    survey_arn_prefix = fields.Str(required=True)
    survey_arn_suffix = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    period = fields.Str(required=True)
    checkpoint = fields.Int(required=True)
    run_id = fields.Str(required=True)
    survey = fields.Str(required=True)


def lambda_handler(event, context):
    """
    This module is responsible to load the provided config parameters from the provided
    survey given in the payload. It is also responsible for adding any parameters which
    are given at run time such as period and id. The module also triggers the respective
    StepFunction providing the payload.
    :return
    """
    current_module = "Config Loader"
    error_message = ""
    logger = logging.getLogger("Config Loader")
    logger.setLevel(10)
    # Define run_id outside of try block.
    run_id = 0
    try:
        logger.info("Running Config loader")
        # Retrieve run_id before input validation.
        # Because it is used in exception handling.
        run_id = event['run_id']

        environment_variables = EnvironmentSchema().load(os.environ)
        runtime_variables = RuntimeSchema().load(event)
        logger.info("Validated parameters")

        bucket_name = environment_variables['bucket_name']
        config_suffix = environment_variables['config_suffix']
        folder_id = run_id
        file_path = environment_variables['file_path']
        payload_reference_name = environment_variables['payload_reference_name']
        step_function_arn = environment_variables['step_function_arn']
        survey_arn_prefix = environment_variables['survey_arn_prefix']
        survey_arn_suffix = environment_variables['survey_arn_suffix']
        survey = runtime_variables[payload_reference_name]
        logger.info("Retrieved configuration variables.")

        client = boto3.client('stepfunctions', region_name='eu-west-2')
        # Append survey to run_id.
        run_id = str(survey) + "-" + str(run_id)
        runtime_variables['run_id'] = run_id
        # Create queue for run.
        queue_url = create_queue(run_id)

        # Add the new queue url to the event to pass downstream.
        runtime_variables['queue_url'] = queue_url

        # Get the rest of the config from s3.
        config_file_name = file_path + \
            survey + \
            config_suffix
        config_string = aws_functions.read_from_s3(bucket_name, config_file_name)
        combined_input = {**json.loads(config_string), **runtime_variables}

        # Setting File Path.
        combined_input["final_output_location"] = combined_input["location"] \
            + "0-latest/"
        combined_input["location"] = combined_input["location"] + folder_id + "/"

        # ARN For SQS Queue.
        constructed_arn = creating_survey_arn(creating_step_arn(step_function_arn),
                                              survey,
                                              survey_arn_prefix,
                                              survey_arn_suffix)

        # Replace file for first checkpoint.
        if 'checkpoint_file' in combined_input:
            combined_input = set_checkpoint_start_file(combined_input['checkpoint_file'],
                                                       combined_input['checkpoint'],
                                                       combined_input)

        sf_response = client.start_execution(stateMachineArn=constructed_arn,
                                             name=run_id + "-" +
                                             str(random.randint(1000, 9999)),
                                             input=json.dumps(combined_input))

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module: " + current_module)

    return {
        "execution_id": sf_response["executionArn"].split(":")[-1]
    }


def creating_step_arn(arn_segment):
    """
    This function will insert the account id into the arn used to reference the step
    function.
    :param arn_segment: Generic step function address - Type: String
    :return String: Step Function arn with the correct account_id added
    """
    account_id = boto3.client('sts',
                              region_name='eu-west-2')\
        .get_caller_identity().get('Account')
    arn_segment = arn_segment.replace("#{AWS::AccountId}", account_id)

    return arn_segment


def creating_survey_arn(arn_segment, survey, prefix, suffix):
    """
    This function is used to construct the step function arn using the provided
    arn_segment and attaches the survey inside of the naming standard format.
    :param arn_segment: Generic step function address - Type: String
    :param survey: Current Survey being run - Type: String
    :param prefix: Prefix for our step function (ES-) - Type: String
    :param suffix: Suffix for our step function (-Results) - Type: String
    :return String: Specified survey arn
    """
    return arn_segment + prefix + survey + suffix


def create_queue(run_id):
    """
    Creates an sqs queue for the results process to use.
    :param run_id: Unique Run id for this run - Type: String
    :return queue_url: url of the newly created queue - Type: String
    """
    sqsclient = boto3.client('sqs', region_name='eu-west-2')
    queue = sqsclient.\
        create_queue(QueueName=run_id + '-results.fifo',
                     Attributes={'FifoQueue': 'True', 'VisibilityTimeout': '40'})
    queue_url = queue['QueueUrl']
    # Queue cannot be used for 1 second after creation.
    # Sleep for a short time to prevent error.
    time.sleep(0.7)
    return queue_url


def set_checkpoint_start_file(checkpoint_file, checkpoint_id, config):
    """
    If a checkpoint_file is set, changes the "in_file_name" section of the config to
    point at a checkpointed file instead of the default.
    :param checkpoint_file: The name of the file to load instead of the default
        - Type: Sting
    :param checkpoint_id: id of the checkpoint to restart from - Type: int
    :param config: the current config to be altered - Type: String/JSON
    :return config: a version of the config with the "in_file_name" section altered
        - Type: String/JSON
    """

    if checkpoint_file is not None and checkpoint_file != "":

        if checkpoint_id == 0:
            config["file_names"]["input_file"] = checkpoint_file
        elif checkpoint_id == 1:
            config["file_names"]["ingest"] = checkpoint_file
        elif checkpoint_id == 2:
            config["file_names"]["enrichment"] = checkpoint_file
        elif checkpoint_id == 3:
            config["file_names"]["strata"] = checkpoint_file
        elif checkpoint_id == 4:
            config["file_names"]["imputation_apply_factors"] = checkpoint_file
        elif checkpoint_id == 5:
            config["file_names"]["aggregation_combiner"] = checkpoint_file

    return config
