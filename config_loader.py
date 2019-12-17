import json
import logging
import os
import random

import boto3
from botocore.exceptions import ClientError
from es_aws_functions import aws_functions
from marshmallow import Schema, fields


class InputSchema(Schema):
    """
    Schema to ensure that environment variables are present and in the correct format.
    These variables are expected by the method, and it will fail to run if not provided.
    :return: None
    """
    step_function_arn = fields.Str(required=True)
    bucket_name = fields.Str(required=True)
    file_path = fields.Str(required=True)
    payload_reference_name = fields.Str(required=True)
    config_suffix = fields.Str(required=True)
    survey_arn_prefix = fields.Str(required=True)
    survey_arn_suffix = fields.Str(required=True)


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
    log_message = ""
    logger = logging.getLogger("Config Loader")
    logger.setLevel(10)
    try:
        logger.info("Running Config loader")

        client = boto3.client('stepfunctions')

        # Initialising environment variables
        schema = InputSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment parameters: {errors}")

        step_function_arn = config['step_function_arn']
        bucket_name = config['bucket_name']
        file_path = config['file_path']
        payload_reference_name = config['payload_reference_name']
        config_suffix = config['config_suffix']
        survey_arn_prefix = config['survey_arn_prefix']
        survey_arn_suffix = config['survey_arn_suffix']

        logger.info("Validated environment parameters")

        config_file_name = file_path + event[payload_reference_name] + config_suffix

        config_string = aws_functions.read_from_s3(bucket_name, config_file_name)

        combined_input = {**json.loads(config_string), **event}

        constructed_arn = creating_survey_arn(step_function_arn,
                                              event[payload_reference_name],
                                              survey_arn_prefix,
                                              survey_arn_suffix)

        client.start_execution(stateMachineArn=constructed_arn,
                               name=str(random.getrandbits(128)),
                               input=json.dumps(combined_input))

    except ClientError as e:
        error_message = ("AWS Error in ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = ("Blank or empty environment variable in "
                         + current_module + " |- "
                         + str(e.args))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            return {"success": False, "error": error_message}

    logger.info("Successfully completed module: " + current_module)

    return combined_input


def creating_survey_arn(arn_segment, survey, prefix, suffix):
    """
    This function is used to construct the step function arn using the provided
    arn_segment and attaches the survey inside of the naming standard format.
    :return String: Specified survey arn
    """
    return arn_segment + prefix + survey + suffix
