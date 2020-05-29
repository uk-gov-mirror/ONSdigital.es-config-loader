import json
from unittest import mock

import pytest
from es_aws_functions import test_generic_library
from moto import mock_sqs, mock_sts

import config_loader as lambda_wrangler_function

runtime_variables = {
    "checkpoint": 1,
    "run_id": "01021",
    "survey": "BMISG",
    "period": 201809,
    "RuntimeVariables": {}
}

environment_variables = {
    "bucket_name": "mock-bucket",
    "config_suffix": "_config.json",
    "file_path": "configs/",
    "payload_reference_name": "survey",
    "step_function_arn": "mock-arn",
    "survey_arn_prefix": "ES-",
    "survey_arn_suffix": "-Results"
}


##########################################################################################
#                                     Generic                                            #
##########################################################################################


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_wrangler_function, runtime_variables,
         environment_variables, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
def test_client_error(which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_wrangler_function, runtime_variables,
         environment_variables, "config_loader.InputSchema",
         "'Exception'", test_generic_library.wrangler_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion",
    [
        (lambda_wrangler_function, environment_variables,
         "KeyError", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion):
    test_generic_library.key_error(which_lambda, which_environment_variables,
                                   expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_runtime_variables",
    [(lambda_wrangler_function,
      "Error validating environment parameters",
      test_generic_library.wrangler_assert,
      runtime_variables)])
def test_value_error(which_lambda, expected_message, assertion,
                     which_runtime_variables):
    test_generic_library.value_error(
        which_lambda, expected_message, assertion,
        runtime_variables=which_runtime_variables)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


def test_creating_survey_arn():
    arn = lambda_wrangler_function.creating_survey_arn("test:arn:",
                                                       "BMISG",
                                                       "ES-",
                                                       "-Results")
    assert arn == "test:arn:ES-BMISG-Results"


@mock_sqs
def test_create_queue():
    queue = lambda_wrangler_function.create_queue("123")
    assert queue == "https://eu-west-2.queue.amazonaws.com/123456789012/123-results.fifo"


@mock_sts
def test_creating_step_arn():
    arn_segment = lambda_wrangler_function.creating_step_arn("#{AWS::AccountId}")
    assert(arn_segment == '123456789012')


@mock.patch("es_aws_functions.aws_functions.read_from_s3")
@mock.patch("config_loader.boto3.client")
def test_config_loader_success(mock_client, mock_aws_functions):
    """
    Test the happy path through config loader
    Checks input data sent to start_execution
    :param mock_client: Mocked boto3 client
    :param mock_aws_functions: Mocked read_from_s3 function
    """
    with mock.patch.dict(lambda_wrangler_function.os.environ,
                         environment_variables):
        with open('tests/fixtures/test_config_prepared_input.json') as file:
            mock_aws_functions.return_value = file.read()
            with mock.patch("config_loader.create_queue") as mock_create_queue:
                mock_create_queue.return_value = "NotARealQueueUrl"
                with mock.patch("config_loader.creating_step_arn") as mock_step_arn:
                    mock_step_arn.return_value = "1234"

                    lambda_wrangler_function.lambda_handler(runtime_variables, None)
                    config = mock_client.return_value.start_execution.call_args
                    with open("tests/fixtures/test_config_prepared_output.json",
                              'r',
                              encoding='utf-8') as f:
                        prepared_output = json.loads(f.read())
                    produced_output = json.loads(config[1]['input'])
                    assert(prepared_output == produced_output)


@pytest.mark.parametrize(
    "checkpointfile,checkpoint,configfilename",
    [
     ("checkpointfile0", 0, "input_file"),
     ("checkpointfile1", 1, "ingest"),
     ("checkpointfile2", 2, "enrichment"),
     ("checkpointfile3", 3, "strata"),
     ("checkpointfile4", 4, "imputation_apply_factors"),
     ("checkpointfile5", 5, "aggregation_combiner")
    ])
def test_set_checkpoint_start_file(checkpointfile,
                                   checkpoint,
                                   configfilename):
    config = lambda_wrangler_function.\
        set_checkpoint_start_file(checkpointfile,
                                  checkpoint,
                                  {"file_names": {}})
    assert config["file_names"][configfilename] == checkpointfile
