import unittest.mock as mock

import config_loader

input_params = {
    "survey": "BMISG",
    "period": 201809,
    "id": "01021",
    "checkpoint": 1,
}


class TestConfigLoader:

    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "bucket_name": "mock-bucket",
                "step_function_arn": "mock-arn",
                "file_path": "configs/",
                "payload_reference_name": "survey",
                "config_suffix": "_config.json",
                "survey_arn_prefix": "ES-",
                "survey_arn_suffix": "-Results"
            }
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    @mock.patch("es_aws_functions.aws_functions.read_from_s3")
    @mock.patch("config_loader.boto3.client")
    def test_lambda_handler(self, mock_client, mock_aws_functions):

        with open('tests/fixtures/returned_s3_data.json') as file:
            mock_aws_functions.return_value = file.read()

        concatenated = config_loader.lambda_handler(input_params, None)

        assert concatenated["id"]
        assert concatenated["checkpoint"]

    def test_creating_survey_arn(self):
        arn = config_loader.creating_survey_arn("test:arn:", "BMISG", "ES-", "-Results")
        assert arn == "test:arn:ES-BMISG-Results"

    @mock.patch("es_aws_functions.aws_functions.read_from_s3")
    @mock.patch("config_loader.boto3.client")
    def test_passed_vars_overwrite(self, mock_client, mock_aws_functions):
        with open('tests/fixtures/returned_s3_data.json') as file:
            mock_aws_functions.return_value = file.read()

        concatenated = config_loader.lambda_handler(input_params, None)

        assert concatenated["period"] == 201809

    @mock.patch("config_loader.boto3.client")
    def test_general_error(self, mock_client):

        returned_value = config_loader.lambda_handler(input_params, None)

        # Not mocking causes the lack of credentials to cause a general error.
        assert("General Error" in returned_value['error'])

    @mock.patch("config_loader.boto3.client")
    def test_key_error(self, mock_client):
        mock_client.side_effect = KeyError("Key Error")

        returned_value = config_loader.lambda_handler(input_params, None)

        assert """Key Error""" in returned_value["error"]

    @mock.patch("config_loader.boto3.client")
    def test_missing_environment_variable(self, mock_client):

        # Will remove the environment variable from any following tests.
        config_loader.os.environ.pop("bucket_name")

        returned_value = config_loader.lambda_handler(input_params, None)

        assert """Error validating environment parameters""" in returned_value['error']
