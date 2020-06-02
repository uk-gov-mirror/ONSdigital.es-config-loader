# es-config-loader

This module is responsible to load the provided config parameters from the provided
survey given in the payload. It is also responsible for adding any parameters which
are given at run time such as period and id. The module also triggers the respective
StepFunction providing the payload.

###Input:

The input to this module is a JSON Payload containing the respective survey code so the
config can be loaded. Additional parameters required for the run should be provided e.g.

    {
      "survey": "BMISG",
      "period": "201809",
      "id": "01020",
      "checkpoint": 1
    }

###Output:

The module's primary purpose is to execute the respective surveys step function passing the 
constructed payload in as the runtime parameters.

The output of this module is the respective survey code's config combined with the provided
payload passed in. 

    {
      "survey": "BMISG",
      "period": "201809",
      "id": "01020",
      "checkpoint": 1,
      "period_column": "period",
      "calculation_type": "movement_calculation_a",
      "distinct_values": "region, strata",
      ...
    }

The lambda response is a dict containing the name of the triggered step function execution.

{
  "execution_id": "sfn-execution-name"
}

### Environment Variables:

    file_path: Used to specify the file path inside the bucket to the configs. e.g. /configs
    
    payload_reference_name: The name of the refrenced used to obtain the survey code from the input JSON.
    
    step_function_arn: This is the partial arn of the step function not including the specific 
    name so that it can be build the full arn dynamically. The account_id part of the arn will be substitued at run time
    by the config loader inserting its own account_id.
    
    config_suffix: The rest of the file name of the config files after the survey code.
    
    survey_arn_prefix: The prefix of the step function name, current standard e.g. ES-
    
    survey_arn_suffix: The suffix of the step function name, current standard e.g. -Results
    
