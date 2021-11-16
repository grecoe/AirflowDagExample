
def az_cli_data_collection(*args, **context):

    """
    Because we have no access to the ConfigurationConstants class, we have to duplicate the keys
    here for what we are looking for in the deployment information. 
    """
    import sys
    import json
    from pprint import pprint

    # Fields we need to get out of the context before we can load our own
    # classes/configurations
    SIDELOAD_SETTINGS = "sideload_info"
    PACKAGE_HOME = "package_home"


    # First, see if the context (extended kwargs) has the deployment information
    sideload_settings = None
    if SIDELOAD_SETTINGS not in context:
        raise Exception(SIDELOAD_SETTINGS, "expected in settings")
    
    print("Sideload Settings")
    sideload_settings = context[SIDELOAD_SETTINGS]
    pprint(sideload_settings)

    # Next, those settings must have the package home in it
    if PACKAGE_HOME not in sideload_settings:
        raise Exception(PACKAGE_HOME, "expected in settings")

    # Add path so we can now access what's inside of our package
    sys.path.append(sideload_settings[PACKAGE_HOME])

    # Import what you need from the source
    from aiinfraexample.utils import ConfigurationConstants, CmdUtils


    if  ConfigurationConstants.DEPLOYMENT_SUBSCRIPTION not in sideload_settings or \
        ConfigurationConstants.COGSRCH_INSTANCE not in sideload_settings or \
        ConfigurationConstants.COGSRCH_INSTANCE_RG not in sideload_settings:
        raise Exception("Required settings missing")
    
    
    # Command to execute to pull Cog Search primary admin key 
    # az search admin-key show --resource-group dangtestcogsearch --service-name dangtestcogsearchinst --query "primaryKey" --output tsv
    version_request = ["az", "--version"]
    login_request = ["az", "login", "--identity"]
    key_request = [
        "az",
        "search",
        "admin-key",
        "show",
        "--resource-group",
        sideload_settings[ConfigurationConstants.COGSRCH_INSTANCE_RG],
        "--service-name",
        sideload_settings[ConfigurationConstants.COGSRCH_INSTANCE],
        "--subscription",
        sideload_settings[ConfigurationConstants.DEPLOYMENT_SUBSCRIPTION],
        "--query",
        '"primaryKey"',
        "--output",
        "tsv"
    ]

    version_result = CmdUtils.get_command_output(version_request, as_json=False)
    print("VERSION RESULT:", version_result)

    login_result = CmdUtils.get_command_output(login_request, as_json=True)
    if not isinstance(login_result, dict) or not isinstance(login_result, list):
        print("Possible Error:", login_result)

    return_data = {}
    key_result = CmdUtils.get_command_output(key_request, as_json=False)
    if key_result:
        return_data["cog_search_key"] = key_result.strip()

    # Get anything else you need here.
    print("Returning...")
    print(json.dumps(return_data))

    return return_data