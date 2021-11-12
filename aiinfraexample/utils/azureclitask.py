
def az_cli_data_collection(*args, **context):

    """
    Because we have no access to the ConfigurationConstants class, we have to duplicate the keys
    here for what we are looking for in the deployment information. 
    """
    import json
    import subprocess

    # Functions to use below
    def get_command_output(command_list, as_json=True):
        print("COMMAND:", " ".join(command_list))
        
        result = subprocess.run(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)

        print("Standard Error:", result.stderr)
        try:
            result = result.stdout.decode("utf-8")
        except UnicodeDecodeError as err:
            print("Unicode error, try again")
            print("Command was: ", " ".join(command_list))
            try:
                result = result.stdout.decode("utf-16")
            except Exception as ex:
                print("Re-attempt failed with ", str(ex))
                result = None

        if as_json and result is not None and len(result):
            return json.loads(result)

        return result    

    DEPLOYMENT_SETTINGS = "deployment_info"
    COG_SEARCH_SUB = "subscription"
    COG_SEARCH_INSTANCE = "cog_search"
    COG_SEARCH_RG = "cog_search_rg"

    deployment_settings = None

    """
    First, see if the context (extended kwargs) has the deployment information
    """
    print("Deployment settings from JSON?")
    if DEPLOYMENT_SETTINGS in context:
        deployment_settings = context[DEPLOYMENT_SETTINGS]

    if not deployment_settings or COG_SEARCH_SUB not in deployment_settings or \
        COG_SEARCH_INSTANCE not in deployment_settings or \
        COG_SEARCH_RG not in deployment_settings:
        raise Exception("Required settings missing")
    # Command
    # az search admin-key show --resource-group dangtestcogsearch --service-name dangtestcogsearchinst --query "primaryKey" --output tsv
    version_request = ["az", "--version"]
    login_request = ["az", "login", "--identity"]
    key_request = [
        "az",
        "search",
        "admin-key",
        "show",
        "--resource-group",
        deployment_settings[COG_SEARCH_RG],
        "--service-name",
        deployment_settings[COG_SEARCH_INSTANCE],
        "--subscription",
        deployment_settings[COG_SEARCH_SUB],
        "--query",
        '"primaryKey"',
        "--output",
        "tsv"
    ]

    version_result = get_command_output(version_request, as_json=False)
    print("VERSION RESULT:", version_result)

    login_result = get_command_output(login_request, as_json=True)
    if not isinstance(login_result, dict):
        print("Houston we have a problem logging in....")

    return_data = {}
    key_result = get_command_output(key_request, as_json=False)
    if key_result:
        return_data["cog_search_key"] = key_result.strip()

    # Get anything else you need here.
    print("Returning...")
    print(json.dumps(return_data))

    return return_data