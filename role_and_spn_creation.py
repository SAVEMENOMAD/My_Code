import openpyxl
import logging
import requests
import json
import uuid
from datetime import datetime
import time
import re
global roleDefinitionId, scope, exist

def get_role_definition_id(subscription_id, role_name, token):
   
    try:
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleDefinitions?$filter=roleName eq '{role_name}'&api-version=2022-04-01"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        role_definitions = response.json().get('value')
        if role_definitions:
            return role_definitions[0]['id']
        else:
            logging.error(f"Role definition ID not found for role '{role_name}'.")
            return None
    except requests.RequestException as e:
        logging.error(f"Error retrieving role definition ID: {e}")
        return None

def assign_role_to_subscription(token, subscription_id, role_name, principal_id):

    try:
        role_definition_id = get_role_definition_id(subscription_id, role_name, token)
        if not role_definition_id:
            logging.error(f"Cannot proceed without role definition ID.")
            return False
        
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleAssignments/{str(uuid.uuid4())}?api-version=2022-04-01"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        body = {
            "properties": {
                "roleDefinitionId": role_definition_id,
                "principalId": principal_id
            }
        }
        response = requests.put(url, headers=headers, json=body)
        if response.status_code in [200, 201]:
            logging.info(f"Role '{role_name}' assigned successfully to subscription {subscription_id}.")
            return True
        else:
            logging.error(f"Failed to assign role '{role_name}' to subscription {subscription_id}.")
            logging.error(f"Status code: {response.status_code}")
            logging.error(f"Error: {response.text}.")
            return False
    except requests.RequestException as e:
        logging.error(f"Error assigning role to subscription: {e}")
        return False
def validate_app_id(app_id):
    pattern = re.compile(
        r'^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$', re.IGNORECASE)
    return bool(pattern.match(app_id))

def get_creds(tenant_id, cmo_tenant_id, api_key):
    global APIHEADER
    try:
        APIHEADER = {"Authorization": "API " + api_key}
        my_url = 'https://api.cloudplatform.accenture.com/secret/tenants/' + cmo_tenant_id + \
            '/accounts/' + tenant_id + '/secrets/'
        response = requests.get(my_url, headers=APIHEADER)
        if response.status_code == 200:
            response.raise_for_status()
            resp = response.json()
            if resp:
                resp = resp[0]
                my_url = my_url + resp.get('id')
                response = requests.get(my_url, headers=APIHEADER)
                if response.status_code == 200:
                    response.raise_for_status()
                    resp = response.json()
                    if 'secret' in resp.keys():
                        logging.info(
                            "Secrets fetched from secret_secret table, application id - {}.".format(resp['secret']['application']))
                        return resp['secret']['application'], resp['secret']['key']
                else:
                    logging.error(
                        "Not able to decrypt secret retrived from secret_secret table.")
                    logging.error(
                        "Error code {} , Error message - {}".format(response.status_code, response.text))
                    return (0, 0)
            else:
                logging.error("No valid secret entry is created for tenant {} & CMO tenant id {}.".format(
                    tenant_id, cmo_tenant_id))
                return (1, 0)
        else:
            logging.error(
                "Error in retreiving secrets from DDB for tenant id - {} & CMO Tenant id - {}.".format(tenant_id, cmo_tenant_id))
            logging.error(
                "Error code {} , Error message - {}".format(response.status_code, response))
            return (0, 0)

    except Exception as err:
        logging.error("Error in fetching the client secrets from CMO.")
        logging.error("Error : {}".format(err))
        return (0, 0)

def azure_login(client_id, client_secret, tenant_id):
    try:
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        headers = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://graph.microsoft.com/.default'
        }
        response = requests.post(url, data=headers)
        token = response.json().get('access_token')
        if response.status_code == 200:
            logging.info("Successfully fetched token for the tenant.")
            return (token)
        else:
            logging.error(
                "Failed to authenticate to azure graph API using credentials provided.")
            logging.error("Status code : {}".format(response.status_code))
            logging.error("Error: {}.".format(response.text))
            return (0)
    except Exception as err:
        logging.error("Error in authenticating to azure - {}".format(err))
        return 0

def azure_login_for_role_creation(client_id, client_secret, tenant_id):
    try:
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        headers = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://management.azure.com/.default'
        }
        response = requests.post(url, data=headers)
        if response.status_code == 200:
            logging.info(
                "Successfully fetched token for management console (role creation) for the tenant.")
            token = response.json().get('access_token')
            return (token)
        else:
            logging.ERROR(
                "Failed to authenticate to azure management API using credentials provided.")
            logging.ERROR("Status code : {}.".format(response.status_code))
            logging.ERROR("Error: {}".format(response.text))
            return (0)

    except Exception as err:
        logging.ERROR(
            "Error in authenticating to azure for role creation- {}".format(err))
        return 0

def get_obj_id(mtar_parent, token):
    try:
        url = f"https://graph.microsoft.com/v1.0/servicePrincipals(appId='{mtar_parent}')"
        headers = {'Authorization': 'Bearer ' +
                   token, 'Content-Type': 'application/json'}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logging.info(
                "Details of child SPN : Name - {} , Object id - {}".format(response.json().get("appDisplayName"), response.json().get("id")))
            return (response.json().get("id"), response.json().get("appDisplayName"))
        else:
            logging.error(
                "Failed get the Object id for the Child MTAR for creating role assignment.")
            logging.error("Status code : {}.".format(response.status_code))
            logging.error("Error: {}.".format(response.text))
            return (0, 0)
    except Exception as err:
        logging.error(
            "Error in getting Object id for exisitng MTAR Child SPN in target tenant- {}".format(err))
        return (0, 0)

def deploy_mtar(mtar_parent, token):
    try:
        spn = {
            "appId": mtar_parent,
        }
        graph_url = 'https://graph.microsoft.com/v1.0/servicePrincipals'
        headers = {'Authorization': 'Bearer ' +
                   token, 'Content-Type': 'application/json'}
        response = requests.post(graph_url, headers=headers,
                                 data=json.dumps(spn))

        if response.status_code == 201:
            logging.info("Child MTAR SPN deployed successfully.")
            sheet['F'+str(i)] = "Deployed"
            time.sleep(10)
            return (get_obj_id(mtar_parent, token))

        elif response.status_code == 409 and "ObjectConflict" in response.text:
            logging.info(
                "Child MTAR SPN with same application ID is already deployed within the tenant.")
            sheet['F'+str(i)] = "Already exists."
            exist = 1
            return (get_obj_id(mtar_parent, token))

        else:
            logging.error("Failed to deploy Child MTAR SPN.")
            logging.error("Status code : {}.".format(response.status_code))
            logging.error("Error: {}.".format(response.text))
            return (0, 0)
    except Exception as err:
        logging.error(
            "Error in MTAR Child SPN deployment on target tenant - {}".format(err))
        return (0, 0)
try:
    timestamp = str(datetime.now().strftime("%d-%m-%Y-[%H;%M;%S]"))
    logfile = "Role-Assignment-Logs-"+timestamp+".log"

    open(logfile, "x")

    print("The log file is", logfile)
    logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s', datefmt='%d-%m-%Y--%H:%M:%S')
    logging.info(f"Execution is now starting.")
    xl_input = "tenant.xlsx"
    api_key = "ZlIgUCUG2ZobDr-3"
    mtar_parent = "d70cbbb5-0d05-4980-bac5-f80ff11f18a4"
    x = validate_app_id(mtar_parent)
    if x is False:
        print("App ID entered is invalid. Please validate")
        logging.error(
            "App ID - {} is invalid. Please validate it.".format(mtar_parent))
        exit()
    wb = openpyxl.load_workbook(xl_input)
    sheet = wb.active
    max = len(sheet['A'])

    if max < 2:
        print("No input rows found in the input file. Please validate.")
        logging.info("No input rows found in the input file. Please validate.")
        exit()

    for i in range(2, max + 1):
        tenant_id = str(sheet['A' + str(i)].value)
        subscription_id = str(sheet['B' + str(i)].value)
        cmo_tenant_id = str(sheet['E' + str(i)].value)
        check_type = str(sheet['C' + str(i)].value).strip().lower()

        if not tenant_id or not subscription_id or not cmo_tenant_id:
            logging.error(f"Missing tenant_id or subscription_id or cmo_tenant_id in row {i}.")
            sheet['D' + str(i)] = "Deployment failed due to missing values."
            continue

        sheet['D' + str(i)] = ""
        logging.info(f"Working on Tenant - {tenant_id}, CMO tenant id - {cmo_tenant_id}")

        client_id, client_secret = get_creds(tenant_id, cmo_tenant_id, api_key)

        if client_id == 0:
            print("Error in fetching credentials from CMO.")
            logging.error(
                "Deployment failed for the tenant - {}.Object id - {}".format(tenant_id, mtar_parent))
            sheet['D'+str(i)] = "Deployment failed due to error in fetching credentuals from CMO."
            pass

        elif client_id == 1:
            print("No Secrets found in secret_secret table.")
            logging.error(
                "Deployment failed for the tenant - {} - MTAR parent Object id - {}".format(tenant_id, mtar_parent))
            sheet['D'+str(i)] = "Deployment failed due to no secrets available in CMO."
            pass

        else:

            token = azure_login(client_id, client_secret, tenant_id)
            if token == 0:
                logging.info(
                    "Authentication to azure failed, skipping the tenant {}.".format(tenant_id))
                print(
                    "Deployment failed for the tenant - {}. Please check log file - {}".format(tenant_id, logfile))
                logging.ERROR(
                    "Deployment failed for the tenant - {}. Object id - {}".format(tenant_id, mtar_name, mtar_parent))
                sheet['D'+str(i)] = "Deployment failed due to error in authetication to azure."
                pass

            mtar_obj_id, mtar_name = deploy_mtar(mtar_parent, token)

            if mtar_obj_id == 0:
                logging.info("Failed to deploy child SPN, Exiting execution.")
                print(
                    "Deployment failed for the tenant - {}. Please check log file - {}".format(tenant_id, logfile))
                logging.ERROR(
                    "Deployment failed for the tenant - {} & MTAR parent object id - {}".format(tenant_id, mtar_parent))
                sheet['F'+str(i)] = "Deployment failed due to error in deploying MTAR"
                pass
            token = azure_login_for_role_creation(
                client_id, client_secret, tenant_id)

            if token == 0:
                logging.info(
                    "Authentication to azure failed for role creation, Exiting execution.")
                print(
                    "Deployment failed for the tenant - {}. Please check log file - {}".format(tenant_id, logfile))
                logging.ERROR(
                    "Deployment partially completed for the tenant - {}. Name - {}, Object id - {}".format(tenant_id, mtar_name, mtar_parent))
                sheet['F'+str(i)] = "Deployment failed due to error in authenticating to azure manangement console."
                pass

            if check_type == "siem":
                role_name = "contributor"
                role_defination="b24988ac-6180-42a0-ab88-20f7382dd24c"
            else:
                role_name = "Log Analytics Contributor"
                role_defination = "92aaf0da-9dab-42b6-94a3-d43ce8d16293"

        if assign_role_to_subscription(token, subscription_id, role_name, mtar_obj_id):
            sheet['D' + str(i)] = f"Role '{role_name}' assigned successfully."
        else:
            sheet['D' + str(i)] = f"Failed to assign role '{role_name}'."

        print(f"Completed for Tenant - {tenant_id}")
        logging.info(f"Completed for Tenant - {tenant_id} with role assignment: {role_name}")

    wb.save(xl_input)
    logging.info("Execution has been completed.")

except Exception as err:
    print("Error occurred while execution - ")
    print(err)
    logging.error(f"Error occurred during the execution - {err}")
    exit()
