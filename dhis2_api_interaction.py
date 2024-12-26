# dhis2_api_interaction.py
# Author: mithilesh
import requests
import json
import base64
from requests.auth import HTTPBasicAuth
from datetime import datetime
import logging
import pandas as pd

dhis2_api_push_url = "https://test.tlhis.org/api/"
dhis2_push_username = "*****"
dhis2_push_password = "*****"

dhis2_api_get_url = "https://ln4.hispindia.org/timor_dev/api/"
dhis2_get_username = "*****"
dhis2_get_password = "*****"



#https://evrimed.wisepill.com/sbapi/v1/episodes/getEpisodes?episode_type

evrimed_wisepill_get_url = "https://evrimed.wisepill.com/api/v1/"
evrimed_wisepill_username = "*****"
evrimed_wisepill_secert = "*****"



program_indicators_api_url = f"{dhis2_api_get_url}programIndicators"
program_indicators_grp_api_url = f"{dhis2_api_get_url}programIndicatorGroups"
orgunit_grp_api_url = f"{dhis2_api_get_url}organisationUnitGroups"
program_indicators_data_value_url = f"{dhis2_api_get_url}analytics/dataValueSet.json"


dataValueSet_endPoint = f"{dhis2_api_push_url}dataValueSets"

# check TEI exist
#https://ln4.hispindia.org/timor_dev/api/trackedEntityInstances.json?ouMode=ALL&program=RUqNUsv6WBp&filter=uuZ2ngqdsjm:eq:EU03475

def get_evrimed_wisepill_episodes():
    
    #header = {"Authorization" : "Username" : {evrimed_wisepill_username}, "Secret" : {evrimed_wisepill_username}}


    #headers = {"content-type": "application/json; charset=UTF-8",'Authorization':'Bearer {}'.format(access_token)}

    #headers = {"content-type": "application/json", "Username" : {evrimed_wisepill_username}, "Secret" : {evrimed_wisepill_username}}

    evrimed_wisepill_episodes = list()
    #https://evrimed.wisepill.com/sbapi/v1/episodes/getEpisodes?device_imei=865340054256718&calendar=1
    #https://ln4.hispindia.org/timor_dev/api/programIndicatorGroups/IbAL4Ou7Rfl.json?fields=id,name,programIndicators[id,name]&paging=false
    url_get_episodes = f"{evrimed_wisepill_get_url}/episodes/getEpisodes?device_imei=865340054256718&calendar=1"

    #print(f"url_with_filters : {url_with_filters}")

    headers = {
        'Username': "*****",
        'Secret': "*****"
    }

    response_evrimed_wisepill_episodes = requests.get(url_get_episodes, headers=headers)

    #print(f"response_evrimed_wisepill_episodes : {response_evrimed_wisepill_episodes.status_code}")
    
    if response_evrimed_wisepill_episodes.status_code == 200:

        response_data_episodes = response_evrimed_wisepill_episodes.json()
        #print(f"response_data_episodes : {response_data_episodes}")
        response_data_episodes_records = response_data_episodes.get('records', [])
        
        #if response_data_episodes_records:
            
            #for episode_details in response_data_episodes_records:
                
                #if episode_details:
                    #episode_id = assign_value_if_not_null(episode_details['episode_id'])
                    #device_imei = assign_value_if_not_null(episode_details['device_imei'])
                    #episode_start_date = assign_value_if_not_null(episode_details['episode_start_date'])
                    #print(f"episode_details. episode_id : {episode_id} . device_imei : {device_imei} . episode_start_date : {episode_start_date}")
                    
                                       
            #print(f"episodes_records_list size {len(response_data_episodes_records)}")
            
        #else:
            #error_message = f"No API response found"
            #print(error_message)

    else:
        print(f"Failed to retrieve evrimed_wisepill_get_url Status code: {response_evrimed_wisepill_episodes.status_code}")

    return response_data_episodes_records


def get_orgunit_grp_member():
    
    orgunits = list()
    #https://ln4.hispindia.org/timor_dev/api/organisationUnitGroups/S9dMHURcWAM.json?fields=id,name,organisationUnits[id,name]&paging=false
    url_with_filters = f"{orgunit_grp_api_url}/S9dMHURcWAM.json?fields=id,name,organisationUnits[id,name]&paging=false"

    #print(f"url_with_filters : {url_with_filters}")

    response_orgunit_grp_member = requests.get(url_with_filters, auth=HTTPBasicAuth(dhis2_get_username, dhis2_get_password))


    if response_orgunit_grp_member.status_code == 200:
        response_data_orgunit_grp = response_orgunit_grp_member.json()
        
        orgunit_grp_data = response_data_orgunit_grp.get('organisationUnits', [])
        
        if orgunit_grp_data:
            
            for orgunit in orgunit_grp_data:
                
                if orgunit:
                    org_unit = orgunit['id']
                    orgunits.append(org_unit)
                                       
            print(f"orgunit_grp_member size {len(orgunits)}")
            orgunit_grp_member_list = ";".join(orgunits)
        else:
            error_message = f"No dataElement_coc found"
            print(error_message)

    else:
        print(f"Failed to retrieve org_unit. Status code: {response_orgunit_grp_member.status_code}")

    return orgunit_grp_member_list


def get_progral_indicators():
    
    program_indicators = list()
    #https://ln4.hispindia.org/timor_dev/api/programIndicatorGroups/IbAL4Ou7Rfl.json?fields=id,name,programIndicators[id,name]&paging=false
    url_with_filters = f"{program_indicators_grp_api_url}/IbAL4Ou7Rfl.json?fields=id,name,programIndicators[id,name]&paging=false"

    #print(f"url_with_filters : {url_with_filters}")

    response_program_indicators_grp = requests.get(url_with_filters, auth=HTTPBasicAuth(dhis2_get_username, dhis2_get_password))


    if response_program_indicators_grp.status_code == 200:
        response_data_program_indicators_grp = response_program_indicators_grp.json()
        
        program_indicators_grp_data = response_data_program_indicators_grp.get('programIndicators', [])
        
        if program_indicators_grp_data:
            
            for program_indicator in program_indicators_grp_data:
                
                if program_indicator:
                    program_indicator = program_indicator['id']
                    program_indicators.append(program_indicator)
                                       
            print(f"program_indicator_list size {len(program_indicators)}")
            program_indicator_list = ";".join(program_indicators)
        else:
            error_message = f"No dataElement_coc found"
            print(error_message)

    else:
        print(f"Failed to retrieve program_indicators units. Status code: {response_program_indicators_grp.status_code}")

    return program_indicator_list


def get_aggregated_de_from_indicators():
    aggregated_de_dict = {}
    filters = [
        f"attributeValues.attribute.id:eq:o8ilsRi1p8b&level=3&paging=false"
    ]

    #https://ln4.hispindia.org/timor_dev/api/programIndicators.json?fields=id,name,attributeValues&filter=attributeValues.attribute.id:eq:o8ilsRi1p8b&paging=false
    url_with_filters = f"{program_indicators_api_url}?fields=id,name,attributeValues&filter={'&filter='.join(filters)}"

    #print(f"url_with_filters : {url_with_filters}")

    response_program_indicators = requests.get(url_with_filters, auth=HTTPBasicAuth(dhis2_get_username, dhis2_get_password))


    if response_program_indicators.status_code == 200:
        response_data_program_indicators = response_program_indicators.json()
        
        program_indicators_data = response_data_program_indicators.get('programIndicators', [])
        
        if program_indicators_data:
            for program_indicator in program_indicators_data:
                program_indicators_attributeValues = program_indicator.get('attributeValues', [])
                
                if program_indicators_attributeValues:
                    for program_indicators_attributeValue in program_indicators_attributeValues:

                        program_indicator = program_indicator['id']
                        dataElement_coc = program_indicators_attributeValue['value']

                        if program_indicator not in aggregated_de_dict:
                                aggregated_de_dict[program_indicator] = dataElement_coc
                        else:
                            if dataElement_coc not in aggregated_de_dict[program_indicator]:
                                #option_dict[code].append(value)
                                aggregated_de_dict[program_indicator] = dataElement_coc                     
        
        else:
            error_message = f"No dataElement_coc found"
            print(error_message)

    else:
        print(f"Failed to retrieve program_indicators units. Status code: {response_program_indicators.status_code}")

    return aggregated_de_dict



def get_program_indicators_data_values( program_indicator_grp_members, orgunit_grp_members ):
    
   #https://ln4.hispindia.org/timor_dev/api/analytics/dataValueSet.json?dimension=dx:K8VVrMcSAUD;K81oZQ4b5Vl;QwOHKYNmdN9;Tak313dv0CT;IfECSBYqrqV;eu9RAPEMXhb;BfoLPFMyQzkB;ragjEZ11Bti;FDxVW7nURcD;doyR9jQvv92;GkgzaLmrg5S;MzPenhNCmy2;ck9AtliGzns;KjbIihlYc5D;v6mPHFvH2Ho;npDd2ehR91M&dimension=pe:202401;202402;202403&dimension=ou:op6sM00UM5R;NdWZGvjX3BN;op6sM00UM5R&showHierarchy=false&hierarchyMeta=false&includeMetadataDetails=true&includeNumDen=true&skipRounding=false&completedOnly=false

    period_list = "202401;202402;202403;202404;202405;202406;202407;202408;202409;202410;202411;202412"
    org_list = "op6sM00UM5R;NdWZGvjX3BN;op6sM00UM5R"
    pi_indicators_list = "K8VVrMcSAUD;K81oZQ4b5Vl;QwOHKYNmdN9;Tak313dv0CT;IfECSBYqrqV;eu9RAPEMXhb;BfoLPFMyQzkB;ragjEZ11Bti;FDxVW7nURcD;doyR9jQvv92;GkgzaLmrg5S;MzPenhNCmy2;ck9AtliGzns;KjbIihlYc5D;v6mPHFvH2Ho;npDd2ehR91M"
    #event_search_url = f"{event_push_endpoint}?orgUnit={orgUnitID}&ouMode=SELECTED&program={programID}&status=ACTIVE&skipPaging=true&filter={event_search_dataElement_uid}:eq:{BenCallID}"

    #event_search_url = f"{program_indicators_data_value_url}?dimension=dx:K8VVrMcSAUD;K81oZQ4b5Vl;QwOHKYNmdN9;Tak313dv0CT;IfECSBYqrqV;eu9RAPEMXhb;BfoLPFMyQzkB;ragjEZ11Bti;FDxVW7nURcD;doyR9jQvv92;GkgzaLmrg5S;MzPenhNCmy2;ck9AtliGzns;KjbIihlYc5D;v6mPHFvH2Ho;npDd2ehR91M&dimension=pe:202401;202402;202403&dimension=ou:op6sM00UM5R;NdWZGvjX3BN;op6sM00UM5R&showHierarchy=false&hierarchyMeta=false&includeMetadataDetails=true&includeNumDen=true&skipRounding=false&completedOnly=false"
    program_indicator_data_value_url = f"{program_indicators_data_value_url}?dimension=dx:{program_indicator_grp_members}&dimension=pe:{period_list}&dimension=ou:{orgunit_grp_members}&showHierarchy=false&hierarchyMeta=false&includeMetadataDetails=true&includeNumDen=true&skipRounding=false&completedOnly=false"

    #print(program_indicator_data_value_url)
    #print(f" program_indicator_data_value_url : {program_indicator_data_value_url}" )
    response = requests.get(program_indicator_data_value_url, auth=HTTPBasicAuth(dhis2_get_username, dhis2_get_password))
    if response.status_code == 200:
        response_data = response.json()
        pi_dataValues = response_data.get('dataValues', [])
        return pi_dataValues 
    else:
        return []

def format_date(val):
    if val is not None and val != "null":
        try:
            formatted_date = pd.to_datetime(val).strftime('%Y-%m-%d')
            return formatted_date
        except ValueError:
            return "Invalid date format"
    else:
        return ""  

def assign_value_if_not_null(value):
    if value is not None and value != "null":
        return value
    else:
        return ""






def assign_value_if_not_null(value):
    if value is not None and value != "null":
        return value
    else:
        return ""

def assign_value_if_NaN(value):
    if pd.notnull(value):
        if isinstance(value, int):
            return value
        elif isinstance(value, float):
            return value
        elif value == 'NULL':
            return ""
        else:
            return str(value)
    
    else:
        return ""

def remove_space_if_NaN(value):
    if pd.notnull(value):
        return value.strip()
    else:
        return ""    
    
    
#use this flot to int
def float_to_int(val):
    if val=='' or pd.isna(val):
        return ''
    else:
        return int(val)
    
def push_dataValueSet_in_dhis2(dataValueSet_payload):
    #print(f"dataValueSet_payload : {json.dumps(dataValueSet_payload)}")
    #logging.info(f"dataValueSet_payload : {json.dumps(dataValueSet_payload)}")

    response = requests.post(
        dataValueSet_endPoint,
        data=json.dumps(dataValueSet_payload),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {base64.b64encode(f'{dhis2_push_username}:{dhis2_push_password}'.encode()).decode()}"
        }
    )
    conflictsDetails = ""
    if response.status_code == 200:
        #print(f"DataValue created successfully.  Row No : {row_no} . orgUnit : {orgUnit} . response . {response.status_code}")
        #print(f"DataValue created successfully.  Row No : {row_no} . orgUnit : {orgUnit} . response . {response.json()}")

        #print(f"DataValue created successfully : {response.text}")
        conflictsDetails   = response.json().get("conflicts",[])
        description   = response.json().get("description", {})
        #print(f"DataValue created successfully description : {description}")
        impCount = response.json().get("importCount", {}).get("imported")
        updateCount = response.json().get("importCount", {}).get("updated")
        ignoreCount = response.json().get("importCount", {}).get("ignored")

        print(f"DataValue created successfully. impCount : {impCount} . updateCount : {updateCount} . ignoreCount : {ignoreCount} . description : {description}")
        logging.info(f"DataValue created successfully. impCount : {impCount} . updateCount : {updateCount} . ignoreCount: {ignoreCount} . description : {description}")
        logging.info(f"conflictsDetails : {conflictsDetails}")
        print(f"conflictsDetails : {conflictsDetails}")
        #logging.info(f"DataValue created successfully : {response.text}")
    else:
        print(f"Failed to create dataValueSet. Error: {response.text}")
        logging.error(f"Failed to dataValueSet events . conflictsDetails : {conflictsDetails} . error details: {response.json()} .Error: {response.text}")
