#!python3
import json
import sys
import argparse
import os
import pandas as pd
from typing import Tuple
import logging

logging.basicConfig(format="%(asctime)-24s %(levelname)-10s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# Import the Veza class from import_requests.py
try:
    from import_requests import VezaHTTPExtender
except ImportError:
    log.error("Error: import_requests module not found. Please ensure it is in the correct directory.")
    sys.exit(1)

# Flatted the Nested JSON data

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

class VezaQueries:
    def __init__(self, configs: dict) -> None:
        self.token = configs.get("token")
        self.host_name = configs.get("hostname")
        self.queries_to_compare = configs.get("queries")
        self.assess_against = configs.get("assess_against")

        self._veza_con = VezaHTTPExtender(url=self.host_name, api_key=self.token)

    def list_queries(self) -> dict:
        customer_queries_to_compare = {}

        try:
            response = self._veza_con._perform_request('get','/api/v1/assessments/queries', None, None)
            
        except Exception as e:
            log.error(e)
            return None
        
        log.debug("API Response from customers tenant:%s", response)

        for query in response.get('values'):
            if isinstance(query, dict) and 'name' in query:
                if query['name'] in self.queries_to_compare:
                    customer_queries_to_compare[query['name']] = query
            else:
                log.error("Unexpected API Response format or missing 'name' key")

        return customer_queries_to_compare
    
    def get_master_queries(self) -> dict:
        master_queries = {}

        master_queries_json_df = pd.read_json(self.assess_against)

        log.debug("Master Query JSON data: %s",json.loads(master_queries_json_df.to_json(orient='records')))
        
        for query in json.loads(master_queries_json_df.to_json(orient='records')):
                if query['name'] in self.queries_to_compare:
                    master_queries[query['name']] = query

        return master_queries



    def compare_queries(self) -> list:
        master_queries = self.get_master_queries()
        customer_queries_to_compare = self.list_queries()
        comparison_results = []

        for query in self.queries_to_compare:

                if query not in master_queries:
                    log.debug("Query \" %s \"not a SYSTEM QUERY" % query)
                    comparison_results.append((query,"NOT A SYSTEM QUERY"))
                    continue
                else:
                    (result, diff) = self.compare_json_objects(master_queries[query], customer_queries_to_compare[query])
                    comparison_results.append((query, result, diff))

        return comparison_results



    def compare_json_objects(self, master_query: dict, customer_query: dict, ignore_keys: list = None) -> Tuple[bool, list]:
        if ignore_keys is None:
            ignore_keys = []


        log.debug("Comparing queries: master_query=%s, customer_query=%s", master_query['name'], customer_query['name'])

        # Flatten the JSON objects
        master_query_flat = flatten_json(master_query)
        customer_query_flat = flatten_json(customer_query)

        log.debug("Flattened Master Query: %s", master_query_flat)

        log.debug("Flattened Customer Query:%s", customer_query_flat)

        # Convert flattened JSON objects to DataFrames
        master_df = pd.DataFrame([master_query_flat])
        customer_df = pd.DataFrame([customer_query_flat])

        columns_to_ignore = ['id']  

        log.debug("dropping columns: %s",columns_to_ignore)

        # Drop the columns you want to ignore
        master_df = master_df.drop(columns=columns_to_ignore, errors='ignore')
        customer_df = customer_df.drop(columns=columns_to_ignore, errors='ignore')

        # Compare the DataFrames and list differences
        differences = []
        comparison_result = master_df.equals(customer_df)
        if not comparison_result:
            for column in master_df.columns:
                diff = {}
                if column != "":
                    if column not in customer_df.columns:
                        diff = {
                            'column' : str(column),
                            'master_value': str(master_df[column].values[0]),
                            'customer_value': ""
                        }
                        differences.append(diff)
                    elif not master_df[column].equals(customer_df[column]):
                        diff = {
                            'column' : str(column),
                            'master_value': str(master_df[column].values[0]),
                            'customer_value': str(customer_df[column].values[0])
                        }
                        differences.append(diff)

        # differences = json.dumps(differences).replace("\"","'")
        # log.debug("Differences in Queries: %s", json.dumps(differences, indent=2))
        return (comparison_result, differences)



# Write differences to the individual CSV files

def create_final_output(resultSet: list) -> None:
    output_dir = 'output'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


    for queryResult in resultSet:

        log.debug("Differences in Query %s", queryResult[0], queryResult[2] )

        output_file = os.path.join(output_dir, f"{queryResult[0]}.csv")


        log.info("Writing to File: %s", output_file)

        if queryResult[1] != "NOT A SYSTEM QUERY":
            json_data = queryResult[2]
            # Ensure json_data is a list of dictionaries
            if isinstance(json_data, str):
                json_data = json.loads(json_data.replace("'", "\""))
            if isinstance(json_data, list) and all(isinstance(item, dict) for item in json_data):
                df = pd.DataFrame(json_data, columns=['column', 'master_value', 'customer_value'])
                df.to_csv(output_file, index=False)
            else:
                log.error("Invalid format for differences data: %s", json_data)


# Method to clean up the Output directoriess

def cleanup():
  ##Clear the output directory

    if os.path.exists('output'):
        for file in os.listdir('output'):
            os.remove(os.path.join('output', file))

    ## Clean up result.csv file
    if os.path.exists('result.csv'):
        os.remove('result.csv')


# main method which will be trigger when executed by the command prompt.

def main():

    ## Cleanup the output folders and files
    cleanup()

    parser = argparse.ArgumentParser(
        description='Get Veza queries from OAA')
    

    parser.add_argument('--host', help='Veza host name for the source tenant')
    parser.add_argument('--csv', help='pass the query names to compare in csv format')
    parser.add_argument('--query', help='Enter the Query you want to compare')
    parser.add_argument('--assess_against', help='input the master queries in Json format')
    parser.add_argument('--debug', action='store_true', help="Switch to enable debug logging")


    args = parser.parse_args()

    veza_api_key = os.getenv('VEZA_API_KEY')
    
    if not veza_api_key:
        log.error("Please set veza cookie or api key as an environment variable \"VEZA API Key\" ")
        sys.exit(1)

    if not args.host:
        log.error("Please provide the source hostname")
        sys.exit(1)
    
    if not args.assess_against:
        log.error("Please provide the master queries, you can get it from the Veza Github link \"https://github.com/cookieai-jar/cookieai-core/blob/master/controlp/cmd/cp_ckdb_datamgr/config/assessment_queries.json\" and save it as a json file")
        sys.exit(1)

    if not args.csv and not args.query: 
        log.error("Please provide the csv file with the query names to compare with --csv or provide the single query name with --query")
        sys.exit(1)

    if args.debug:
        print("debug enabled")
        log.setLevel(logging.DEBUG)
  


    query_df = pd.read_csv(args.csv)

    query_set = set(query_df.iloc[:,0])  

    query_tuple = tuple(query_set)

    configs = {
        "token": veza_api_key,
        "hostname": args.host,
        "queries": query_tuple,
        "assess_against": args.assess_against
    }


    veza_queries = VezaQueries(configs)

    resultSet = veza_queries.compare_queries()


    # Write difference to the file
    
    result_df = pd.DataFrame(resultSet, columns=['Query', 'Result', 'Differences'])

    result_df.to_csv('result.csv', index=False)

    # Write differences to the individual files

    create_final_output(resultSet)

if __name__ == '__main__':
   main()


