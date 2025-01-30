#!python3
import csv
from itertools import count
from oaaclient.client import OAAClient, OAAClientError
import oaaclient.utils as oaautils
import json
import sys
import argparse
import os
import requests
import pandas as pd
from typing import Tuple

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

        # self._veza_con = OAAClient(url=self.host_name, token=self.bearer_token)

    def list_queries(self) -> dict:
        customer_queries_to_compare = {}
        cookies = {
            'token': self.token
        }

        try:
            response = requests.get("https://"+self.host_name+"/api/v1/assessments/queries", cookies=cookies)
        except Exception as e:
            print(e)
            return None

        if response.status_code != 200:
            print("Error: ", response.json())
            return None

        for query in response.json().get('values'):
            if isinstance(query, dict) and 'name' in query:
                if query['name'] in self.queries_to_compare:
                    customer_queries_to_compare[query['name']] = query
            else:
                print("Unexpected query format or missing 'name' key")

        return customer_queries_to_compare
    
    def get_master_queries(self) -> dict:
        master_queries = {}
        with open(self.assess_against) as f:
            master_queries_json = json.load(f)
        
        for query in master_queries_json:
                if query['name'] in self.queries_to_compare:
                    master_queries[query['name']] = query

        return master_queries



    def compare_queries(self) -> list:
        master_queries = self.get_master_queries()
        customer_queries_to_compare = self.list_queries()
        comparison_results = []

        for query in self.queries_to_compare:

                if query not in master_queries:
                    # print("Query \" %s \"not a SYSTEM QUERY" % query)
                    comparison_results.append((query,"NOT A SYSTEM QUERY"))
                    continue
                else:
                    (result, diff) = self.compare_json_objects(master_queries[query], customer_queries_to_compare[query])
                    comparison_results.append((query, result, diff))

        return comparison_results



    def compare_json_objects(self, master_query: dict, customer_query: dict, ignore_keys: list = None) -> Tuple[bool, list]:
        if ignore_keys is None:
            ignore_keys = []

        # Flatten the JSON objects
        master_query_flat = flatten_json(master_query)
        customer_query_flat = flatten_json(customer_query)

        # Convert flattened JSON objects to DataFrames
        master_df = pd.DataFrame([master_query_flat])
        customer_df = pd.DataFrame([customer_query_flat])

        columns_to_ignore = ['id']  
        # Drop the columns you want to ignore
        master_df = master_df.drop(columns=columns_to_ignore, errors='ignore')
        customer_df = customer_df.drop(columns=columns_to_ignore, errors='ignore')

        # Compare the DataFrames and list differences
        differences = []
        comparison_result = master_df.equals(customer_df)
        if not comparison_result:
            for column in master_df.columns:
                if column not in customer_df.columns:
                    differences.append({
                        "column": column,
                        "master_value": master_df[column].values[0],
                        "customer_value": ""
                    })
                elif not master_df[column].equals(customer_df[column]):
                    differences.append({
                        "column": column,
                        "master_value": master_df[column].values[0],
                        "customer_value": customer_df[column].values[0]
                    })
        return (comparison_result, differences)


def main():

    parser = argparse.ArgumentParser(
        description='Get Veza queries from OAA')
    

    parser.add_argument('--host', help='Veza host name for the source tenant')
    parser.add_argument('--csv', help='pass the query names to compare in csv format')
    parser.add_argument('--query', help='Enter the Query you want to compare')
    parser.add_argument('--assess_against', help='input the master queries in Json format')

    args = parser.parse_args()

    veza_cookie = os.getenv('VEZA_COOKIE')
    veza_api_key = os.getenv('VEZA_API_KEY')
    
    if not veza_cookie or not veza_api_key:
        print("Please set VEZA_COOKIE or VEZA_API_KEY environment variable, VEZA Cookie will take precedence over VEZA API Key")
        sys.exit(1)

    if not args.host:
        print("Please provide the source hostname")
        sys.exit(1)
    
    if not args.assess_against:
        print("Please provide the master queries, you can get it from the Veza Github link \"https://github.com/cookieai-jar/cookieai-core/blob/master/controlp/cmd/cp_ckdb_datamgr/config/assessment_queries.json\" and save it as a json file")
        sys.exit(1)

    if not args.csv and not args.query: 
        print("Please provide the csv file with the query names to compare with --csv or provide the single query name with --query")
        sys.exit(1)

    query_df = pd.read_csv(args.csv)

    query_set = set(query_df.iloc[:,0])  

    query_tuple = tuple(query_set)

    configs = {
        "token": veza_cookie if veza_cookie else veza_api_key,
        "hostname": args.host,
        "queries": query_tuple,
        "assess_against": args.assess_against
    }


    veza_queries = VezaQueries(configs)

    resultSet = veza_queries.compare_queries()
    
    result_df = pd.DataFrame(resultSet, columns=['Query', 'Result', 'Differences'])
    result_df.to_csv('result.csv', index=False)


if __name__ == '__main__':
   main()
