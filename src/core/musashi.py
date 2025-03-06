from core.utils import *
import os
import requests
import shutil
import zipfile
import glob
import time
import subprocess
import yaml
import json
import pandas as pd
from evtx import PyEvtxParser
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from sigma.rule import SigmaRule
from sigma.pipelines.microsoftxdr import microsoft_xdr_pipeline
from sigma.pipelines.azuremonitor import azure_monitor_pipeline
from sigma.pipelines.carbonblack import CarbonBlack_pipeline, CarbonBlackResponse_pipeline
from sigma.pipelines.cortexxdr import CortexXDR_pipeline
from sigma.processing.pipeline import ProcessingPipeline
import re
from typing import ClassVar, Dict, List, Optional, Pattern, Tuple, Union, Any
from sigma.conversion.deferred import DeferredQueryExpression
from sigma.conversion.state import ConversionState
from sigma.exceptions import SigmaFeatureNotSupportedByBackendError
from sigma.rule import SigmaRule
from sigma.conversion.base import TextQueryBackend
from sigma.conditions import (
    ConditionItem,
    ConditionAND,
    ConditionOR,
    ConditionNOT,
    ConditionValueExpression,
    ConditionFieldEqualsValueExpression,
)
from sigma.types import (
    SigmaCompareExpression,
    SigmaString,
    SpecialChars,
    SigmaCIDRExpression,
)
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.neighbors import LocalOutlierFactor
from sklearn.decomposition import PCA
import rrcf
import tensorflow.keras as keras

def flatten_json(nested_json, prefix=""):
    """
    Recursively flattens a nested JSON dictionary.
    """
    flattened = {}
    if isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = f"{prefix}{key}" if prefix else key
            if isinstance(value, dict):
                flattened.update(flatten_json(value, new_key + "_"))
            elif isinstance(value, list):
                # Convert list values to a comma-separated string
                flattened[new_key] = ", ".join(map(str, value))
            else:
                flattened[new_key] = value
    else:
        flattened[prefix] = nested_json
    return flattened

class superDBBackend(TextQueryBackend):
    """SuperDB backend."""
    name: ClassVar[str] = "SuperDB backend"
    formats: Dict[str, str] = {"default": "SuperDB queries"}
    requires_pipeline: bool = False

    precedence: ClassVar[Tuple[ConditionItem, ConditionItem, ConditionItem]] = (
        ConditionNOT, ConditionAND, ConditionOR
    )
    parenthesize: bool = True
    group_expression: ClassVar[str] = "({expr})"
    token_separator: str = " "
    or_token: ClassVar[str] = "OR"
    and_token: ClassVar[str] = "AND"
    not_token: ClassVar[str] = "NOT"
    eq_token: ClassVar[str] = "=="
    field_quote: ClassVar[str] = "`"
    field_quote_pattern: ClassVar[Pattern] = re.compile("^[a-zA-Z0-9_]*$")
    field_quote_pattern_negation: ClassVar[bool] = True
    str_quote: ClassVar[str] = "'"
    escape_char: ClassVar[str] = "\\"
    wildcard_multi: ClassVar[str] = "%"
    wildcard_single: ClassVar[str] = "_"
    wildcard_glob: ClassVar[str] = "*"
    wildcard_glob_single: ClassVar[str] = "?"
    add_escaped: ClassVar[str] = "\\"
    bool_values: ClassVar[Dict[bool, str]] = {True: "true", False: "false"}
    
    startswith_expression: ClassVar[str] = "grep(/^{value}.*/, this['{field}'])"
    endswith_expression: ClassVar[str] = "grep(/.*{value}/, this['{field}'])"
    contains_expression: ClassVar[str] = "grep(/.*{value}.*/, this['{field}'])"
    wildcard_match_expression: ClassVar[str] = "grep(/.*{value}.*/, this['{field}'])"
    
    field_exists_expression: ClassVar[str] = "{field} == {field}"
    wildcard_match_str_expression: ClassVar[str] = "grep(/.*{value}.*/, {field})"
    re_expression: ClassVar[str] = "{field} REGEXP '{regex}'"
    re_escape_char: ClassVar[str] = ""
    re_escape: ClassVar[Tuple[str]] = ()
    re_escape_escape_char: bool = True
    re_flag_prefix: bool = True
    case_sensitive_match_expression: ClassVar[str] = "{field} GLOB {value} ESCAPE '\\'"
    
    compare_op_expression: ClassVar[str] = "{field} {operator} {value}"
    compare_operators: ClassVar[Dict[SigmaCompareExpression.CompareOperators, str]] = {
        SigmaCompareExpression.CompareOperators.LT: "<",
        SigmaCompareExpression.CompareOperators.LTE: "<=",
        SigmaCompareExpression.CompareOperators.GT: ">",
        SigmaCompareExpression.CompareOperators.GTE: ">=",
    }
    
    field_equals_field_expression: ClassVar[Optional[str]] = None
    field_equals_field_escaping_quoting: Tuple[bool, bool] = (True, True)
    field_null_expression: ClassVar[str] = "{field}==null"
    
    convert_or_as_in: ClassVar[bool] = False
    convert_and_as_in: ClassVar[bool] = False
    in_expressions_allow_wildcards: ClassVar[bool] = False
    field_in_list_expression: ClassVar[str] = "{field} {op} ({list})"
    or_in_operator: ClassVar[str] = "IN"
    list_separator: ClassVar[str] = ", "
    
    deferred_start: ClassVar[str] = ""
    deferred_separator: ClassVar[str] = ""
    deferred_only_query: ClassVar[str] = ""

    table = "<TABLE_NAME>"

    def convert_value_str(self, s: SigmaString, state: ConversionState, no_quote: bool = False, glob_wildcards: bool = False) -> str:
        """Convert a SigmaString into a query-compatible string."""
        converted = s.convert(
            escape_char=self.escape_char,
            wildcard_multi=self.wildcard_glob if glob_wildcards else self.wildcard_multi,
            wildcard_single=self.wildcard_glob_single if glob_wildcards else self.wildcard_single,
            add_escaped=self.add_escaped,
            filter_chars=self.filter_chars,
        ).replace("'", "''")

        return self.quote_string(converted) if self.decide_string_quoting(s) and not no_quote else converted

    def convert_condition_field_eq_val_str(self, cond: ConditionFieldEqualsValueExpression, state: ConversionState) -> Union[str, DeferredQueryExpression]:
        """Conversion of field = string value expressions."""
        remove_quote = True

        if cond.value.endswith(SpecialChars.WILDCARD_MULTI) and not cond.value[:-1].contains_special():
            expr, value = self.startswith_expression, cond.value[:-1]
        elif cond.value.startswith(SpecialChars.WILDCARD_MULTI) and not cond.value[1:].contains_special():
            expr, value = self.endswith_expression, cond.value[1:]
        elif cond.value.startswith(SpecialChars.WILDCARD_MULTI) and cond.value.endswith(SpecialChars.WILDCARD_MULTI) and not cond.value[1:-1].contains_special():
            expr, value = self.contains_expression, cond.value[1:-1]
        elif any(x in cond.value for x in [self.wildcard_multi, self.wildcard_single, self.escape_char]) or cond.value.contains_special():
            expr, value = self.wildcard_match_expression, cond.value
        else:
            expr, value, remove_quote = "{field}" + self.eq_token + "{value}", cond.value, False

        return expr.format(
            field=self.escape_and_quote_field(cond.field),
            value=self.convert_value_str(value, state, remove_quote)
        )

    def convert_condition_field_eq_val_cidr(self, cond: ConditionFieldEqualsValueExpression, state: ConversionState) -> Union[str, DeferredQueryExpression]:
        """Convert field matches CIDR value expressions."""
        expanded = cond.value.expand()
        expanded_cond = ConditionOR(
            [ConditionFieldEqualsValueExpression(cond.field, SigmaString(network)) for network in expanded],
            cond.source,
        )
        return self.convert_condition(expanded_cond, state)

    def finalize_query_default(self, rule: SigmaRule, query: str, index: int, state: ConversionState) -> Any:
        """Finalizes the SuperDB query."""
        return f"{query}"

    def convert_condition_val_str(self, cond: ConditionValueExpression, state: ConversionState) -> Union[str, DeferredQueryExpression]:
        """Disallow value-only string expressions."""
        raise SigmaFeatureNotSupportedByBackendError("Value-only string expressions are not supported by the backend.")

    def convert_condition_val_num(self, cond: ConditionValueExpression, state: ConversionState) -> Union[str, DeferredQueryExpression]:
        """Disallow value-only number expressions."""
        raise SigmaFeatureNotSupportedByBackendError("Value-only number expressions are not supported by the backend.")

class LogAnalyzer:
    def __init__(self, log_path, result_path, log_format='winevtx', sigma_rule_path=None, num_threads=None):
        """
        Initializes LogAnalyzer.
        
        Args:
            log_path (str): Path to log file or directory.
            result_path (str): Path to store results.
            log_format (str): Log format (default: 'winevtx').
            sigma_rule_path (str, optional): Path to Sigma rules.
            num_threads (int, optional): Number of threads to use (default: all CPU cores).
        """
        self.output_path = os.path.abspath(result_path)
        self.log_path = os.path.abspath(log_path)
        self.superdb = SuperDBAPI()
        self.log_format = log_format
        self.num_threads = num_threads or os.cpu_count()  # Use all available cores if not set

        if sigma_rule_path is None:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            self.sigma_rule_path = os.path.abspath(os.path.join(project_root, "sigma_all_rules", "rules", "windows"))
        else: 
            self.sigma_rule_path = os.path.abspath(sigma_rule_path)

        if os.path.isdir(self.sigma_rule_path):
            self.sigma_rule_path = os.path.join(self.sigma_rule_path, "**", "*.yml")

        # Ensure Sigma rule path exists
        if not os.path.exists(self.sigma_rule_path.split("**")[0]):
            self.get_sigma_rules()
            #raise ValueError(f"Error: Sigma rule path '{self.sigma_rule_path}' does not exist!")
        
        print(f"Input Log: {self.log_path}")
        print(f"Output path results: {self.output_path}")
        print(f"Using Log Format: {self.log_format}")
        print(f"Using Sigma rules path: {self.sigma_rule_path}")
        print(f"Using {self.num_threads} threads")
        self.__set_lake()
        self.__get_logs()

    def get_pool_data(self, log_type='sigma', filter_query=""):
        """
        Retrieves logs from the pool in super database based on the specified log type.
    
        Parameters:
            log_type (str): The type of logs to retrieve. 
                            Options: 'raw', 'sigma', 'autoencoders', 'isolation_forest'.
            filter_query (str): Optional additional filtering query.
    
        Returns:
            pd.DataFrame: A DataFrame containing the retrieved log data.
        """
        # Map log types to database tables
        log_mapping = {
            'sigma': 'sigma',
            'raw': 'logs',
            'autoencoders': 'anomalies_autoencoder',
            'isolation_forest': 'anomalies_isolation_forest',
            'lof': 'anomalies_lof',
            'ocsvm': 'anomalies_ocsvm',
            'pca': 'anomalies_pca',
            'anomalies': 'anomalies_ensemble'
        }
    
        # Validate log_type
        if log_type not in log_mapping:
            print(f"Warning: Invalid log_type '{log_type}'. Defaulting to 'sigma_result'.")
            log_type = 'sigma_result'
    
        # Construct the query
        table_name = log_mapping[log_type]
        super_query = f"from {table_name}"
        
        if filter_query:
            super_query += f" | {filter_query}"  # Append additional filter if provided
    
        # Execute the query
        try:
            res = self.superdb.execute_query(query=super_query)
            if not res:
                print(f"No results found in '{table_name}'.")
                return pd.DataFrame()  # Return empty DataFrame for consistency
            return pd.DataFrame(res)
        except Exception as e:
            print(f"Error retrieving logs from '{table_name}': {e}")
            return pd.DataFrame()

    def __get_root_directory(self):
        """Finds the root directory (taptap/) based on script location."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while current_dir and not os.path.exists(os.path.join(current_dir, "core")):
            current_dir = os.path.dirname(current_dir)

        if not current_dir:
            raise RuntimeError("Error: Could not find taptap root directory!")

        return current_dir

    def __set_lake(self):
        """Initializes the SuperDB data lake."""
        try:
            print("Initializing the lake...")
            root_dir = self.__get_root_directory()
            os.chdir(root_dir)

            if os.path.exists("datalake"):
                print("Removing existing datalake/ directory...")
                shutil.rmtree("datalake")

            subprocess.run(["./bin/super", "db", "init", "-lake", "datalake"], check=True)

            # Kill any existing SuperDB processes
            subprocess.run("pkill -f 'super db serve'", shell=True, stderr=subprocess.DEVNULL)

            # Start SuperDB in background
            server_process = subprocess.Popen(
                "nohup ./bin/super db serve -lake datalake > superdb.log 2>&1 &",
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )

            time.sleep(3)
            print("Lake initialized and SuperDB server started.")
            return server_process

        except subprocess.CalledProcessError as e:
            print(f"Error in set_lake: {e}")
        except Exception as e:
            print(f"Unexpected error in set_lake: {e}")

    def __process_log_file(self, file_path):
        """Processes a single log file based on its extension."""
        file_ext = os.path.splitext(file_path)[-1].lower()

        try:
            if file_ext == ".csv":
                df = pd.read_csv(file_path, low_memory=False)
                df.columns = df.columns.str.replace(" ", "")
                data = df.to_csv(index=False)
                self.superdb.load_data_to_branch('logs', 'main', data, csv_delim=',')
                print(f" CSV processed: {file_path} ({len(df)} records)")

            elif file_ext == ".tsv":
                df = pd.read_csv(file_path, sep="\t", low_memory=False)
                df.columns = df.columns.str.replace(" ", "")
                data = df.to_json(orient="records")
                self.superdb.load_data_to_branch('logs', 'main', data, csv_delim=',')
                print(f" TSV processed: {file_path} ({len(df)} records)")

            elif file_ext == ".json":
                df = pd.read_json(file_path, low_memory=False)
                df.columns = df.columns.str.replace(" ", "")
                data = df.to_json(orient="records")
                self.superdb.load_data_to_branch('logs', 'main', data, csv_delim=',')
                print(f" JSON processed: {file_path} ({len(df)} records)")

            elif file_ext == ".evtx":
                print(f"Parsing EVTX: {file_path}")
                records_list = []
                parser = PyEvtxParser(file_path)

                for record in parser.records_json():
                    event_id = record.get("event_record_id")
                    timestamp = record.get("timestamp")

                    try:
                        event_data = json.loads(record["data"])["Event"]
                        flattened_event = flatten_json(event_data)  # Flatten nested JSON
                        flattened_event["Event Record ID"] = event_id
                        flattened_event["Timestamp"] = timestamp
                        records_list.append(flattened_event)

                    except json.JSONDecodeError:
                        print(f"⚠️ JSON decoding error in EVTX: {file_path} (Event ID: {event_id})")

                df = pd.DataFrame(records_list)
                data = df.to_csv(index=False)
                self.superdb.load_data_to_branch('logs', 'main', data, csv_delim=',')
                print(f"EVTX processed: {file_path} ({len(df)} records)")

            else:
                print(f"Unsupported file format: {file_path}")

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    def __get_logs(self):
        """
        Loads logs from a single file or an entire directory.
        Supports CSV, JSON, EVTX, and TSV.
        """
        print(f"\nLoading logs from: {self.log_path}")

        # Ensure the 'logs' pool exists in SuperDB
        self.superdb.create_pool(name='logs', layout_order='asc', layout_keys=[['EventTime']], thresh=None)

        # Check if log_path is a directory or a single file
        if os.path.isdir(self.log_path):
            log_files = glob.glob(os.path.join(self.log_path, "*.*"))  # Get all files
            log_files = [f for f in log_files if f.endswith(('.csv', '.json', '.evtx', '.tsv'))]

            if not log_files:
                print("No supported log files found in directory.")
                return

            print(f"Found {len(log_files)} log files to process.")

            # Process files in parallel
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                executor.map(self.__process_log_file, log_files)

        elif os.path.isfile(self.log_path):
            self.__process_log_file(self.log_path)

        else:
            print("Invalid log path! Must be a valid file or directory.")

        print("Log ingestion completed.")

    def get_sigma_rules(self):
        """Fetches the latest Sigma rules from GitHub."""
        github_api_url = "https://api.github.com/repos/SigmaHQ/sigma/releases/latest"
        local_dir = "sigma_all_rules"
        zip_file = "sigma_all_rules.zip"
        version_file = "sigma_rules_version.txt"

        response = requests.get(github_api_url, headers={"Accept": "application/vnd.github.v3+json"})
        if response.status_code != 200:
            print("Error: Could not fetch release data.")
            return

        release_data = response.json()
        latest_version = release_data.get("tag_name", "unknown")

        if os.path.exists(version_file):
            with open(version_file, "r") as f:
                current_version = f.read().strip()
            if current_version == latest_version:
                print(f"Sigma Rules are already up to date! (Version {latest_version})")
                return

        zip_url = next((asset["browser_download_url"] for asset in release_data.get("assets", []) if asset["name"].endswith(".zip")), None)

        if not zip_url:
            print("Error: No ZIP file found in latest release.")
            return

        response = requests.get(zip_url, stream=True)
        if response.status_code != 200:
            print(f"Error: Failed to download {zip_url}")
            return

        with open(zip_file, "wb") as file:
            for chunk in response.iter_content(1024):
                file.write(chunk)
        print(f"Downloaded: {zip_file}")

        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)
            print(f"Deleted old files in {local_dir}")

        os.makedirs(local_dir, exist_ok=True)
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(local_dir)
        print(f"Extracted files to {local_dir}")

        with open(version_file, "w") as f:
            f.write(latest_version)
        print(f"Updated version info: {latest_version}")

        os.remove(zip_file)
        print("Update completed!")

    def __get_queries(self):
        """Loads and converts Sigma rules in parallel."""
        sigma_rule_files = glob.glob(self.sigma_rule_path, recursive=True)
        if not sigma_rule_files:
            print(f"No Sigma rules found in {self.sigma_rule_path}")
            return []

        converted_rules = []
        loaded_count = 0

        def process_rule(rule_file):
            nonlocal loaded_count
            if not os.path.exists(rule_file):
                print(f"Error: File {rule_file} does not exist!")
                return None

            try:
                with open(rule_file, "r") as f:
                    sigma_rule_yaml = f.read()
                sigma_rule = SigmaRule.from_yaml(sigma_rule_yaml)

                if self.log_format == "azure":
                    pipeline = azure_monitor_pipeline()
                    pipeline.apply(sigma_rule)
                elif self.log_format == "defender":
                    pipeline = microsoft_xdr_pipeline()
                    pipeline.apply(sigma_rule)
                elif self.log_format == "cortex":
                    pipeline = CortexXDR_pipeline()
                    pipeline.apply(sigma_rule)
                elif self.log_format == "carbonblackresponse":
                    pipeline = CarbonBlackResponse_pipeline()
                    pipeline.apply(sigma_rule)
                elif self.log_format == "carbonblack":
                    pipeline = CarbonBlack_pipeline()
                    pipeline.apply(sigma_rule)
                elif self.log_format == "winevtx":
                    mapping_file = os.path.join(os.path.dirname(__file__), "windows_mapping_superdb.yml")
                    if not os.path.exists(mapping_file):
                        print(f"Warning: Mapping file {mapping_file} not found.")
                        return None                        

                    with open(mapping_file, "r") as f:
                        yaml_content = yaml.safe_load(f)
                    pipeline = ProcessingPipeline.from_dict(yaml_content)
                    pipeline.apply(sigma_rule)

                converted_rule = superDBBackend().convert_rule(sigma_rule)
                loaded_count += 1
                return sigma_rule.title, converted_rule[0], sigma_rule.tags

            except Exception as e:
                #print(f"⚠️ Error processing rule {rule_file}: {e}")
                return None

        with ThreadPoolExecutor() as executor:
            results = list(executor.map(process_rule, sigma_rule_files))

        converted_rules = [res for res in results if res]
        print(f"Number of rules loaded: {loaded_count}")
        return converted_rules

    def __execute_query(self, query, rule_title, mitre_tags, all_results, rule_hit_count, mitre_timeline):
        """Executes a query and processes results."""
        try:
            fin_query = f"from logs | {query}"
            res = self.superdb.execute_query(query=fin_query)
            if not res:
                return

            df = pd.DataFrame(res)
            df["SigmaRule"] = rule_title

            csv_output = os.path.join(self.output_path, f"{rule_title}.csv")
            df.to_csv(csv_output, index=False)

            all_results.append(df)
            rule_hit_count[rule_title] = len(df)

            for mitre in mitre_tags:
                mitre_timeline[mitre].append(rule_title)

            print(f"Sigma Rule Triggered: {rule_title} ({len(df)} event matches)")

        except Exception as e:
            print(f"Error executing query for rule {rule_title}: {e}")

    def get_sigma_detections(self):
        """
        Executes queries in parallel, logs hits with rule titles, saves individual results,
        """
        all_results = []
        rule_hit_count = Counter()
        mitre_timeline = defaultdict(list)

        queries = self.__get_queries()
        if not queries:
            print("No queries were generated from Sigma rules. Skipping detections.")
            return

        print("\n=== Running Detections ===")

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = [
                executor.submit(self.__execute_query, query, rule_title, mitre_tags, all_results, rule_hit_count, mitre_timeline)
                for rule_title, query, mitre_tags in queries
            ]
            for future in futures:
                future.result()  # Ensure all threads complete

        if all_results:
            merged_df = pd.concat(all_results, ignore_index=True)
            merged_output = os.path.join(self.output_path, "all-sigma-results.csv")
            merged_df.to_csv(merged_output, index=False)
            print(f"All Sigma results merged and saved to {merged_output}")
            # Ingest merged results into SuperDB lake
            data = merged_df.to_csv(index=False)
            self.superdb.create_pool(name='sigma', layout_order='asc', thresh=None)
            self.superdb.load_data_to_branch('sigma', 'main', data, csv_delim=',')
        else:
            print("No detections found.")

        print("Detection process completed.")

    def __save_anomalies(self, anomalies, name):
        """Helper function to save anomalies and load them into the database."""
        file_path = os.path.join(self.output_path, f"anomalies_{name}.csv")
        anomalies.to_csv(file_path, index=False)

        self.superdb.create_pool(name=f"anomalies_{name}", layout_order="asc", thresh=None)
        self.superdb.load_data_to_branch(f"anomalies_{name}", "main", anomalies.to_csv(index=False), csv_delim=",")

        print(f"{name} anomalies detected and saved.")
    
    def __get_isolationforest_detections(self, features_scaled, data):
        """Performs anomaly detection using Isolation Forest."""
        iso_forest = IsolationForest(contamination=0.05, random_state=42)
        iso_predictions = iso_forest.fit_predict(features_scaled)
        data['Anomaly_IsolationForest'] = (iso_predictions == -1).astype(int)
        anomalies = data[data['Anomaly_IsolationForest'] == 1]
        self.__save_anomalies(anomalies, "isolation_forest")
        return anomalies

    def __get_autoencoders_detections(self, features_scaled, data):
        """Performs anomaly detection using a deep learning autoencoder."""
        model = keras.Sequential([
            keras.layers.Dense(32, activation='relu', input_shape=(features_scaled.shape[1],)),
            keras.layers.Dense(16, activation='relu'),
            keras.layers.Dense(8, activation='relu'),
            keras.layers.Dense(16, activation='relu'),
            keras.layers.Dense(32, activation='relu'),
            keras.layers.Dense(features_scaled.shape[1], activation='linear')
        ])
        model.compile(optimizer='adam', loss='mse')

        model.fit(features_scaled, features_scaled, epochs=10, batch_size=64, validation_split=0.1, verbose=1)

        reconstructions = model.predict(features_scaled)
        mse = np.mean(np.abs(reconstructions - features_scaled), axis=1)

        threshold = np.percentile(mse, 90)
        data['Anomaly_Autoencoder'] = (mse > threshold).astype(int)
        anomalies = data[data['Anomaly_Autoencoder'] == 1]
        self.__save_anomalies(anomalies, "autoencoder")
        return anomalies
    
    def __get_ocsvm_detections(self, features_scaled, data):
        """Performs anomaly detection using One-Class SVM."""
        oc_svm = OneClassSVM(kernel="rbf", nu=0.05)
        predictions = oc_svm.fit_predict(features_scaled)
        data['Anomaly_OCSVM'] = (predictions == -1).astype(int)
        anomalies = data[data['Anomaly_OCSVM'] == 1]
        self.__save_anomalies(anomalies, "ocsvm")
        return anomalies

    def __get_lof_detections(self, features_scaled, data):
        """Performs anomaly detection using Local Outlier Factor (LOF)."""
        lof = LocalOutlierFactor(n_neighbors=20, contamination=0.05)
        predictions = lof.fit_predict(features_scaled)
        data['Anomaly_LOF'] = (predictions == -1).astype(int)
        anomalies = data[data['Anomaly_LOF'] == 1]
        self.__save_anomalies(anomalies, "lof")
        return anomalies

    def __get_pca_detections(self, features_scaled, data):
        """Performs anomaly detection using PCA-based reconstruction error."""
        pca = PCA(n_components=0.95)
        reduced = pca.fit_transform(features_scaled)
        reconstructed = pca.inverse_transform(reduced)
        reconstruction_error = np.mean((features_scaled - reconstructed) ** 2, axis=1)

        threshold = np.percentile(reconstruction_error, 95)
        data['Anomaly_PCA'] = (reconstruction_error > threshold).astype(int)
        anomalies = data[data['Anomaly_PCA'] == 1]
        self.__save_anomalies(anomalies, "pca")
        return anomalies

    def __get_rrcf_detections(self, features_scaled, data):
        """Performs anomaly detection using Robust Random Cut Forest (RRCF)."""
        num_trees = 100
        tree_size = 256
        forest = [rrcf.RCTree() for _ in range(num_trees)]

        anomaly_scores = np.zeros(len(features_scaled))

        for i, point in enumerate(features_scaled):
            for tree in forest:
                if len(tree.leaves) < tree_size:
                    tree.insert_point(point, index=i)
                else:
                    tree.forget_point(i)
                    tree.insert_point(point, index=i)
                anomaly_scores[i] += tree.codisp(i)

        threshold = np.percentile(anomaly_scores, 95)
        data['Anomaly_RRCF'] = (anomaly_scores > threshold).astype(int)
        anomalies = data[data['Anomaly_RRCF'] == 1]
        self.__save_anomalies(anomalies, "rrcf")
        return anomalies

    def perform_anomaly_detections(self):
        """Runs anomaly detection using multiple methods and combines results via ensemble voting."""
        
        # Load data
        data = self.get_pool_data(log_type='raw')        
        numeric_features = data.select_dtypes(include=['number'])
        
        if numeric_features.empty:
            raise ValueError("No numeric columns found! Ensure logs contain valid numeric data.")

        # Handle NaNs before scaling
        numeric_features = numeric_features.fillna(0)

        # Normalize data
        scaler = MinMaxScaler()
        features_scaled = scaler.fit_transform(numeric_features)

        # Run different anomaly detection methods
        anomalies_autoencoder = self.__get_autoencoders_detections(features_scaled, data)
        anomalies_isolation = self.__get_isolationforest_detections(features_scaled, data)
        anomalies_ocsvm = self.__get_ocsvm_detections(features_scaled, data)
        anomalies_lof = self.__get_lof_detections(features_scaled, data)
        anomalies_pca = self.__get_pca_detections(features_scaled, data)
        #anomalies_rrcf = self.__get_rrcf_detections(features_scaled, data)

        # Combine results using ensemble voting
        anomaly_scores = (
            data["Anomaly_Autoencoder"] +
            data["Anomaly_IsolationForest"] +
            data["Anomaly_OCSVM"] +
            data["Anomaly_LOF"] +
            data["Anomaly_PCA"] #+
            #data["Anomaly_RRCF"]
        )

        # Consider a data point anomalous if at least 3 methods agree
        data["Anomaly_Ensemble"] = (anomaly_scores >= 3).astype(int)
        anomalies_ensemble = data[data["Anomaly_Ensemble"] == 1]
        self.__save_anomalies(anomalies_ensemble, "ensemble")

        print("Anomaly detection complete! Ensemble results saved.")

    def execute(self):
        """Runs the entire pipeline."""
        try:
            self.get_sigma_rules()
            self.get_sigma_detections()
            self.perform_anomaly_detections()
            # subprocess.run("pkill -f 'super db serve'", shell=True, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"Execution failed: {e}")

