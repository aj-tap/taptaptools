from taptap.core.utils import *
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
    def __init__(self, log_path, result_path, log_format='winevtx', sigma_rule_path=None):
        self.output_path = os.path.abspath(result_path)
        self.log_path = os.path.abspath(log_path)
        self.superdb = SuperDBAPI()
        self.log_format = log_format
        self.df_results = ""

        if sigma_rule_path is None:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            self.sigma_rule_path = os.path.abspath(os.path.join(project_root, "sigma_rules", "rules", "windows"))
        else: 
            self.sigma_rule_path = os.path.abspath(sigma_rule_path)

        if os.path.isdir(self.sigma_rule_path):
            self.sigma_rule_path = os.path.join(self.sigma_rule_path, "**", "*.yml")

        # Ensure Sigma rule path exists
        if not os.path.exists(self.sigma_rule_path.split("**")[0]):
            raise ValueError(f"Error: Sigma rule path '{self.sigma_rule_path}' does not exist!")
        
        print(f"Input Log: {self.log_path}")
        print(f"Output path results: {self.output_path}")
        print(f"Using Log Format: {self.log_format}")
        print(f"Using Sigma rules path: {self.sigma_rule_path}")

    def get_root_directory(self):
        """Finds the root directory (taptap/) based on script location."""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while current_dir and not os.path.exists(os.path.join(current_dir, "core")):
            current_dir = os.path.dirname(current_dir)

        if not current_dir:
            raise RuntimeError("Error: Could not find taptap root directory!")

        return current_dir

    def set_lake(self):
        """Initializes the SuperDB data lake."""
        try:
            print("Initializing the lake...")
            root_dir = self.get_root_directory()
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

    def get_logs(self):
        """
        Loads logs from the specified file path and ingests them into the SuperDB lake.
        """
        try:
            print(f"Loading logs from: {self.log_path}")

            # Ensure the 'logs' pool exists in SuperDB
            self.superdb.create_pool(name='logs', layout_order='asc', layout_keys=[['EventTime']], thresh=None)

            # Get file extension
            file_ext = os.path.splitext(self.log_path)[-1].lower()

            if file_ext == ".csv":
                df = pd.read_csv(self.log_path, low_memory=False)
                df.columns = df.columns.str.replace(" ", "")  # Remove spaces in column names
                data = df.to_csv(index=False)
                self.superdb.load_data_to_branch('logs', 'main', data, csv_delim=',')
                print(f"CSV logs successfully ingested ({len(df)} records).")

            elif file_ext == ".tsv":
                df = pd.read_csv(self.log_path, sep="\t", low_memory=False)
                df.columns = df.columns.str.replace(" ", "")
                data = df.to_json(orient="records")
                self.superdb.load_data_to_branch('logs', 'main', data, csv_delim=',')
                print(f"TSV logs successfully ingested ({len(df)} records).")

            elif file_ext == ".json":
                df = pd.read_json(self.log_path, low_memory=False)
                df.columns = df.columns.str.replace(" ", "")
                data = df.to_json(orient="records")
                self.superdb.load_data_to_branch('logs', 'main', data, csv_delim=',')
                print(f"JSON logs successfully ingested ({len(df)} records).")

            elif file_ext == ".evtx":
                print("Parsing EVTX logs...")
                records_list = []
                parser = PyEvtxParser(self.log_path)

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
                        print(f"Error decoding JSON for Event Record ID: {event_id}")

                df = pd.DataFrame(records_list)
                data = df.to_csv(index=False)
                self.superdb.load_data_to_branch('logs', 'main', data, csv_delim=',')
                print(f"EVTX logs successfully ingested ({len(df)} records).")

            else:
                print("Unsupported file format. Only CSV, TSV, JSON, and EVTX are supported.")
                return None

        except Exception as e:
            print(f"Error in log processing: {e}")
            return None

    def get_sigma_rules(self):
        """Fetches the latest Sigma rules from GitHub."""
        github_api_url = "https://api.github.com/repos/SigmaHQ/sigma/releases/latest"
        local_dir = "sigma_rules"
        zip_file = "sigma_rules_latest.zip"
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

    def get_queries(self):
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

    def get_detections(self):
        """
        Executes queries in parallel, logs hits with rule titles, saves individual results, and builds a MITRE ATT&CK timeline.
        """
        all_results = []
        rule_hit_count = Counter()
        mitre_timeline = defaultdict(list)  # Stores MITRE techniques mapped to triggered rules
        
        queries = self.get_queries()  # Returns (rule_title, query, mitre_tags)    

        if not queries:
            print("⚠️ No queries were generated from Sigma rules. Skipping detections.")
            return

        def execute_query(query, rule_title, mitre_tags):
            try:
                fin_query = f"from logs | {query}"
                res = self.superdb.execute_query(query=fin_query)                 
                if not res:
                    #print(f"No results found for rule: {rule_title}")
                    return

                df = pd.DataFrame(res)
                df["SigmaRule"] = rule_title

                csv_output = os.path.join(self.output_path, f"{rule_title}.csv")
                df.to_csv(csv_output, index=False)

                all_results.append(df)
                rule_hit_count[rule_title] = len(df)

                # Log MITRE ATT&CK techniques
                for mitre in mitre_tags:
                    mitre_timeline[mitre].append(rule_title)

                print(f"Sigma Rule Triggered: {rule_title} ({len(df)} event matches)")

            except Exception as e:
                print(f"Error executing query for rule {rule_title}: {e}")

        print("\n=== Running Detections ===")
        with ThreadPoolExecutor() as executor:
            executor.map(lambda q: execute_query(q[1], q[0], q[2]), queries)

        if all_results:
            self.df_results = pd.concat(all_results, ignore_index=True)
            merged_output = os.path.join(self.output_path, "all-sigma-results.csv")
            self.df_results.to_csv(merged_output, index=False)
            print(f"All Sigma results merged and saved to {merged_output}")

            # Ingest merged results into SuperDB lake
            data = self.df_results.to_csv(index=False)
            self.superdb.create_pool(name='detections', layout_order='asc', thresh=None)
            self.superdb.load_data_to_branch('detections', 'main', data, csv_delim=',')            
        else:
            print("No detections found.")
        print("Detection process completed.")

    def execute(self):
        """Runs the entire pipeline."""
        try:
            self.get_sigma_rules()
            self.set_lake()
            self.get_logs()            
            self.get_detections()
            subprocess.run("pkill -f 'super db serve'", shell=True, stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"Execution failed: {e}")

