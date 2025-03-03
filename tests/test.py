import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from taptap.core.musashi import LogAnalyzer

def main():
    print("Starting Log Analysis...")

    # Define paths for logs and output
    log_file = 'tests/sample.evtx'
    output_dir = 'tests/output'
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Initialize and execute the log analyzer
    log_analyzer = LogAnalyzer(log_file, output_dir, 'winevtx')
    
    # Run the full analysis
    log_analyzer.execute()

    print("Log Analysis Completed.")

if __name__ == "__main__":
    main()
