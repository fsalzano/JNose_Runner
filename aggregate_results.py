import os
import json
import csv
import logging
import psycopg2
from pathlib import Path
from collections import defaultdict

# Database configuration (using existing credentials)
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "10.64.160.163"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "database": os.environ.get("POSTGRES_DATABASE", "testing-agentic-prs"),
    "user": os.environ.get("POSTGRES_USER", "user"),
    "password": os.environ.get("POSTGRES_PASSWORD", "Un!m0l1s3"),
}

BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"
OUTPUT_FILE = BASE_DIR / "aggregated_results.jsonl"
LOG_FILE = BASE_DIR / "logs" / "aggregation.log"

# Ensure logs directory exists
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_pr_data():
    """Fetches PR and commit information from the database."""
    logging.info("Fetching PR data from database...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        query = """
            SELECT 
                pr.id, 
                pr.number, 
                r.name_with_owner,
                pr.base_commit_oid, 
                pr.merge_commit_oid, 
                pr.head_commit_oid
            FROM pull_requests pr
            JOIN repositories r ON pr.base_repository_id = r.id
            WHERE pr.has_test_files = TRUE;
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        logging.error(f"Database error: {e}")
        return []

def parse_jnose_csv(file_path):
    """Parses a JNose CSV file and returns smell counts per file, category, and method."""
    if file_path is None or not file_path.exists():
        return None

    data = {
        "total_smells": 0,
        "categories": defaultdict(int),
        "files": defaultdict(lambda: {
            "total_smells": 0,
            "categories": defaultdict(int),
            "methods": defaultdict(lambda: defaultdict(int))
        })
    }

    try:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                prod_file = row.get('production_file', 'unknown')
                smell_cat = row.get('smell', 'unknown')
                methods_str = row.get('method', '')
                
                # Split methods (they can be comma-separated)
                methods = [m.strip() for m in methods_str.split(',') if m.strip()]
                if not methods:
                    methods = ['unknown_method']

                # JNose CSV usually has one row per smell type per class/file, 
                # but might list multiple methods. 
                # For aggregation, we count the occurrence of the smell.
                data["total_smells"] += 1
                data["categories"][smell_cat] += 1
                
                f_data = data["files"][prod_file]
                f_data["total_smells"] += 1
                f_data["categories"][smell_cat] += 1
                
                for m in methods:
                    f_data["methods"][m][smell_cat] += 1
                    
        return data
    except Exception as e:
        logging.error(f"Error parsing {file_path}: {e}")
        return None

def calculate_delta(before, after):
    """Calculates the difference between after and before metrics."""
    delta = {
        "total_smells": (after.get("total_smells", 0) if after else 0) - (before.get("total_smells", 0) if before else 0),
        "categories": {}
    }
    
    all_cats = set((before.get("categories", {}).keys() if before else [])) | \
               set((after.get("categories", {}).keys() if after else []))
    
    for cat in all_cats:
        b_val = before.get("categories", {}).get(cat, 0) if before else 0
        a_val = after.get("categories", {}).get(cat, 0) if after else 0
        delta["categories"][cat] = a_val - b_val
        
    return delta

def process_aggregation():
    pr_rows = fetch_pr_data()
    if not pr_rows:
        logging.error("No PR data to process.")
        return

    logging.info(f"Processing {len(pr_rows)} PR records...")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as out_f:
        for pr_id, pr_num, repo_full_name, base_sha, merge_sha, head_sha in pr_rows:
            # TRY BOTH: name_with_owner (owner_repo) and short name
            repo_short_name = repo_full_name.split('/')[-1]
            repo_disk_name_long = repo_full_name.replace('/', '_')
            
            target_sha = merge_sha if merge_sha else head_sha
            
            # Paths to check
            base_file = None
            target_file = None
            
            possible_base_paths = [
                RESULTS_DIR / f"{repo_disk_name_long}_{base_sha}.csv",
                RESULTS_DIR / f"{repo_short_name}_{base_sha}.csv"
            ]
            possible_target_paths = [
                RESULTS_DIR / f"{repo_disk_name_long}_{target_sha}.csv",
                RESULTS_DIR / f"{repo_short_name}_{target_sha}.csv"
            ]
            
            for p in possible_base_paths:
                if p.exists():
                    base_file = p
                    break
            
            for p in possible_target_paths:
                if p.exists():
                    target_file = p
                    break
            
            if not base_file and not target_file:
                continue
            
            if not base_file:
                logging.warning(f"Base CSV missing for PR {pr_num} ({repo_full_name}) at {base_sha}")
            if not target_file:
                logging.warning(f"Target CSV missing for PR {pr_num} ({repo_full_name}) at {target_sha}")

            before_data = parse_jnose_csv(base_file)
            after_data = parse_jnose_csv(target_file)
            
            # Prepare result object
            result = {
                "pr_id": pr_id,
                "pr_number": pr_num,
                "repository": repo_full_name,
                "base_commit": base_sha,
                "target_commit": target_sha,
                "metrics": {
                    "before": before_data,
                    "after": after_data,
                    "delta": calculate_delta(before_data, after_data)
                }
            }
            
            # Convert defaultdicts to normal dicts for JSON serialization
            def dictify(obj):
                if isinstance(obj, defaultdict):
                    return {k: dictify(v) for k, v in obj.items()}
                return obj

            out_f.write(json.dumps(dictify(result)) + '\n')

    logging.info(f"Aggregation completed. Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    process_aggregation()
