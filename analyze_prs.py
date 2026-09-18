import json
import os
import subprocess
import logging
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Constants and paths
BASE_DIR = Path(__file__).resolve().parent

# Logging configuration
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / "analysis.log"),
        # Removed StreamHandler to avoid interference with tqdm
    ]
)
# The repositories directory is outside the project folder, at the path specified by the user
REPOS_DIR = Path("/home/stakelab/testing-agentic-prs/data/repos")
RESULTS_DIR = BASE_DIR / "results"
TOOLS_DIR = BASE_DIR / "tools"
JNOSE_JAR = TOOLS_DIR / "jnose-core.jar"
RUNNER_BIN = TOOLS_DIR / "bin"
CHECKOUT_INFO_FILE = BASE_DIR / "checkout_info.json"

# Lock dictionary for repositories to avoid concurrent checkouts on the same repo
repo_locks = {}
repo_locks_lock = threading.Lock()

def get_repo_lock(repo_name):
    """Returns a lock for a specific repository."""
    with repo_locks_lock:
        if repo_name not in repo_locks:
            repo_locks[repo_name] = threading.Lock()
        return repo_locks[repo_name]

def run_command(command, cwd=None):
    """Executes a shell command and returns the result."""
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

def checkout_commit(repo_path, commit_hash):
    """Checks out a specific commit."""
    # Use -f to force checkout if there are local changes
    success, output_or_error = run_command(["git", "checkout", "-f", commit_hash], cwd=repo_path)
    if not success:
        error_msg = f"Error during checkout of {commit_hash} in {repo_path}: {output_or_error}"
        logging.error(error_msg)
        print(error_msg)
    return success

def run_jnose(repo_path, output_name, pr_id):
    """Runs JNose via Docker and saves the results with a specific name."""
    # Each PR needs its own temporary output directory to avoid conflicts in multithreading
    temp_output_dir = RESULTS_DIR / "temp_jnose" / str(pr_id)
    temp_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Pre-clean the temporary folder to avoid residues
    for f in temp_output_dir.glob("*"):
        if f.is_file():
            f.unlink()

    logging.info(f"Running JNose on {repo_path} (PR: {pr_id})")
    
    # Docker path mapping: REPOS_DIR -> /projects, TOOLS_DIR -> /tools, temp_output_dir -> /results
    repo_rel_path = os.path.relpath(repo_path, REPOS_DIR)
    
    docker_cmd = [
        "docker", "run", "--rm",
        "-v", f"{REPOS_DIR.resolve()}:/projects:ro",
        "-v", f"{TOOLS_DIR.resolve()}:/tools:ro",
        "-v", f"{temp_output_dir.resolve()}:/results",
        "eclipse-temurin:25-jdk",
        "java", "-cp", "/tools/bin:/tools/jnose-core.jar",
        "JNoseBatchRunner",
        f"/projects/{repo_rel_path}",
        "/results"
    ]
    
    success, output_or_error = run_command(docker_cmd)
    
    if not success:
        error_msg = f"Error during JNose execution for PR {pr_id} on {repo_path}: {output_or_error}"
        logging.error(error_msg)
        print(error_msg)
        return False

    # Move and rename results
    smells_csv = temp_output_dir / "test_smells.csv"
    if smells_csv.exists():
        final_csv = RESULTS_DIR / f"{output_name}.csv"
        smells_csv.replace(final_csv)
        msg = f"Result successfully saved to {final_csv}"
        logging.info(msg)
        print(msg)
        return True
    else:
        error_msg = f"test_smells.csv not found for PR {pr_id} after execution in {temp_output_dir}. Output: {output_or_error}"
        logging.error(error_msg)
        print(error_msg)
        return False

def process_single_pr(pr, repo_path):
    """Processes a single PR: checkout and JNose analysis for both base and target commits."""
    repo_name = pr['repo_name']
    lock = get_repo_lock(repo_name)
    
    try:
        # Use a lock to ensure only one thread is operating on the same repo folder at a time
        with lock:
            # 1. Base Commit Analysis
            if checkout_commit(repo_path, pr['base_commit']):
                output_name_base = f"{repo_name}_{pr['base_commit']}"
                run_jnose(repo_path, output_name_base, pr['pr_id'])
            
            # 2. Target Commit Analysis (merge or head)
            if checkout_commit(repo_path, pr['target_commit']):
                output_name_target = f"{repo_name}_{pr['target_commit']}"
                run_jnose(repo_path, output_name_target, pr['pr_id'])
                
    except Exception as e:
        logging.error(f"Exception during analysis of PR #{pr.get('pr_number', 'unknown')}: {str(e)}")
    
    # Cleanup temp directory for this PR
    temp_output_dir = RESULTS_DIR / "temp_jnose" / str(pr['pr_id'])
    if temp_output_dir.exists():
        import shutil
        shutil.rmtree(temp_output_dir)

def analyze_prs(limit=0, workers=32):
    """Loads PR info and starts multithreaded analysis."""
    if not CHECKOUT_INFO_FILE.exists():
        logging.error(f"File {CHECKOUT_INFO_FILE} not found.")
        return

    with open(CHECKOUT_INFO_FILE, "r") as f:
        data = json.load(f)

    # Identify PRs to analyze based on repository presence in REPOS_DIR
    prs_to_analyze = []
    
    # Debug: check REPOS_DIR content
    print(f"Checking REPOS_DIR: {REPOS_DIR.resolve()}")
    if REPOS_DIR.exists():
        subdirs = [d.name for d in REPOS_DIR.iterdir() if d.is_dir()]
        print(f"Found {len(subdirs)} directories in {REPOS_DIR.resolve()}. First 10: {subdirs[:10]}")
    else:
        print(f"CRITICAL: REPOS_DIR does not exist at {REPOS_DIR.resolve()}")

    for pr in data:
        # Use repo_full_name with '/' replaced by '_' to match the folder structure on server
        repo_name_on_disk = pr['repo_full_name'].replace('/', '_')
        repo_path = REPOS_DIR / repo_name_on_disk
        if repo_path.exists():
            prs_to_analyze.append((pr, repo_path))
        elif not prs_to_analyze and pr == data[0]:
            # Print a sample mismatch for the first entry to understand the difference
            print(f"Sample mismatch: PR repo_full_name is '{pr['repo_full_name']}', transformed to '{repo_name_on_disk}', expected path {repo_path.resolve()}")
            
        if limit > 0 and len(prs_to_analyze) >= limit:
            break

    if not prs_to_analyze:
        msg = f"No repositories found in {REPOS_DIR.resolve()} for PRs in {CHECKOUT_INFO_FILE.resolve()}."
        logging.warning(msg)
        print(msg)
        return

    logging.info(f"Starting analysis of {len(prs_to_analyze)} PRs with {workers} workers.")
    
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # We use a list to keep track of futures and tqdm for the progress bar
        futures = [executor.submit(process_single_pr, pr, repo_path) for pr, repo_path in prs_to_analyze]
        
        # tqdm updates when each future finishes
        for _ in tqdm(futures, desc="PR Analysis"):
            _.result() # Wait for completion and raise exceptions if any occurred inside the thread

if __name__ == "__main__":
    # Analyze all available PRs with 8 workers
    analyze_prs(limit=0, workers=8)
