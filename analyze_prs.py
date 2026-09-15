import json
import os
import subprocess
import logging
from pathlib import Path
from tqdm import tqdm

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("analysis.log"),
        # Removed StreamHandler to avoid interference with tqdm
    ]
)

# Constants and paths
BASE_DIR = Path(__file__).resolve().parent
# The repositories directory is outside the project folder, at the same level
REPOS_DIR = BASE_DIR.parent / "repos"
RESULTS_DIR = BASE_DIR / "results"
TOOLS_DIR = BASE_DIR / "tools"
JNOSE_JAR = TOOLS_DIR / "jnose-core.jar"
RUNNER_BIN = TOOLS_DIR / "bin"
CHECKOUT_INFO_FILE = BASE_DIR / "checkout_info.json"

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
    success, error = run_command(["git", "checkout", "-f", commit_hash], cwd=repo_path)
    if not success:
        logging.error(f"Error during checkout of {commit_hash} in {repo_path}: {error}")
    return success

def run_jnose(repo_path, output_name):
    """Runs JNose via Docker and saves the results with a specific name."""
    # The runner saves test_class_summary.csv and test_smells.csv in a folder
    # We rename test_smells.csv to <repo>_<hash>.csv
    
    temp_output_dir = RESULTS_DIR / "temp_jnose"
    temp_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Pre-clean the temporary folder to avoid residues
    for f in temp_output_dir.glob("*"):
        if f.is_file():
            f.unlink()

    logging.info(f"Running JNose on {repo_path}")
    
    # Docker path mapping: REPOS_DIR -> /projects, TOOLS_DIR -> /tools, temp_output_dir -> /results
    repo_rel_path = os.path.relpath(repo_path, REPOS_DIR)
    
    docker_cmd = [
        "docker", "run", "--rm",
        "-v", f"{REPOS_DIR}:/projects:ro",
        "-v", f"{TOOLS_DIR}:/tools:ro",
        "-v", f"{temp_output_dir}:/results",
        "eclipse-temurin:25-jdk",
        "java", "-cp", "/tools/bin:/tools/jnose-core.jar",
        "JNoseBatchRunner",
        f"/projects/{repo_rel_path}",
        "/results"
    ]
    
    success, error = run_command(docker_cmd)
    
    if not success:
        logging.error(f"Error during JNose execution: {error}")
        return False

    # Move and rename results
    smells_csv = temp_output_dir / "test_smells.csv"
    if smells_csv.exists():
        final_csv = RESULTS_DIR / f"{output_name}.csv"
        smells_csv.replace(final_csv)
        logging.info(f"Result saved in {final_csv}")
        return True
    else:
        logging.error(f"test_smells.csv not found after execution.")
        return False

def analyze_prs(limit=0):
    """Loads PR info and starts analysis."""
    if not CHECKOUT_INFO_FILE.exists():
        logging.error(f"File {CHECKOUT_INFO_FILE} not found.")
        return

    with open(CHECKOUT_INFO_FILE, "r") as f:
        data = json.load(f)

    # Identify PRs to analyze based on repository presence in REPOS_DIR
    prs_to_analyze = []
    
    for pr in data:
        repo_path = REPOS_DIR / pr['repo_name']
        if repo_path.exists():
            prs_to_analyze.append((pr, repo_path))
            
        if limit > 0 and len(prs_to_analyze) >= limit:
            break

    if not prs_to_analyze:
        logging.warning(f"No repositories found in {REPOS_DIR} for PRs in {CHECKOUT_INFO_FILE}.")
        return

    logging.info(f"Starting analysis of {len(prs_to_analyze)} PRs.")
    
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Use tqdm to show analysis progress
    for pr, repo_path in tqdm(prs_to_analyze, desc="PR Analysis"):
        try:
            # 1. Base Commit Analysis
            if checkout_commit(repo_path, pr['base_commit']):
                output_name_base = f"{pr['repo_name']}_{pr['base_commit']}"
                run_jnose(repo_path, output_name_base)
            
            # 2. Target Commit Analysis (merge or head)
            if checkout_commit(repo_path, pr['target_commit']):
                output_name_target = f"{pr['repo_name']}_{pr['target_commit']}"
                run_jnose(repo_path, output_name_target)
                
        except Exception as e:
            logging.error(f"Exception during analysis of PR #{pr['pr_number']}: {str(e)}")
            continue

if __name__ == "__main__":
    # Analyze all available PRs
    analyze_prs(limit=0)
