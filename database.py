#!/usr/bin/env python3
"""
Hamster PR Test Analysis Pipeline
=================================
Analyzes test code changes in Pull Requests using Hamster and CLDK:
1. Queries PostgreSQL for PRs with test files.
2. Extracts minimal 1-level + transitive superclass dependency slices for Base and Head commits directly from Git.
3. Executes CLDK & Hamster on the isolated slices.
4. Computes model-level test diffs (added, modified, deleted) and co-evolution classifications.
5. Flags suspicious or unsupported test formats for manual double-checking.
6. Persists results and resumption status to PostgreSQL (and optional disk JSON files).
"""

import argparse
import concurrent.futures
from concurrent.futures.process import BrokenProcessPool
import gzip
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import psycopg2
import psycopg2.extras

# Add hamster/src to sys.path
BASE_DIR = Path(__file__).resolve().parent
HAMSTER_SRC = BASE_DIR / "hamster" / "src"
if HAMSTER_SRC.exists() and str(HAMSTER_SRC) not in sys.path:
    sys.path.insert(0, str(HAMSTER_SRC))

try:
    from cldk import CLDK
    from cldk.analysis import AnalysisLevel
    from hamster.code_analysis.common import CommonAnalysis
    from hamster.code_analysis.model.models import ProjectAnalysis
    from hamster.code_analysis.test_statistics import ProjectAnalysisInfo, TestClassAnalysisInfo
    from hamster.utils.pretty.progress_bar import ProgressBarFactory


    # Patch rich Progress in parallel workers to prevent stdout lock contention
    class SilentProgressBar:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def track(self, iterable, **kwargs):
            return iterable


    ProgressBarFactory.get_progress_bar = classmethod(lambda cls: SilentProgressBar())
except ImportError as e:
    logging.warning("Hamster/CLDK imports failed: %s. Ensure environment has cldk and hamster dependencies.", e)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("hamster_pr_analyzer")


def setup_logging(log_file: Optional[Path] = None, log_level: int = logging.INFO):
    """Sets up unified logging to console and a rotating/timestamped log file."""
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Configure root/module logger
    logger.setLevel(log_level)
    logger.handlers.clear()

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    # File Handler
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_file), mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        logger.addHandler(file_handler)
        logger.info("Logging initialized. Output will be saved to %s", log_file.resolve())


# Regex for parsing Java files
IMPORT_REGEX = re.compile(r"^\s*import\s+(?:static\s+)?([\w.]+)\s*;", re.MULTILINE)
PACKAGE_REGEX = re.compile(r"^\s*package\s+([\w.]+)\s*;", re.MULTILINE)
EXTENDS_REGEX = re.compile(r"\bclass\s+\w+(?:<[^>]+>)?\s+extends\s+([A-Za-z0-9_]+)", re.MULTILINE)
IMPLEMENTS_REGEX = re.compile(
    r"\bclass\s+\w+(?:<[^>]+>)?\s+(?:extends\s+[A-Za-z0-9_]+\s+)?implements\s+([A-Za-z0-9_,\s]+)\{", re.MULTILINE)


# ==============================================================================
# Database Utilities
# ==============================================================================

def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "10.64.160.163"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        database=os.environ.get("POSTGRES_DATABASE", "testing-agentic-prs"),
        user=os.environ.get("POSTGRES_USER", "user"),
        password=os.environ.get("POSTGRES_PASSWORD", "Un!m0l1s3"),
    )


def init_db(conn):
    """Creates the tracking and results table in PostgreSQL if it does not exist."""
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pull_request_analysis
            (
                pr_id
                TEXT
                PRIMARY
                KEY
                REFERENCES
                pull_requests
            (
                id
            ),
                status TEXT NOT NULL, -- 'SUCCESS', 'FAILED', 'NO_TESTS', 'SKIPPED'
                error_message TEXT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Quality & Manual Review Flags
                needs_manual_review BOOLEAN DEFAULT FALSE,
                review_reasons TEXT, -- Comma-separated or JSON list of reasons
                test_files_count INT DEFAULT 0,

                -- Co-evolution High-level Metrics
                co_evolution_type TEXT, -- 'CO_EVOLUTION_PURE', 'TEST_FOCUSED_PURE', 'MIXED', 'NO_PROD_CHANGED'
                co_evolution_ratio DOUBLE PRECISION,

                -- Test Deltas
                added_tests_count INT DEFAULT 0,
                modified_tests_count INT DEFAULT 0,
                deleted_tests_count INT DEFAULT 0,

                -- Assertion, Complexity, Mock, and Size Deltas
                total_assertions_delta INT DEFAULT 0,
                total_complexity_delta INT DEFAULT 0,
                total_mocks_delta INT DEFAULT 0,
                total_ncloc_delta INT DEFAULT 0,

                -- Detailed JSON payloads
                hamster_diff_json JSONB,
                base_model_json JSONB,
                head_model_json JSONB
                );
            CREATE INDEX IF NOT EXISTS idx_pr_hamster_status ON pull_request_analysis(status);
            CREATE INDEX IF NOT EXISTS idx_pr_hamster_review ON pull_request_analysis(needs_manual_review);
            """
        )
        conn.commit()


# ==============================================================================
# Git Helpers (Thread-safe, non-destructive via git show / cat-file)
# ==============================================================================

def run_git(repo_dir: Path, args: List[str], check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo_dir)] + args,
        check=check,
        capture_output=True,
        text=True,
        errors="replace",
    )


def check_commit_exists(repo_dir: Path, commit_sha: str) -> bool:
    if not commit_sha:
        return False
    res = run_git(repo_dir, ["cat-file", "-e", f"{commit_sha}^{{commit}}"], check=False)
    return res.returncode == 0


def ensure_commit(repo_dir: Path, commit_sha: str, pr_number: Optional[int] = None) -> bool:
    if not commit_sha:
        return False
    if check_commit_exists(repo_dir, commit_sha):
        return True

    # 1. Fetch the commit directly
    run_git(repo_dir, ["fetch", "origin", commit_sha], check=False)
    if check_commit_exists(repo_dir, commit_sha):
        return True

    # 2. Fetch PR refs from GitHub
    if pr_number:
        run_git(repo_dir, ["fetch", "origin", f"pull/{pr_number}/head:pr_{pr_number}_head"], check=False)
        run_git(repo_dir, ["fetch", "origin", f"pull/{pr_number}/merge:pr_{pr_number}_merge"], check=False)
        if check_commit_exists(repo_dir, commit_sha):
            return True

    # 3. Fetch all PR refs fallback
    run_git(repo_dir, ["fetch", "origin", "+refs/pull/*/head:refs/remotes/origin/pr/*"], check=False)
    return check_commit_exists(repo_dir, commit_sha)


# In-memory cache for git ls-tree results to avoid redundant tree traversals per commit
_COMMIT_FILES_CACHE: Dict[Tuple[str, str], List[str]] = {}


def get_all_java_files_at_commit(repo_dir: Path, commit_sha: str) -> List[str]:
    """Returns a list of all .java file paths in the repo at the given commit, using an in-memory cache."""
    cache_key = (str(repo_dir), commit_sha)
    if cache_key in _COMMIT_FILES_CACHE:
        return _COMMIT_FILES_CACHE[cache_key]

    res = run_git(repo_dir, ["ls-tree", "-r", "--name-only", commit_sha], check=False)
    if res.returncode == 0:
        files = [line.strip().replace("\\", "/") for line in res.stdout.splitlines() if line.strip().endswith(".java")]
        _COMMIT_FILES_CACHE[cache_key] = files
        return files
    return []


def get_batch_file_contents(repo_dir: Path, commit_sha: str, paths: List[str]) -> Dict[str, str]:
    """
    Fetches contents for multiple files at a commit using a single `git cat-file --batch` process.
    Up to 50x faster than spawning individual `git show` subprocesses per file on Windows.
    """
    if not paths or not commit_sha:
        return {}

    clean_paths = [p.replace("\\", "/").lstrip("/") for p in paths]
    input_lines = "\n".join(f"{commit_sha}:{p}" for p in clean_paths) + "\n"

    res = subprocess.run(
        ["git", "-C", str(repo_dir), "cat-file", "--batch"],
        input=input_lines.encode("utf-8"),
        capture_output=True,
        check=False,
    )
    if res.returncode != 0:
        return {}

    results = {}
    stream = io.BytesIO(res.stdout)

    for path in clean_paths:
        header_line = stream.readline().decode("utf-8", errors="replace").strip()
        if not header_line or "missing" in header_line:
            continue
        parts = header_line.split()
        if len(parts) >= 3 and parts[1] == "blob":
            size = int(parts[2])
            content_bytes = stream.read(size)
            # Consume trailing newline
            stream.read(1)
            results[path] = content_bytes.decode("utf-8", errors="replace")

    return results


def get_file_content_at_commit(repo_dir: Path, commit_sha: str, file_path: str) -> Optional[str]:
    """Convenience helper for single file extraction."""
    res = get_batch_file_contents(repo_dir, commit_sha, [file_path])
    norm_path = file_path.replace("\\", "/").lstrip("/")
    return res.get(norm_path)


# ==============================================================================
# Fast Dependency Slice Extraction
# ==============================================================================

def extract_dependency_closure(
        repo_dir: Path,
        commit_sha: str,
        changed_test_files: List[str],
        all_repo_java_files: List[str],
) -> Dict[str, str]:
    """
    High-performance batched extraction of the minimal dependency closure:
    Pass 1: Batch fetch all changed test files in 1 process.
    Pass 2: Parse test files in memory for imports, superclasses, and package classes.
    Pass 3: Batch fetch all 1st-level dependencies in 1 process.
    Pass 4: Batch fetch transitive superclasses of dependencies.

    Returns:
        Dict[str, str]: Mapping of relative file path -> file content.
    """
    if not changed_test_files or not commit_sha:
        return {}

    # Index files by simple class name and relative paths
    file_map: Dict[str, str] = {p.replace("\\", "/").lstrip("/"): p for p in all_repo_java_files}
    simple_name_map: Dict[str, List[str]] = {}
    for p in file_map:
        simple = Path(p).stem
        simple_name_map.setdefault(simple, []).append(p)

    # Pass 1: Batch fetch all changed test files
    collected_files: Dict[str, str] = get_batch_file_contents(repo_dir, commit_sha, changed_test_files)
    if not collected_files:
        return {}

    visited: Set[str] = set(collected_files.keys())
    needed_deps: Set[str] = set()

    # Pass 2: Parse test files in-memory
    for test_path, content in list(collected_files.items()):
        # 1. Direct Imports
        for imp in IMPORT_REGEX.findall(content):
            imp_suffix = imp.replace(".", "/") + ".java"
            for full_path in file_map:
                if full_path.endswith(imp_suffix) and full_path not in visited:
                    needed_deps.add(full_path)

        # 2. Direct Superclasses & Interfaces
        for super_name in EXTENDS_REGEX.findall(content):
            for candidate in simple_name_map.get(super_name, []):
                if candidate not in visited:
                    needed_deps.add(candidate)

        for impl_group in IMPLEMENTS_REGEX.findall(content):
            for iface_name in impl_group.split(","):
                iface_name = iface_name.strip().split("<")[0].strip()
                for candidate in simple_name_map.get(iface_name, []):
                    if candidate not in visited:
                        needed_deps.add(candidate)

        # 3. Same-package classes referenced in test file
        pkg_match = PACKAGE_REGEX.search(content)
        if pkg_match:
            pkg_dir = pkg_match.group(1).replace(".", "/")
            for full_path in file_map:
                if pkg_dir in full_path and full_path not in visited:
                    simple_name = Path(full_path).stem
                    if re.search(r"\b" + re.escape(simple_name) + r"\b", content):
                        needed_deps.add(full_path)

    # Pass 3: Batch fetch all 1st-level dependencies in one shot
    if needed_deps:
        dep_contents = get_batch_file_contents(repo_dir, commit_sha, list(needed_deps))
        collected_files.update(dep_contents)
        visited.update(dep_contents.keys())

        # Pass 4: Transitive superclasses of dependencies
        transitive_supers: Set[str] = set()
        for dep_path, dep_content in dep_contents.items():
            for super_name in EXTENDS_REGEX.findall(dep_content):
                for candidate in simple_name_map.get(super_name, []):
                    if candidate not in visited:
                        transitive_supers.add(candidate)

        if transitive_supers:
            trans_contents = get_batch_file_contents(repo_dir, commit_sha, list(transitive_supers))
            collected_files.update(trans_contents)

    return collected_files


# ==============================================================================
# Hamster Execution on Slice
# ==============================================================================

def run_hamster_on_slice(
        file_dict: Dict[str, str],
        dataset_name: str,
        seed_test_files: List[str],
        prod_files: List[str],
) -> Optional[ProjectAnalysis]:
    """
    Writes dependency slice files to a temporary directory and executes CLDK + Hamster.
    Explicitly categorizes classes based on ground-truth seed test files and production files
    from the database, bypassing brittle directory path heuristics.
    """
    if not file_dict:
        return None

    with tempfile.TemporaryDirectory(prefix="hamster_slice_") as temp_dir:
        temp_path = Path(temp_dir)
        for rel_path, content in file_dict.items():
            target = temp_path / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8", errors="ignore")

        try:
            cldk = CLDK(language="java").analysis(
                project_path=str(temp_path),
                analysis_backend_path=None,
                analysis_level=AnalysisLevel.symbol_table,
                analysis_json_path=None,
            )

            # Categorize classes explicitly by ground-truth seed test files & prod files
            test_class_methods: Dict[str, List[str]] = {}
            application_classes: List[str] = []
            test_utility_classes: List[str] = []

            norm_seed_tests = {p.replace("\\", "/").lstrip("/") for p in seed_test_files}
            norm_prod_files = {p.replace("\\", "/").lstrip("/") for p in prod_files}

            common_analyzer = CommonAnalysis(cldk)

            for q_class in cldk.get_classes():
                java_file = cldk.get_java_file(q_class)
                if not java_file:
                    continue
                norm_java = java_file.replace("\\", "/").lstrip("/")

                is_seed_test = any(norm_java.endswith(st) or st.endswith(norm_java) for st in norm_seed_tests)
                is_prod = any(norm_java.endswith(pf) or pf.endswith(norm_java) for pf in norm_prod_files)

                if is_seed_test:
                    methods = cldk.get_methods_in_class(q_class)
                    testing_frameworks = common_analyzer.get_testing_frameworks_for_class(q_class)

                    # 1. Primary: Use Hamster's exact test method detection
                    test_methods = [
                        m_sig for m_sig in methods
                        if common_analyzer.is_test_method(m_sig, q_class, testing_frameworks)
                    ]

                    # 2. Fallback: If no standard @Test annotations found (e.g. custom testing DSL),
                    # include all non-constructor/callable scenario methods
                    if not test_methods:
                        test_methods = [
                            m_sig for m_sig in methods
                            if not m_sig.startswith("<init>") and not m_sig.startswith("<clinit>")
                        ]

                    if test_methods:
                        test_class_methods[q_class] = test_methods
                    else:
                        test_utility_classes.append(q_class)
                elif is_prod:
                    application_classes.append(q_class)
                else:
                    # Dependencies from test imports (fixtures, base test classes, helpers)
                    test_utility_classes.append(q_class)

            test_class_analysis_obj = TestClassAnalysisInfo(
                analysis=cldk,
                dataset_name=dataset_name,
                application_classes=application_classes,
                test_utility_classes=test_utility_classes,
            )

            test_class_analyses = []
            for test_class, test_methods in test_class_methods.items():
                try:
                    analysis = test_class_analysis_obj.get_test_class_analysis(
                        qualified_class_name=test_class,
                        test_methods=test_methods,
                    )
                    test_class_analyses.append(analysis)
                except Exception as e:
                    logger.warning("Failed analyzing test class %s: %s", test_class, e)

            return ProjectAnalysis(
                dataset_name=dataset_name,
                application_class_count=len(application_classes),
                application_method_count=0,
                application_cyclomatic_complexity=0,
                application_types=[],
                test_class_count=len(test_class_analyses),
                test_method_count=sum(len(tc.test_method_analyses) for tc in test_class_analyses),
                test_utility_class_count=len(test_utility_classes),
                test_utility_method_count=0,
                test_class_analyses=test_class_analyses,
            )
        except Exception as e:
            logger.warning("CLDK/Hamster analysis failed on slice %s: %s", dataset_name, e)
            return None


## ==============================================================================
# Model Diff & Co-Evolution Computation
# ==============================================================================

def _clean_fqn(fqn: Optional[str]) -> str:
    """Strips generics and array notation from a fully qualified name, preserving package hierarchy."""
    if not fqn:
        return ""
    return fqn.split("<")[0].replace("[]", "").strip()


def _matches_prod_fqn(candidate_fqn: Optional[str], prod_fqns: Set[str]) -> bool:
    """Checks whether a candidate FQN matches any production class FQN, including inner classes."""
    if not candidate_fqn or not prod_fqns:
        return False
    clean = _clean_fqn(candidate_fqn)
    if clean in prod_fqns:
        return True
    if "$" in clean and clean.split("$")[0] in prod_fqns:
        return True
    return False


def _get_class_focal_classes(test_class_analysis: Any) -> Set[str]:
    """Extracts candidate focal class FQNs for a test class from method focal classes and package naming."""
    focals: Set[str] = set()
    if not test_class_analysis:
        return focals

    methods = getattr(test_class_analysis, "test_method_analyses", []) or []
    if isinstance(test_class_analysis, dict):
        methods = test_class_analysis.get("test_method_analyses", []) or []

    for m in methods:
        f_list = getattr(m, "focal_classes", []) or []
        if isinstance(m, dict):
            f_list = m.get("focal_classes", []) or []
        for fc in f_list:
            f_cls = getattr(fc, "focal_class", "") if hasattr(fc, "focal_class") else (
                fc.get("focal_class", "") if isinstance(fc, dict) else "")
            clean = _clean_fqn(f_cls)
            if clean:
                focals.add(clean)

    q_name = getattr(test_class_analysis, "qualified_class_name", "") if hasattr(test_class_analysis,
                                                                                 "qualified_class_name") else (
        test_class_analysis.get("qualified_class_name", "") if isinstance(test_class_analysis, dict) else "")
    clean_q = _clean_fqn(q_name)
    if "." in clean_q:
        pkg, simple_name = clean_q.rsplit(".", 1)
        for suffix in ("Tests", "Test", "TestCase"):
            if simple_name.endswith(suffix) and len(simple_name) > len(suffix):
                focals.add(f"{pkg}.{simple_name[:-len(suffix)]}")
                break
        if simple_name.startswith("Test") and len(simple_name) > 4:
            focals.add(f"{pkg}.{simple_name[4:]}")
    return focals


def _callable_list_signature(call_list: Optional[List[Any]]) -> List[Tuple]:
    """Extracts a lightweight canonical signature from a list of CallableDetails for structural diffing."""
    if not call_list:
        return []
    sig = []
    for c in call_list:
        m_name = getattr(c, "method_name", None) or (c.get("method_name") if isinstance(c, dict) else None) or ""
        r_type = getattr(c, "receiver_type", None) or (c.get("receiver_type") if isinstance(c, dict) else None) or ""
        args = tuple(
            getattr(c, "argument_types", None) or (c.get("argument_types") if isinstance(c, dict) else None) or [])
        sig.append((m_name, _clean_fqn(r_type), args))
    return sig


def _count_assertions(method_analysis) -> int:
    """Helper to count total assertions in a test method."""
    count = 0
    seqs = getattr(method_analysis, "call_assertion_sequences", []) or []
    if isinstance(method_analysis, dict):
        seqs = method_analysis.get("call_assertion_sequences", []) or []
    for seq in seqs:
        asserts = getattr(seq, "assertion_details", []) or []
        if isinstance(seq, dict):
            asserts = seq.get("assertion_details", []) or []
        count += len(asserts)
    return count


def _has_assertions_or_verifications(method_analysis) -> bool:
    """Checks whether a test method uses assertions, mock verifications, or mocking."""
    if _count_assertions(method_analysis) > 0:
        return True
    is_mock = getattr(method_analysis, "is_mocking_used", False) or (
        method_analysis.get("is_mocking_used", False) if isinstance(method_analysis, dict) else False)
    if is_mock:
        return True
    mocks_num = getattr(method_analysis, "number_of_mocks_created", 0) or (
        method_analysis.get("number_of_mocks_created", 0) if isinstance(method_analysis, dict) else 0)
    if mocks_num > 0:
        return True

    seqs = getattr(method_analysis, "call_assertion_sequences", []) or []
    if isinstance(method_analysis, dict):
        seqs = method_analysis.get("call_assertion_sequences", []) or []
    for seq in seqs:
        calls = getattr(seq, "call_sequence_details", []) or []
        if isinstance(seq, dict):
            calls = seq.get("call_sequence_details", []) or []
        for c in calls:
            m_name = getattr(c, "method_name", "") or (c.get("method_name", "") if isinstance(c, dict) else "")
            sec = getattr(c, "secondary_assertion", False) or (
                c.get("secondary_assertion", False) if isinstance(c, dict) else False)
            if sec or m_name in ("verify", "never", "times", "atLeast", "atMost", "verifyNoMoreInteractions", "fail",
                                 "assertFail"):
                return True
    return False


def extract_changed_prod_methods_and_classes(
        repo_dir: Path,
        base_sha: str,
        head_sha: str,
        prod_files: List[str],
) -> Tuple[Set[str], Set[str]]:
    """
    Extracts exact Fully Qualified Names of changed production classes and their modified/added
    methods and constructors using git diff line numbers and Java regex parsing.
    Returns:
        (changed_prod_classes_fqn: Set[str], changed_prod_methods_fqn: Set[str])
    """
    changed_classes: Set[str] = set()
    changed_methods: Set[str] = set()
    if not prod_files or not base_sha or not head_sha:
        return changed_classes, changed_methods

    method_decl_regex = re.compile(
        r'(?:(?:public|protected|private|static|final|synchronized|abstract|default)\s+)*'
        r'(?:<[^>]+>\s+)?'
        r'(?:[\w<>\[\]]+\s+)?'
        r'(\w+)\s*\([^)]*\)\s*'
        r'(?:throws\s+[\w,\s]+)?\s*\{',
        re.MULTILINE,
    )

    java_prod_files = [p.replace("\\", "/").lstrip("/") for p in prod_files if p.endswith(".java")]
    if not java_prod_files:
        return changed_classes, changed_methods

    head_contents = get_batch_file_contents(repo_dir, head_sha, java_prod_files)

    file_to_fqn: Dict[str, str] = {}
    for path in java_prod_files:
        content = head_contents.get(path, "")
        stem = Path(path).stem
        m_pkg = PACKAGE_REGEX.search(content)
        fqn = f"{m_pkg.group(1)}.{stem}" if m_pkg else stem
        file_to_fqn[path] = fqn
        changed_classes.add(fqn)

    diff_res = run_git(repo_dir, ["diff", "-U0", base_sha, head_sha, "--"] + java_prod_files, check=False)
    if diff_res.returncode != 0 or not diff_res.stdout:
        return changed_classes, changed_methods

    current_file = None
    file_changed_lines: Dict[str, List[int]] = {}
    hunk_regex = re.compile(r'\+(\d+)(?:,(\d+))?')

    for line in diff_res.stdout.splitlines():
        if line.startswith("diff --git"):
            parts = line.split()
            if len(parts) >= 4 and parts[3].startswith("b/"):
                current_file = parts[3][2:].replace("\\", "/").lstrip("/")
                file_changed_lines[current_file] = []
        elif line.startswith("@@") and current_file:
            m = hunk_regex.search(line)
            if m:
                start = int(m.group(1))
                count = int(m.group(2)) if m.group(2) is not None else 1
                file_changed_lines[current_file].extend(range(start, start + max(count, 1)))

    for path, changed_lines in file_changed_lines.items():
        if not changed_lines:
            continue
        content = head_contents.get(path)
        if not content:
            continue

        class_fqn = file_to_fqn.get(path, Path(path).stem)
        class_stem = Path(path).stem

        for m in method_decl_regex.finditer(content):
            m_name = m.group(1)
            if m_name in ("if", "for", "while", "switch", "catch", "synchronized", "class", "interface", "enum",
                          "record"):
                continue
            start_pos = m.start()
            start_line = content.count("\n", 0, start_pos) + 1

            brace_count = 1
            pos = m.end()
            while pos < len(content) and brace_count > 0:
                char = content[pos]
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                pos += 1
            end_line = content.count("\n", 0, pos) + 1

            if any(start_line <= l <= end_line for l in changed_lines):
                changed_methods.add(f"{class_fqn}#{m_name}")
                if m_name == class_stem:
                    changed_methods.add(f"{class_fqn}#<init>")

    return changed_classes, changed_methods


def compute_pr_test_diff(
        base_model: Optional[ProjectAnalysis],
        head_model: Optional[ProjectAnalysis],
        changed_prod_classes: Set[str],
        changed_prod_methods: Set[str],
        changed_test_files: List[str],
) -> Dict[str, Any]:
    """
    Compares Base and Head ProjectAnalysis models:
    - Added tests (Head \\ Base)
    - Deleted tests (Base \\ Head)
    - Modified tests (Base ∩ Head with attribute, call, or structural AST changes)
    - Computes metrics deltas (complexity, assertions, mocks, ncloc) including fixture deltas
    - Tags each test method with co-evolution type (METHOD, CLASS, COLLABORATOR, or TEST_FOCUSED).
    - Evaluates data quality / unsupported format flags for manual double-checking.
    """
    base_methods: Dict[Tuple[str, str], Any] = {}
    base_classes = set()
    base_class_map: Dict[str, Any] = {}
    base_tc_list = getattr(base_model, "test_class_analyses", None) or (
        base_model.get("test_class_analyses") if isinstance(base_model, dict) else None)
    if base_model and base_tc_list:
        for cls in base_tc_list:
            q_cls = getattr(cls, "qualified_class_name", "") or (
                cls.get("qualified_class_name", "") if isinstance(cls, dict) else "")
            base_classes.add(q_cls)
            base_class_map[q_cls] = cls
            methods = getattr(cls, "test_method_analyses", []) or (
                cls.get("test_method_analyses", []) if isinstance(cls, dict) else [])
            for m in methods:
                sig = getattr(m, "method_signature", "") or (
                    m.get("method_signature", "") if isinstance(m, dict) else "")
                base_methods[(q_cls, sig)] = m

    head_methods: Dict[Tuple[str, str], Any] = {}
    head_classes = set()
    head_class_map: Dict[str, Any] = {}
    head_tc_list = getattr(head_model, "test_class_analyses", None) or (
        head_model.get("test_class_analyses") if isinstance(head_model, dict) else None)
    if head_model and head_tc_list:
        for cls in head_tc_list:
            q_cls = getattr(cls, "qualified_class_name", "") or (
                cls.get("qualified_class_name", "") if isinstance(cls, dict) else "")
            head_classes.add(q_cls)
            head_class_map[q_cls] = cls
            methods = getattr(cls, "test_method_analyses", []) or (
                cls.get("test_method_analyses", []) if isinstance(cls, dict) else [])
            for m in methods:
                sig = getattr(m, "method_signature", "") or (
                    m.get("method_signature", "") if isinstance(m, dict) else "")
                head_methods[(q_cls, sig)] = m

    added_methods = []
    modified_methods = []
    deleted_methods = []

    co_evolving_count = 0
    test_focused_count = 0

    total_assertions_delta = 0
    total_complexity_delta = 0
    total_mocks_delta = 0
    total_ncloc_delta = 0

    def classify_method_evolution(method_analysis, test_class_analysis=None, m_base=None) -> Tuple[str, Optional[str]]:
        class_focals = _get_class_focal_classes(test_class_analysis) if test_class_analysis else set()

        # 1. Direct Target Method Match
        f_list = getattr(method_analysis, "focal_classes", []) or (
            method_analysis.get("focal_classes", []) if isinstance(method_analysis, dict) else [])
        for fc in f_list:
            f_cls = _clean_fqn(getattr(fc, "focal_class", "") if hasattr(fc, "focal_class") else (
                fc.get("focal_class", "") if isinstance(fc, dict) else ""))
            f_names = getattr(fc, "focal_method_names", []) if hasattr(fc, "focal_method_names") else (
                fc.get("focal_method_names", []) if isinstance(fc, dict) else [])
            for fm in f_names:
                full_sig = f"{f_cls}#{fm}"
                if full_sig in changed_prod_methods:
                    return "CO_EVOLVING_METHOD", full_sig

        # Direct constructor calls matching changed_prod_methods
        c_calls = getattr(method_analysis, "constructor_call_details", []) or (
            method_analysis.get("constructor_call_details", []) if isinstance(method_analysis, dict) else [])
        for c_call in c_calls:
            rec = _clean_fqn(getattr(c_call, "receiver_type", "") if hasattr(c_call, "receiver_type") else (
                c_call.get("receiver_type", "") if isinstance(c_call, dict) else ""))
            if rec and f"{rec}#<init>" in changed_prod_methods:
                return "CO_EVOLVING_METHOD", f"{rec}#<init>"

        # Direct application calls matching changed_prod_methods
        app_calls = getattr(method_analysis, "application_call_details", []) or (
            method_analysis.get("application_call_details", []) if isinstance(method_analysis, dict) else [])
        for app_call in app_calls:
            rec = _clean_fqn(getattr(app_call, "receiver_type", "") if hasattr(app_call, "receiver_type") else (
                app_call.get("receiver_type", "") if isinstance(app_call, dict) else ""))
            m_name = getattr(app_call, "method_name", "") if hasattr(app_call, "method_name") else (
                app_call.get("method_name", "") if isinstance(app_call, dict) else "")
            if rec and m_name and f"{rec}#{m_name}" in changed_prod_methods:
                return "CO_EVOLVING_METHOD", f"{rec}#{m_name}"

        # 2. Focal Class match (method-declared focal classes)
        for fc in f_list:
            f_cls = _clean_fqn(getattr(fc, "focal_class", "") if hasattr(fc, "focal_class") else (
                fc.get("focal_class", "") if isinstance(fc, dict) else ""))
            if _matches_prod_fqn(f_cls, changed_prod_classes):
                return "CO_EVOLVING_CLASS", f_cls

        # 3. Test Class level focal class match (unit test for the changed production class)
        for cf in class_focals:
            if _matches_prod_fqn(cf, changed_prod_classes):
                return "CO_EVOLVING_CLASS", cf

        # 4. Collaborator Call Match:
        # A test method calling a changed collaborator is CO_EVOLVING_COLLABORATOR if:
        # a) It calls a modified method/constructor of that collaborator (handled in Step 1), OR
        # b) Its interaction (calls/arguments) with that collaborator was modified between Base and Head
        if m_base is not None:
            h_c_sig = {(c[1], c[0], c[2]) for c in _callable_list_signature(c_calls) if
                       _matches_prod_fqn(c[1], changed_prod_classes)}
            b_c_sig = {(c[1], c[0], c[2]) for c in _callable_list_signature(
                getattr(m_base, "constructor_call_details", []) or (
                    m_base.get("constructor_call_details", []) if isinstance(m_base, dict) else [])) if
                       _matches_prod_fqn(c[1], changed_prod_classes)}
            if h_c_sig != b_c_sig and h_c_sig:
                collab_cls = next(iter(h_c_sig))[0]
                return "CO_EVOLVING_COLLABORATOR", collab_cls

            h_a_sig = {(c[1], c[0], c[2]) for c in _callable_list_signature(app_calls) if
                       _matches_prod_fqn(c[1], changed_prod_classes)}
            b_a_sig = {(c[1], c[0], c[2]) for c in _callable_list_signature(
                getattr(m_base, "application_call_details", []) or (
                    m_base.get("application_call_details", []) if isinstance(m_base, dict) else [])) if
                       _matches_prod_fqn(c[1], changed_prod_classes)}
            if h_a_sig != b_a_sig and h_a_sig:
                collab_cls = next(iter(h_a_sig))[0]
                return "CO_EVOLVING_COLLABORATOR", collab_cls
        else:
            # Added test method: if it invokes a changed collaborator
            for c_call in c_calls:
                rec = _clean_fqn(getattr(c_call, "receiver_type", "") if hasattr(c_call, "receiver_type") else (
                    c_call.get("receiver_type", "") if isinstance(c_call, dict) else ""))
                if _matches_prod_fqn(rec, changed_prod_classes):
                    return "CO_EVOLVING_COLLABORATOR", rec
            for app_call in app_calls:
                rec = _clean_fqn(getattr(app_call, "receiver_type", "") if hasattr(app_call, "receiver_type") else (
                    app_call.get("receiver_type", "") if isinstance(app_call, dict) else ""))
                if _matches_prod_fqn(rec, changed_prod_classes):
                    return "CO_EVOLVING_COLLABORATOR", rec

        return "TEST_FOCUSED", None

    # Added Tests
    for key, m_head in head_methods.items():
        if key not in base_methods:
            cls_obj = head_class_map.get(key[0])
            evo_type, matched = classify_method_evolution(m_head, cls_obj)
            if evo_type != "TEST_FOCUSED":
                co_evolving_count += 1
            else:
                test_focused_count += 1

            assert_count = _count_assertions(m_head)
            cc = (getattr(m_head, "cyclomatic_complexity", 0) or (
                m_head.get("cyclomatic_complexity", 0) if isinstance(m_head, dict) else 0)) or 0
            ncloc = (getattr(m_head, "ncloc", 0) or (m_head.get("ncloc", 0) if isinstance(m_head, dict) else 0)) or 0
            mocks = (getattr(m_head, "number_of_mocks_created", 0) or (
                m_head.get("number_of_mocks_created", 0) if isinstance(m_head, dict) else 0)) or 0
            is_mocking = getattr(m_head, "is_mocking_used", False) or (
                m_head.get("is_mocking_used", False) if isinstance(m_head, dict) else False)
            t_type = getattr(m_head, "test_type", "unknown") if hasattr(m_head, "test_type") else (
                m_head.get("test_type", "unknown") if isinstance(m_head, dict) else "unknown")
            t_type_val = t_type.value if hasattr(t_type, "value") else str(t_type)

            total_assertions_delta += assert_count
            total_complexity_delta += cc
            total_ncloc_delta += ncloc
            total_mocks_delta += mocks

            added_methods.append(
                {
                    "class": key[0],
                    "signature": key[1],
                    "change_type": "ADDED",
                    "evolution_type": evo_type,
                    "matched_prod_entity": matched,
                    "test_type": t_type_val,
                    "ncloc": ncloc,
                    "cyclomatic_complexity": cc,
                    "assertions_count": assert_count,
                    "mocking_used": is_mocking,
                    "mocks_created": mocks,
                    "has_verifications": _has_assertions_or_verifications(m_head),
                }
            )

    # Deleted Tests
    for key, m_base in base_methods.items():
        if key not in head_methods:
            assert_count = _count_assertions(m_base)
            cc = (getattr(m_base, "cyclomatic_complexity", 0) or (
                m_base.get("cyclomatic_complexity", 0) if isinstance(m_base, dict) else 0)) or 0
            ncloc = (getattr(m_base, "ncloc", 0) or (m_base.get("ncloc", 0) if isinstance(m_base, dict) else 0)) or 0
            mocks = (getattr(m_base, "number_of_mocks_created", 0) or (
                m_base.get("number_of_mocks_created", 0) if isinstance(m_base, dict) else 0)) or 0

            total_assertions_delta -= assert_count
            total_complexity_delta -= cc
            total_ncloc_delta -= ncloc
            total_mocks_delta -= mocks

            deleted_methods.append(
                {
                    "class": key[0],
                    "signature": key[1],
                    "change_type": "DELETED",
                    "ncloc": ncloc,
                    "cyclomatic_complexity": cc,
                    "assertions_count": assert_count,
                    "mocks_created": mocks,
                }
            )

    # Modified Tests
    for key, m_head in head_methods.items():
        if key in base_methods:
            m_base = base_methods[key]

            h_ncloc = (getattr(m_head, "ncloc", 0) or (m_head.get("ncloc", 0) if isinstance(m_head, dict) else 0)) or 0
            b_ncloc = (getattr(m_base, "ncloc", 0) or (m_base.get("ncloc", 0) if isinstance(m_base, dict) else 0)) or 0
            ncloc_diff = h_ncloc - b_ncloc

            h_mocks = (getattr(m_head, "number_of_mocks_created", 0) or (
                m_head.get("number_of_mocks_created", 0) if isinstance(m_head, dict) else 0)) or 0
            b_mocks = (getattr(m_base, "number_of_mocks_created", 0) or (
                m_base.get("number_of_mocks_created", 0) if isinstance(m_base, dict) else 0)) or 0
            mock_diff = h_mocks - b_mocks

            h_cc = (getattr(m_head, "cyclomatic_complexity", 0) or (
                m_head.get("cyclomatic_complexity", 0) if isinstance(m_head, dict) else 0)) or 0
            b_cc = (getattr(m_base, "cyclomatic_complexity", 0) or (
                m_base.get("cyclomatic_complexity", 0) if isinstance(m_base, dict) else 0)) or 0
            cc_diff = h_cc - b_cc

            assert_diff = _count_assertions(m_head) - _count_assertions(m_base)

            h_mock_used = getattr(m_head, "is_mocking_used", False) or (
                m_head.get("is_mocking_used", False) if isinstance(m_head, dict) else False)
            b_mock_used = getattr(m_base, "is_mocking_used", False) or (
                m_base.get("is_mocking_used", False) if isinstance(m_base, dict) else False)
            mock_used_diff = h_mock_used != b_mock_used

            h_objs = (getattr(m_head, "number_of_objects_created", 0) or (
                m_head.get("number_of_objects_created", 0) if isinstance(m_head, dict) else 0)) or 0
            b_objs = (getattr(m_base, "number_of_objects_created", 0) or (
                m_base.get("number_of_objects_created", 0) if isinstance(m_base, dict) else 0)) or 0
            objs_diff = h_objs != b_objs

            # Check structural/callable call differences
            h_constructors = getattr(m_head, "constructor_call_details", []) or (
                m_head.get("constructor_call_details", []) if isinstance(m_head, dict) else [])
            b_constructors = getattr(m_base, "constructor_call_details", []) or (
                m_base.get("constructor_call_details", []) if isinstance(m_base, dict) else [])
            constructor_diff = _callable_list_signature(h_constructors) != _callable_list_signature(b_constructors)

            h_apps = getattr(m_head, "application_call_details", []) or (
                m_head.get("application_call_details", []) if isinstance(m_head, dict) else [])
            b_apps = getattr(m_base, "application_call_details", []) or (
                m_base.get("application_call_details", []) if isinstance(m_base, dict) else [])
            app_diff = _callable_list_signature(h_apps) != _callable_list_signature(b_apps)

            if ncloc_diff != 0 or mock_diff != 0 or cc_diff != 0 or assert_diff != 0 or mock_used_diff or objs_diff or constructor_diff or app_diff:
                cls_obj = head_class_map.get(key[0])
                evo_type, matched = classify_method_evolution(m_head, cls_obj, m_base)
                if evo_type != "TEST_FOCUSED":
                    co_evolving_count += 1
                else:
                    test_focused_count += 1

                modified_methods.append(
                    {
                        "class": key[0],
                        "signature": key[1],
                        "change_type": "MODIFIED",
                        "evolution_type": evo_type,
                        "matched_prod_entity": matched,
                        "ncloc_delta": ncloc_diff,
                        "complexity_delta": cc_diff,
                        "assertions_delta": assert_diff,
                        "mocks_delta": mock_diff,
                        "has_verifications": _has_assertions_or_verifications(m_head),
                    }
                )
                total_ncloc_delta += ncloc_diff
                total_complexity_delta += cc_diff
                total_assertions_delta += assert_diff
                total_mocks_delta += mock_diff

    # Diff Fixtures (setup_analyses and teardown_analyses deltas)
    for q_cls, h_cls in head_class_map.items():
        if q_cls in base_class_map:
            b_cls = base_class_map[q_cls]
            h_setups = {(getattr(s, "method_signature", "") or (
                s.get("method_signature", "") if isinstance(s, dict) else "")): s for s in (
                                    getattr(h_cls, "setup_analyses", []) or (
                                h_cls.get("setup_analyses", []) if isinstance(h_cls, dict) else []) or [])}
            b_setups = {(getattr(s, "method_signature", "") or (
                s.get("method_signature", "") if isinstance(s, dict) else "")): s for s in (
                                    getattr(b_cls, "setup_analyses", []) or (
                                b_cls.get("setup_analyses", []) if isinstance(b_cls, dict) else []) or [])}
            for s_sig, h_s in h_setups.items():
                if s_sig in b_setups:
                    b_s = b_setups[s_sig]
                    sn_diff = ((getattr(h_s, "ncloc", 0) or (
                        h_s.get("ncloc", 0) if isinstance(h_s, dict) else 0)) or 0) - ((getattr(b_s, "ncloc", 0) or (
                        b_s.get("ncloc", 0) if isinstance(b_s, dict) else 0)) or 0)
                    scc_diff = ((getattr(h_s, "cyclomatic_complexity", 0) or (
                        h_s.get("cyclomatic_complexity", 0) if isinstance(h_s, dict) else 0)) or 0) - ((getattr(b_s,
                                                                                                                "cyclomatic_complexity",
                                                                                                                0) or (
                                                                                                            b_s.get(
                                                                                                                "cyclomatic_complexity",
                                                                                                                0) if isinstance(
                                                                                                                b_s,
                                                                                                                dict) else 0)) or 0)
                    sm_diff = ((getattr(h_s, "number_of_mocks_created", 0) or (
                        h_s.get("number_of_mocks_created", 0) if isinstance(h_s, dict) else 0)) or 0) - ((getattr(b_s,
                                                                                                                  "number_of_mocks_created",
                                                                                                                  0) or (
                                                                                                              b_s.get(
                                                                                                                  "number_of_mocks_created",
                                                                                                                  0) if isinstance(
                                                                                                                  b_s,
                                                                                                                  dict) else 0)) or 0)
                    total_ncloc_delta += sn_diff
                    total_complexity_delta += scc_diff
                    total_mocks_delta += sm_diff

    total_changed = len(added_methods) + len(modified_methods)
    ratio = (co_evolving_count / total_changed) if total_changed > 0 else 0.0

    if not changed_prod_classes:
        evo_summary = "NO_PROD_CHANGED"
    elif ratio == 1.0:
        evo_summary = "CO_EVOLUTION_PURE"
    elif ratio == 0.0:
        evo_summary = "TEST_FOCUSED_PURE"
    else:
        evo_summary = "MIXED"

    # ==============================================================================
    # Quality Flags & Incompleteness Detection (for manual double-checking)
    # ==============================================================================
    review_reasons = []
    total_detected_classes = len(head_classes.union(base_classes))
    total_detected_methods = len(head_methods) + len(base_methods)

    # 1. Test files modified in PR, but zero tests extracted by Hamster
    if len(changed_test_files) > 0 and total_detected_methods == 0:
        review_reasons.append("TEST_FILES_CHANGED_BUT_ZERO_TESTS_EXTRACTED")

    # 2. Number of analyzed test classes is less than number of changed test files
    if len(changed_test_files) > total_detected_classes and total_detected_classes > 0:
        review_reasons.append("PARTIAL_TEST_CLASS_EXTRACTION")

    # 3. Tests detected, but all have 0 assertions and 0 mock verifications
    if total_changed > 0:
        all_changed_methods = added_methods + modified_methods
        has_any_assertions = any(
            (
                    m.get("assertions_count", 0) > 0
                    or m.get("assertions_delta", 0) != 0
                    or m.get("has_verifications", False)
            )
            for m in all_changed_methods
        )
        if not has_any_assertions:
            review_reasons.append("ZERO_ASSERTIONS_CUSTOM_DSL")

    needs_manual_review = len(review_reasons) > 0

    return {
        "co_evolution_type": evo_summary,
        "co_evolution_ratio": ratio,
        "added_tests_count": len(added_methods),
        "modified_tests_count": len(modified_methods),
        "deleted_tests_count": len(deleted_methods),
        "total_assertions_delta": total_assertions_delta,
        "total_complexity_delta": total_complexity_delta,
        "total_mocks_delta": total_mocks_delta,
        "total_ncloc_delta": total_ncloc_delta,
        "quality_flags": {
            "needs_manual_review": needs_manual_review,
            "review_reasons": review_reasons,
            "changed_test_files_count": len(changed_test_files),
            "detected_test_classes_count": total_detected_classes,
        },
        "added_methods": added_methods,
        "modified_methods": modified_methods,
        "deleted_methods": deleted_methods,
    }


# ==============================================================================
# JSONB Size Management (255 MB Limit Offloader)
# ==============================================================================

MAX_JSONB_BYTES = 255 * 1024 * 1024  # 255 MB Postgres JSONB limit


def serialize_or_offload_json(
        payload: Any,
        pr_id: str,
        payload_name: str,
        repo_name: str,
        storage_base_dir: Path,
) -> Tuple[Optional[str], bool]:
    """
    Serializes a Python object to JSON string for Postgres JSONB.
    If the serialized string meets or exceeds MAX_JSONB_BYTES (255 MB),
    compresses and saves the payload to disk as a .json.gz file,
    and returns a lightweight JSON pointer for PostgreSQL.

    Returns:
        Tuple[json_string, was_offloaded: bool]
    """
    if payload is None:
        return None, False

    try:
        raw_json_str = json.dumps(payload)
    except Exception as e:
        logger.warning("Failed to serialize %s for PR %s: %s", payload_name, pr_id, e)
        return json.dumps({"_error": str(e)}), True

    size_bytes = len(raw_json_str.encode("utf-8"))
    if size_bytes < MAX_JSONB_BYTES:
        return raw_json_str, False

    # Offload to disk even when compressed
    offload_dir = storage_base_dir / "large_payloads" / repo_name.replace("/", "_")
    offload_dir.mkdir(parents=True, exist_ok=True)
    offload_file = offload_dir / f"{pr_id}_{payload_name}.json.gz"

    with gzip.open(offload_file, "wt", encoding="utf-8") as f:
        f.write(raw_json_str)

    compressed_size = offload_file.stat().st_size
    pointer = {
        "_offloaded": True,
        "format": "gzip",
        "file_path": str(offload_file.resolve()),
        "original_size_bytes": size_bytes,
        "compressed_size_bytes": compressed_size,
    }
    logger.info(
        "[%s] PR %s %s payload (%.2f MB) reached 255 MB limit; saved to file %s (compressed: %.2f MB)",
        repo_name,
        pr_id,
        payload_name,
        size_bytes / (1024 * 1024),
        offload_file.name,
        compressed_size / (1024 * 1024),
    )
    return json.dumps(pointer), True


# ==============================================================================
# Single PR Processing Pipeline
# ==============================================================================

def process_single_pr(
        pr_row: Tuple,
        repos_dir: Path,
        output_json_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Processes a single PR:
    1. Fetches PR files from DB.
    2. Extracts dependency slices for Base and Head commits.
    3. Runs Hamster on both slices.
    4. Computes Diff, Co-evolution, and Quality check flags.
    5. Saves result to Postgres.
    """
    pr_id, pr_number, base_sha, head_sha, merge_sha, name_with_owner = pr_row
    conn = get_db_connection()

    try:
        repo_dir = repos_dir / name_with_owner.replace("/", "_")
        if not repo_dir.exists():
            # Clone repo if missing
            logger.info("[%s] Cloning repository to %s...", name_with_owner, repo_dir)
            repo_dir.parent.mkdir(parents=True, exist_ok=True)
            res = subprocess.run(
                ["git", "clone", f"https://github.com/{name_with_owner}.git", str(repo_dir)],
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                raise RuntimeError(f"Git clone failed: {res.stderr.strip()}")

        # Fetch changed files from pull_request_files
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT path, change_type, is_test
                FROM pull_request_files
                WHERE pr_id = %s
                  AND path LIKE '%%.java'
                """,
                (pr_id,),
            )
            files = cursor.fetchall()

        if not files:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO pull_request_analysis (pr_id, status, error_message, needs_manual_review,
                                                       review_reasons, test_files_count)
                    VALUES (%s, 'NO_TESTS', 'No Java files found in PR', FALSE, NULL, 0) ON CONFLICT (pr_id) DO
                    UPDATE SET
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message,
                        needs_manual_review = FALSE
                    """,
                    (pr_id,),
                )
                conn.commit()
            return {"pr_id": pr_id, "status": "NO_TESTS"}

        test_files = [f[0] for f in files if f[2] is True]
        prod_files = [f[0] for f in files if f[2] is False or f[2] is None]

        if not test_files:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO pull_request_analysis (pr_id, status, error_message, needs_manual_review,
                                                       review_reasons, test_files_count)
                    VALUES (%s, 'NO_TESTS', 'No test files changed in PR', FALSE, NULL, 0) ON CONFLICT (pr_id) DO
                    UPDATE SET
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message,
                        needs_manual_review = FALSE
                    """,
                    (pr_id,),
                )
                conn.commit()
            return {"pr_id": pr_id, "status": "NO_TESTS"}

        # Target commit resolution
        target_head_sha = head_sha or merge_sha
        target_base_sha = base_sha

        ensure_commit(repo_dir, target_base_sha, pr_number)
        ensure_commit(repo_dir, target_head_sha, pr_number)

        # Get all java files at commits
        all_java_head = get_all_java_files_at_commit(repo_dir, target_head_sha)
        all_java_base = get_all_java_files_at_commit(repo_dir, target_base_sha)

        # Extract dependency closures
        head_slice = extract_dependency_closure(repo_dir, target_head_sha, test_files, all_java_head)
        base_slice = extract_dependency_closure(repo_dir, target_base_sha, test_files, all_java_base)

        # Run Hamster on slices with explicit seed test files and prod files
        head_model = run_hamster_on_slice(head_slice, f"{name_with_owner}_pr_{pr_number}_head", test_files, prod_files)
        base_model = run_hamster_on_slice(base_slice, f"{name_with_owner}_pr_{pr_number}_base", test_files, prod_files)

        # Extract changed production classes & methods (FQN-based)
        changed_prod_classes, changed_prod_methods = extract_changed_prod_methods_and_classes(
            repo_dir=repo_dir,
            base_sha=target_base_sha,
            head_sha=target_head_sha,
            prod_files=prod_files,
        )

        # Compute Diff & Quality Flags
        diff_result = compute_pr_test_diff(
            base_model=base_model,
            head_model=head_model,
            changed_prod_classes=changed_prod_classes,
            changed_prod_methods=changed_prod_methods,
            changed_test_files=test_files,
        )

        quality_flags = diff_result["quality_flags"]
        needs_review = quality_flags["needs_manual_review"]
        reasons_str = ",".join(quality_flags["review_reasons"]) if quality_flags["review_reasons"] else None

        # Convert models to dict for JSONB persistence
        base_json = base_model.model_dump(mode="json") if base_model else None
        head_json = head_model.model_dump(mode="json") if head_model else None

        # Serialize or offload payloads meeting or exceeding 255 MB
        large_payload_dir = repos_dir.parent / "hamster_large_payloads"
        diff_str, diff_offloaded = serialize_or_offload_json(diff_result, pr_id, "diff", name_with_owner,
                                                             large_payload_dir)
        base_str, base_offloaded = serialize_or_offload_json(base_json, pr_id, "base_model", name_with_owner,
                                                             large_payload_dir)
        head_str, head_offloaded = serialize_or_offload_json(head_json, pr_id, "head_model", name_with_owner,
                                                             large_payload_dir)

        if diff_offloaded or base_offloaded or head_offloaded:
            needs_review = True
            reasons_list = quality_flags.get("review_reasons") or []
            if "LARGE_PAYLOAD_SAVED_TO_FILE" not in reasons_list:
                reasons_list.append("LARGE_PAYLOAD_SAVED_TO_FILE")
            reasons_str = ",".join(reasons_list)

        def execute_db_insert(d_val, b_val, h_val, r_str, n_rev):
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO pull_request_analysis (pr_id, status, error_message, needs_manual_review,
                                                       review_reasons, test_files_count,
                                                       co_evolution_type, co_evolution_ratio,
                                                       added_tests_count, modified_tests_count, deleted_tests_count,
                                                       total_assertions_delta, total_complexity_delta,
                                                       total_mocks_delta, total_ncloc_delta,
                                                       hamster_diff_json, base_model_json, head_model_json)
                    VALUES (%s, 'SUCCESS', NULL, %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s) ON CONFLICT (pr_id) DO
                    UPDATE SET
                        status = EXCLUDED.status,
                        error_message = NULL,
                        needs_manual_review = EXCLUDED.needs_manual_review,
                        review_reasons = EXCLUDED.review_reasons,
                        test_files_count = EXCLUDED.test_files_count,
                        co_evolution_type = EXCLUDED.co_evolution_type,
                        co_evolution_ratio = EXCLUDED.co_evolution_ratio,
                        added_tests_count = EXCLUDED.added_tests_count,
                        modified_tests_count = EXCLUDED.modified_tests_count,
                        deleted_tests_count = EXCLUDED.deleted_tests_count,
                        total_assertions_delta = EXCLUDED.total_assertions_delta,
                        total_complexity_delta = EXCLUDED.total_complexity_delta,
                        total_mocks_delta = EXCLUDED.total_mocks_delta,
                        total_ncloc_delta = EXCLUDED.total_ncloc_delta,
                        hamster_diff_json = EXCLUDED.hamster_diff_json,
                        base_model_json = EXCLUDED.base_model_json,
                        head_model_json = EXCLUDED.head_model_json,
                        processed_at = CURRENT_TIMESTAMP
                    """,
                    (
                        pr_id,
                        n_rev,
                        r_str,
                        len(test_files),
                        diff_result["co_evolution_type"],
                        diff_result["co_evolution_ratio"],
                        diff_result["added_tests_count"],
                        diff_result["modified_tests_count"],
                        diff_result["deleted_tests_count"],
                        diff_result["total_assertions_delta"],
                        diff_result["total_complexity_delta"],
                        diff_result["total_mocks_delta"],
                        diff_result["total_ncloc_delta"],
                        d_val,
                        b_val,
                        h_val,
                    ),
                )
                conn.commit()

        try:
            execute_db_insert(diff_str, base_str, head_str, reasons_str, needs_review)
        except Exception as db_err:
            # Fallback if PostgreSQL still raises size or JSONB limit error:
            # force save all models to compressed files and store lightweight pointers
            logger.warning(
                "[%s] PR #%d DB insert failed with size/JSONB error (%s). Forcing compression to file...",
                name_with_owner,
                pr_number,
                db_err,
            )
            conn.rollback()

            def force_save_to_file(obj, name):
                if obj is None:
                    return None
                odir = large_payload_dir / name_with_owner.replace("/", "_")
                odir.mkdir(parents=True, exist_ok=True)
                ofile = odir / f"{pr_id}_{name}_force.json.gz"
                raw = json.dumps(obj)
                with gzip.open(ofile, "wt", encoding="utf-8") as gf:
                    gf.write(raw)
                return json.dumps({
                    "_offloaded": True,
                    "format": "gzip",
                    "file_path": str(ofile.resolve()),
                    "original_size_bytes": len(raw.encode("utf-8")),
                    "compressed_size_bytes": ofile.stat().st_size,
                })

            fallback_diff_ptr = force_save_to_file(diff_result, "diff")
            fallback_base_ptr = force_save_to_file(base_json, "base_model")
            fallback_head_ptr = force_save_to_file(head_json, "head_model")

            fallback_reasons = (reasons_str.split(",") if reasons_str else []) + ["LARGE_PAYLOAD_SAVED_TO_FILE"]
            fallback_reasons_str = ",".join(list(dict.fromkeys(fallback_reasons)))

            execute_db_insert(fallback_diff_ptr, fallback_base_ptr, fallback_head_ptr, fallback_reasons_str, True)

        # Optional disk JSON backup
        if output_json_dir:
            repo_out = output_json_dir / name_with_owner.replace("/", "_")
            repo_out.mkdir(parents=True, exist_ok=True)
            with open(repo_out / f"pr_{pr_number}.json", "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "pr_id": pr_id,
                        "pr_number": pr_number,
                        "repo": name_with_owner,
                        "diff": diff_result,
                    },
                    f,
                    indent=2,
                )

        review_tag = f" [FLAGGED: {reasons_str}]" if needs_review else ""
        logger.info(
            "[%s] PR #%d: SUCCESS (added=%d, mod=%d, del=%d, evo=%s, ΔCC=%d)%s",
            name_with_owner,
            pr_number,
            diff_result["added_tests_count"],
            diff_result["modified_tests_count"],
            diff_result["deleted_tests_count"],
            diff_result["co_evolution_type"],
            diff_result["total_complexity_delta"],
            review_tag,
        )
        return {"pr_id": pr_id, "status": "SUCCESS", "needs_manual_review": needs_review}

    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error("[%s] PR #%d FAILED: %s", name_with_owner, pr_number, e)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO pull_request_analysis (pr_id, status, error_message, needs_manual_review,
                                                       review_reasons, test_files_count)
                    VALUES (%s, 'FAILED', %s, TRUE, 'EXCEPTION_IN_PIPELINE', 0) ON CONFLICT (pr_id) DO
                    UPDATE SET
                        status = 'FAILED',
                        error_message = EXCLUDED.error_message,
                        needs_manual_review = TRUE,
                        review_reasons = 'EXCEPTION_IN_PIPELINE'
                    """,
                    (pr_id, str(e)),
                )
                conn.commit()
        except Exception:
            pass
        return {"pr_id": pr_id, "status": "FAILED", "error": str(e)}

    finally:
        conn.close()


# ==============================================================================
# Main Orchestrator
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Analyze PR test changes using Hamster and Postgres.")
    parser.add_argument("--workers", type=int, default=12, help="Number of concurrent worker threads (default: 12)")
    parser.add_argument("--repo", type=str, default=None, help="Filter for specific repository (owner/name)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of PRs to process")
    parser.add_argument("--reprocess-failed", action="store_true", help="Reprocess previously failed PRs")
    parser.add_argument("--save-json", action="store_true", help="Also save JSON results to data/hamster_results/")
    parser.add_argument("--log-file", type=str, default="logs/hamster_analysis.log",
                        help="Path to save log file (default: logs/hamster_analysis.log)")
    parser.add_argument("--executor", choices=["process", "thread"], default="process",
                        help="Concurrency backend: 'process' (true multi-core multiprocessing, recommended) or 'thread'")
    args = parser.parse_args()

    # Initialize Logging
    setup_logging(log_file=Path(args.log_file))

    repos_dir = BASE_DIR / "data" / "repos"
    repos_dir.mkdir(parents=True, exist_ok=True)

    json_dir = (BASE_DIR / "data" / "hamster_results") if args.save_json else None
    if json_dir:
        json_dir.mkdir(parents=True, exist_ok=True)

    conn = get_db_connection()
    init_db(conn)

    # Build query for unanalyzed PRs
    status_filter = "('SUCCESS', 'NO_TESTS')" if not args.reprocess_failed else "('SUCCESS', 'NO_TESTS', 'FAILED')"
    query = f"""
        SELECT p.id, p.number, p.base_commit_oid, p.head_commit_oid, p.merge_commit_oid, r.name_with_owner
        FROM pull_requests p
        JOIN repositories r ON p.base_repository_id = r.id
        WHERE p.has_test_files = TRUE
          AND p.id NOT IN (
              SELECT pr_id FROM pull_request_analysis WHERE status IN {status_filter}
          )
    """
    params = []
    if args.repo:
        query += " AND r.name_with_owner = %s"
        params.append(args.repo)

    if args.limit:
        query += f" LIMIT {args.limit}"

    with conn.cursor() as cursor:
        cursor.execute(query, params)
        pending_prs = cursor.fetchall()

    conn.close()

    total_prs = len(pending_prs)
    logger.info("Found %d pending PRs to analyze using %s executor with %d workers.", total_prs, args.executor,
                args.workers)

    if total_prs == 0:
        logger.info("No PRs to process. Exiting.")
        return

    # Upfront Pre-cloning: Ensure all required repositories are cloned once before workers start
    distinct_repos = sorted(list({pr_row[5] for pr_row in pending_prs}))
    logger.info("Checking %d distinct repositories for pre-cloning...", len(distinct_repos))
    for name_with_owner in distinct_repos:
        repo_dir = repos_dir / name_with_owner.replace("/", "_")
        if not repo_dir.exists():
            logger.info("[%s] Pre-cloning repository to %s...", name_with_owner, repo_dir)
            repo_dir.parent.mkdir(parents=True, exist_ok=True)
            res = subprocess.run(
                ["git", "clone", f"https://github.com/{name_with_owner}.git", str(repo_dir)],
                capture_output=True,
                text=True,
            )
            if res.returncode != 0:
                logger.error("[%s] Git clone failed: %s", name_with_owner, res.stderr.strip())
            else:
                logger.info("[%s] Pre-clone complete.", name_with_owner)

    success_count = 0
    flagged_count = 0
    failed_count = 0

    ExecutorClass = (
        concurrent.futures.ProcessPoolExecutor
        if args.executor == "process"
        else concurrent.futures.ThreadPoolExecutor
    )

    remaining_prs = list(pending_prs)
    max_in_flight = max(1, args.workers * 2)

    while remaining_prs:
        executor = ExecutorClass(max_workers=args.workers)
        active_futures: Dict[concurrent.futures.Future, Tuple] = {}

        try:
            # Seed initial batch of in-flight tasks
            while remaining_prs and len(active_futures) < max_in_flight:
                pr_row = remaining_prs.pop(0)
                fut = executor.submit(process_single_pr, pr_row, repos_dir, json_dir)
                active_futures[fut] = pr_row

            while active_futures:
                # Wait for at least one future to complete
                done, _ = concurrent.futures.wait(
                    list(active_futures.keys()),
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )

                for fut in done:
                    pr_row = active_futures.pop(fut)
                    try:
                        res = fut.result()
                        if res.get("status") == "SUCCESS":
                            success_count += 1
                            if res.get("needs_manual_review"):
                                flagged_count += 1
                        else:
                            failed_count += 1
                    except BrokenProcessPool as bpe:
                        active_futures[fut] = pr_row
                        raise bpe
                    except Exception as e:
                        failed_count += 1
                        logger.error("[%s] PR #%d worker error: %s", pr_row[5], pr_row[1], e)

                    # Replenish with next PR from queue
                    if remaining_prs and len(active_futures) < max_in_flight:
                        next_pr = remaining_prs.pop(0)
                        new_fut = executor.submit(process_single_pr, next_pr, repos_dir, json_dir)
                        active_futures[new_fut] = next_pr

        except BrokenProcessPool:
            logger.error(
                "Process pool was terminated abruptly (likely Out-Of-Memory or C-extension crash). "
                "Recovering and re-instantiating pool..."
            )
            # Mark in-flight PRs as failed in DB so they don't block subsequent runs
            crash_conn = get_db_connection()
            try:
                with crash_conn.cursor() as cursor:
                    for crashed_pr in active_futures.values():
                        c_pr_id = crashed_pr[0]
                        c_pr_num = crashed_pr[1]
                        c_repo = crashed_pr[5]
                        logger.warning("[%s] PR #%d was in-flight during abrupt crash; marking as FAILED.", c_repo,
                                       c_pr_num)
                        cursor.execute(
                            """
                            INSERT INTO pull_request_analysis (pr_id, status, error_message, needs_manual_review,
                                                               review_reasons, test_files_count)
                            VALUES (%s, 'FAILED', 'Process pool terminated abruptly (OOM or native crash)', TRUE,
                                    'PROCESS_TERMINATED_ABRUPTLY_CRASH_OR_OOM', 0) ON CONFLICT (pr_id) DO
                            UPDATE SET
                                status = 'FAILED',
                                error_message = EXCLUDED.error_message,
                                needs_manual_review = TRUE,
                                review_reasons = 'PROCESS_TERMINATED_ABRUPTLY_CRASH_OR_OOM'
                            """,
                            (c_pr_id,),
                        )
                        failed_count += 1
                crash_conn.commit()
            except Exception as dbe:
                logger.error("Failed to mark crashed PRs in DB: %s", dbe)
            finally:
                crash_conn.close()

            # Clean shutdown of broken executor
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            active_futures.clear()
            logger.info("Resuming remaining %d PRs with a fresh process pool...", len(remaining_prs))
        finally:
            try:
                executor.shutdown(wait=False)
            except Exception:
                pass

    logger.info(
        "Pipeline completed. Total: %d, Success: %d (Flagged for Review: %d), Failed: %d",
        total_prs,
        success_count,
        flagged_count,
        failed_count,
    )


if __name__ == "__main__":
    main()