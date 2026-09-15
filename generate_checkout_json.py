import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json

def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "10.64.160.163"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        database=os.environ.get("POSTGRES_DATABASE", "testing-agentic-prs"),
        user=os.environ.get("POSTGRES_USER", "user"),
        password=os.environ.get("POSTGRES_PASSWORD", "Un!m0l1s3"),
    )

def generate_json():
    try:
        conn = get_db_connection()
        # Use RealDictCursor to get results as dictionaries
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query to fetch PRs with tests and repository information
        query = """
            SELECT 
                pr.id, 
                pr.number, 
                pr.base_commit_oid, 
                pr.merge_commit_oid, 
                pr.head_commit_oid,
                r.name as repo_name,
                r.name_with_owner as repo_full_name
            FROM pull_requests pr
            JOIN repositories r ON pr.base_repository_id = r.id
            WHERE pr.has_test_files = TRUE;
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        checkout_data = []
        for row in rows:
            # Commit selection logic:
            # use merge_commit_oid if available, otherwise head_commit_oid
            target_commit = row['merge_commit_oid'] if row['merge_commit_oid'] else row['head_commit_oid']
            
            entry = {
                "pr_id": row['id'],
                "pr_number": row['number'],
                "repo_name": row['repo_name'],
                "repo_full_name": row['repo_full_name'],
                "base_commit": row['base_commit_oid'],
                "target_commit": target_commit,
                "analysis_commit_type": "merge" if row['merge_commit_oid'] else "head"
            }
            checkout_data.append(entry)
            
        # Save results in JSON format
        output_file = "checkout_info.json"
        with open(output_file, "w") as f:
            json.dump(checkout_data, f, indent=4)
            
        print(f"Generated {output_file} with {len(checkout_data)} records.")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    generate_json()
