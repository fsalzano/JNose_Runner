import psycopg2
import os

def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "10.64.160.163"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        database=os.environ.get("POSTGRES_DATABASE", "testing-agentic-prs"),
        user=os.environ.get("POSTGRES_USER", "user"),
        password=os.environ.get("POSTGRES_PASSWORD", "Un!m0l1s3"),
    )

def filter_prs_with_tests():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Select PRs with test files
        query = "SELECT id, number, title, state FROM pull_requests WHERE has_test_files = TRUE;"
        cur.execute(query)
        
        rows = cur.fetchall()
        
        print(f"Found {len(rows)} Pull Requests with test files.")
        print("-" * 50)
        for row in rows[:10]:  # Print first 10 results for brevity
            print(f"ID: {row[0]} | Number: {row[1]} | State: {row[3]} | Title: {row[2]}")
        
        if len(rows) > 10:
            print("...")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Database access error: {e}")

if __name__ == "__main__":
    filter_prs_with_tests()
