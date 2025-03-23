import psycopg2
import os
import sys

def validate_data():
    try:

        source_conn = psycopg2.connect(os.environ['SOURCE_DB_URL'])
        dest_conn = psycopg2.connect(os.environ['DEST_DB_URL'])

        with source_conn.cursor() as src_cur, dest_conn.cursor() as dest_cur:
            src_cur.execute("SELECT COUNT(*) FROM recommendation")
            dest_cur.execute("SELECT COUNT(*) FROM recommendation")
            src_count = src_cur.fetchone()[0]
            dest_count = dest_cur.fetchone()[0]

            if src_count != dest_count:
                print(f"❌ Row count mismatch! Source: {src_count}, Dest: {dest_count}")
                sys.exit(1)
            else:
                print(f"✅ Row counts match: {src_count} records")

        with source_conn.cursor() as src_cur, dest_conn.cursor() as dest_cur:
            src_cur.execute("""
                SELECT user_id, md5(json_agg(recommendation ORDER BY id)::text)
                FROM recommendation
                GROUP BY user_id
                ORDER BY user_id
            """)
            dest_cur.execute("""
                SELECT user_id, md5(json_agg(recommendation ORDER BY id)::text)
                FROM recommendation
                GROUP BY user_id
                ORDER BY user_id
            """)

            src_hashes = dict(src_cur.fetchall())
            dest_hashes = dict(dest_cur.fetchall())

            for user_id in src_hashes:
                if user_id not in dest_hashes:
                    print(f"❌ User {user_id} missing in destination!")
                    sys.exit(1)
                elif src_hashes[user_id] != dest_hashes[user_id]:
                    print(f"❌ Hash mismatch for user {user_id}")
                    sys.exit(1)

            print("✅ All user hashes match!")

    except Exception as e:
        print(f"🚨 Validation failed: {str(e)}")
        sys.exit(1)
    finally:
        source_conn.close()
        dest_conn.close()

if __name__ == '__main__':
    validate_data()