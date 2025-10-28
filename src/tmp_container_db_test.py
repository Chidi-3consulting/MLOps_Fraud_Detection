import os, time, json, traceback
from dotenv import load_dotenv
import pg8000

load_dotenv('src/.env')
creds = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}
print('Container script: attempting to connect to Postgres', json.dumps({k:v if k!='password' else '***' for k,v in creds.items()}))
try:
    conn = pg8000.connect(user=creds['user'], password=creds['password'], host=creds['host'], port=creds['port'], database=creds['database'])
    cur = conn.cursor()
    cur.execute('SELECT version()')
    print('Container script: connected, server version:', cur.fetchone())
    # create table and insert a test row
    cur.execute('''CREATE TABLE IF NOT EXISTS transactions (
        id BIGSERIAL PRIMARY KEY,
        transaction_id TEXT UNIQUE,
        user_id TEXT,
        payload JSONB,
        amount NUMERIC,
        is_fraud INTEGER,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
    );''')
    conn.commit()
    tx_id = f'container-test-{int(time.time())}'
    cur.execute("INSERT INTO transactions (transaction_id, user_id, payload, amount, is_fraud) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (transaction_id) DO NOTHING", (tx_id,'ct-user', json.dumps({'note':'from container'}), 1.23, 0))
    conn.commit()
    print('Container script: inserted test transaction id=', tx_id)
    cur.close()
    conn.close()
except Exception:
    print('Container script: error during DB work:')
    traceback.print_exc()

print('Container script: done')
