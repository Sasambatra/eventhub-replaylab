"""Verify receiver egress and database credentials with bounded real probes."""

import json
import subprocess

from docker_command import ROOT, docker_command

PROBE = """import json,os,socket
import psycopg
result={}
try:
 s=socket.create_connection(('1.1.1.1',443),timeout=1);s.close();result['external_egress_blocked']=False
except OSError:
 result['external_egress_blocked']=True
try:
 c=psycopg.connect(host='postgres',port=5432,user=os.environ['LAB_DB_USER'],password=os.environ['LAB_DB_PASSWORD'],dbname='eventhub',connect_timeout=2);c.close();result['core_database_denied']=False
except psycopg.Error:
 result['core_database_denied']=True
result['core_secrets_absent']=all(k not in os.environ for k in ['DB_PASSWORD','MIGRATION_PASSWORD','POSTGRES_PASSWORD','KEY_HASH_PEPPER','SECRET_ENCRYPTION_KEY'])
result['docker_socket_absent']=not os.path.exists('/var/run/docker.sock')
print(json.dumps(result))
"""
results = {}
for variant in ["a", "b"]:
    response = subprocess.run(
        docker_command() + ["exec", "eventhub-replaylab-receiver-" + variant + "-1", "python", "-c", PROBE],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    results[variant] = json.loads(response.stdout)
path = ROOT / ".local/reports/isolation.json"
path.write_text(json.dumps(results, indent=2) + "\n")
path.chmod(0o600)
print(json.dumps(results))
if not all(value for result in results.values() for value in result.values()):
    raise SystemExit(1)
