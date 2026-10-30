import argparse

import uvicorn

from lab.app import create_lab_app

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", type=int, required=True)
args = parser.parse_args()
uvicorn.run(create_lab_app(), host=args.host, port=args.port, log_level="warning", access_log=False)
