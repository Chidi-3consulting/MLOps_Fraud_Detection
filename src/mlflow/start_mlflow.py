#!/usr/bin/env python3
"""
MLflow startup script that patches Flask host validation and then runs the
standard `mlflow server` CLI in-process so the patch takes effect.
"""
import os
import sys

import werkzeug.serving as wz_serving
from werkzeug.serving import WSGIRequestHandler

class NoHostCheckHandler(WSGIRequestHandler):
    """Request handler that bypasses Flask's host header validation."""

    def make_environ(self):
        environ = super().make_environ()
        host = environ.get('HTTP_HOST', 'localhost:5500')
        environ['_ORIGINAL_HTTP_HOST'] = host
        environ['HTTP_HOST'] = 'mlflow-server:5500'
        environ['SERVER_NAME'] = 'mlflow-server'
        return environ

# Apply the patched handler globally before mlflow imports Flask
wz_serving.WSGIRequestHandler = NoHostCheckHandler

def build_cli_argv():
    backend_store_uri = os.environ.get('MLFLOW_BACKEND_STORE_URI', '')
    artifact_root = os.environ.get('MLFLOW_ARTIFACTS_DESTINATION')
    argv = [
        'mlflow',
        'server',
        '--host',
        '0.0.0.0',
        '--port',
        '5500',
        '--backend-store-uri',
        backend_store_uri,
    ]
    if artifact_root:
        argv.extend(['--default-artifact-root', artifact_root])
    return argv

def main():
    # Build CLI args and run mlflow CLI in-process
    sys.argv = build_cli_argv()
    try:
        from mlflow import cli
        cli.cli()
    except SystemExit as exc:
        raise
    except Exception as exc:
        print(f'Error starting MLflow server: {exc}', file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()