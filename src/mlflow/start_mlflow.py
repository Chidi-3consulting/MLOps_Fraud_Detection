#!/usr/bin/env python3
"""
MLflow startup script that patches Flask before starting MLflow server.
"""
import os
import sys

# Patch Werkzeug/Flask host checking BEFORE importing mlflow
import werkzeug.serving
from werkzeug.serving import WSGIRequestHandler

class NoHostCheckHandler(WSGIRequestHandler):
    def make_environ(self):
        environ = super().make_environ()
        if 'HTTP_HOST' in environ:
            environ['HTTP_HOST'] = 'localhost:5500'
        return environ

# Patch Werkzeug functions
_original_run_simple = werkzeug.serving.run_simple
def patched_run_simple(*args, **kwargs):
    kwargs['request_handler'] = NoHostCheckHandler
    return _original_run_simple(*args, **kwargs)
werkzeug.serving.run_simple = patched_run_simple

_original_make_server = werkzeug.serving.make_server
def patched_make_server(*args, **kwargs):
    kwargs['request_handler'] = NoHostCheckHandler
    return _original_make_server(*args, **kwargs)
werkzeug.serving.make_server = patched_make_server

# Now start MLflow using subprocess (patches won't apply, so use direct API call)
try:
    from mlflow.server import _run_server
    
    backend_store_uri = os.environ.get('MLFLOW_BACKEND_STORE_URI', '')
    artifact_root = os.environ.get('MLFLOW_ARTIFACTS_DESTINATION', None)
    
    _run_server(
        host='0.0.0.0',
        port=5500,
        backend_store_uri=backend_store_uri,
        default_artifact_root=artifact_root,
    )
except ImportError:
    # Fallback: use subprocess
    import subprocess
    backend_store_uri = os.environ.get('MLFLOW_BACKEND_STORE_URI', '')
    artifact_root = os.environ.get('MLFLOW_ARTIFACTS_DESTINATION', None)
    cmd = ['mlflow', 'server', '--host', '0.0.0.0', '--port', '5500', '--backend-store-uri', backend_store_uri]
    if artifact_root:
        cmd.extend(['--default-artifact-root', artifact_root])
    sys.exit(subprocess.call(cmd))
