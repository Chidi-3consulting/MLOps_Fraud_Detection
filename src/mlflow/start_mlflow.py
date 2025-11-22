#!/usr/bin/env python3
"""
MLflow server startup script that disables Flask host header validation.
This fixes the "Invalid Host header - possible DNS rebinding attack detected" error
when MLflow is accessed from other containers in Docker.
"""
import os
import sys

# Patch Werkzeug to disable host header validation before importing MLflow
# This must be done before any Flask/Werkzeug imports
def patch_werkzeug():
    """Patch Werkzeug to disable host header validation"""
    try:
        import werkzeug.serving
        
        # Monkey-patch Werkzeug's host validation to always return True
        def _patched_validate_host(hostname, allowed_hosts):
            """Always return True to disable host validation"""
            return True
        
        werkzeug.serving.validate_host = _patched_validate_host
        print("✓ Patched Werkzeug host validation")
        return True
    except Exception as e:
        print(f"Warning: Could not patch Werkzeug: {e}")
        return False

# Apply patch before any MLflow imports
patch_werkzeug()

# Get configuration from environment variables
backend_store_uri = os.environ.get('MLFLOW_BACKEND_STORE_URI')
artifact_root = os.environ.get('MLFLOW_ARTIFACTS_DESTINATION')
host = os.environ.get('MLFLOW_SERVER_HOST', '0.0.0.0')
port = os.environ.get('MLFLOW_SERVER_PORT', '5500')

# Build command arguments
cmd_args = [
    'mlflow', 'server',
    '--host', host,
    '--port', str(port),
]

if backend_store_uri:
    cmd_args.extend(['--backend-store-uri', backend_store_uri])

if artifact_root:
    cmd_args.extend(['--default-artifact-root', artifact_root])

# Start MLflow server
print(f"Starting MLflow server on {host}:{port}")
print(f"Backend store: {backend_store_uri}")
print(f"Artifact root: {artifact_root}")

# Set sys.argv and call MLflow's CLI
sys.argv = cmd_args

# Import and run MLflow server CLI
# Re-patch after import in case werkzeug was re-imported
try:
    from mlflow.server import cli
    # Re-apply patch after import
    patch_werkzeug()
    cli()
except ImportError:
    # Fallback: use subprocess if direct import doesn't work
    import subprocess
    sys.exit(subprocess.call(cmd_args))

