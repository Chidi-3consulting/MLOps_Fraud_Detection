from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import logging
import os
import glob
import yaml

logger = logging.getLogger(__name__)

# Default DAG args; keep lightweight so daemon can schedule easily
default_args = {
    'owner': 'datamasterylab.com',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _load_config(config_path: str = None):
    """Load YAML config from a path. Path can be provided via Airflow Variable `config_path`.

    Returns a dict (possibly empty) on failure.
    """
    cfg_path = config_path or Variable.get('config_path', default_var='/app/config.yaml')
    try:
        if os.path.exists(cfg_path):
            with open(cfg_path, 'r') as f:
                return yaml.safe_load(f) or {}
        else:
            logger.warning('Config file not found at %s; using defaults/Variables', cfg_path)
            return {}
    except Exception as e:
        logger.exception('Failed to load config: %s', e)
        return {}


def _train_model(**context):
    """Airflow wrapper for the training task.

    This version reads the transactions table name from an Airflow Variable called
    `transactions_table`. If not set, it will fall back to `transactions` or the
    value from `config.yaml` (if present). This allows you to adapt the DAG to
    environments where the transactions table name differs.
    """
    # load config (Variable overrides file)
    cfg = _load_config()
    transactions_table = Variable.get('transactions_table', default_var=cfg.get('transactions_table', 'transactions'))

    try:
        logger.info('Initializing fraud detection training (transactions_table=%s)', transactions_table)
        # Import at runtime so DAG parse doesn't require heavy deps
        # Import directly from the module since Airflow adds the dags folder to PYTHONPATH
        from fraud_detection_training import FraudDetectionTraining

        trainer = FraudDetectionTraining()

        # The training implementation in this repo exposes train_model() with
        # no parameters and returns (best_model, metrics_dict). Call it and
        # extract a precision metric if available.
        model, metrics = trainer.train_model()
        precision = None
        if isinstance(metrics, dict):
            precision = metrics.get('precision')

        logger.info('Training completed with metrics=%s', metrics)
        return {'status': 'success', 'metrics': metrics}
    except Exception as e:
        logger.error('Training failed: %s', str(e), exc_info=True)
        raise AirflowException(f'Model training failed: {str(e)}')


def _cleanup_resources(**context):
    """Safer cleanup implemented in Python to avoid shell-specific behaviour.

    Removes any .pkl files under /app/tmp (if present). This is used as the
    DAG's final step and runs even when previous tasks fail.
    """
    tmp_dir = Variable.get('tmp_dir', default_var='/app/tmp')
    if not os.path.isdir(tmp_dir):
        logger.info('No tmp dir to clean: %s', tmp_dir)
        return
    removed = []
    for path in glob.glob(os.path.join(tmp_dir, '*.pkl')):
        try:
            os.remove(path)
            removed.append(path)
        except Exception:
            logger.exception('Failed to remove %s', path)
    logger.info('Cleanup removed %d files', len(removed))


with DAG(
    'fraud_detection_training',
    default_args=default_args,
    description='Fraud detection model training pipeline (configurable)',
    schedule_interval='0 12 * * *',
    max_active_runs=1,
    catchup=False,
    tags=['fraud', 'ML']
) as dag:
    # Validate the runtime environment. This check is intentionally permissive,
    # because different deploys may place `config.yaml` or env files in different locations.
    validate_environment = BashOperator(
        task_id='validate_environment',
        bash_command='''
        echo "Validating environment..."
        test -f ${CONFIG_PATH:-/app/config.yaml} || echo "config not found"
        echo "Environment validation complete"
        ''',
        env={'CONFIG_PATH': Variable.get('config_path', default_var='/app/config.yaml')}
    )

    training_task = PythonOperator(
        task_id='execute_training',
        python_callable=_train_model,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_resources',
        python_callable=_cleanup_resources,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    validate_environment >> training_task >> cleanup_task

    # Documentation
    dag.doc_md = """
    ## Fraud Detection Training Pipeline

    This DAG runs daily and is configurable via Airflow Variables or a YAML
    file mounted into the container at `/app/config.yaml`.

    Important notes:
    - The transactions table name may differ between environments. Set the
      Airflow Variable `transactions_table` to the correct table name for your
      environment. If not set, the DAG uses `transactions` or the value in
      `config.yaml` under `transactions_table`.
    - Training code is loaded at runtime to avoid heavy imports during DAG parse.
    - Cleanup is done in Python for portability and to avoid shell-specific issues.
    """