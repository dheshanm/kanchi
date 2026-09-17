"""add airflow task identity columns

Airflow's Celery executor submits every task instance as the same Celery task,
so the DAG/task/run identity has to be decoded from the payload and stored
alongside the event. These columns stay NULL for non-Airflow deployments.

Revision ID: d3b7c1a45e90
Revises: c8a4e7d9b123
Create Date: 2026-09-16

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd3b7c1a45e90'
down_revision = 'c8a4e7d9b123'
branch_labels = None
depends_on = None


AIRFLOW_COLUMNS = (
    ('celery_task_name', sa.String(length=255)),
    ('airflow_dag_id', sa.String(length=255)),
    ('airflow_task_id', sa.String(length=255)),
    ('airflow_run_id', sa.String(length=255)),
    ('airflow_try_number', sa.Integer()),
    ('airflow_map_index', sa.Integer()),
    ('airflow_meta', sa.JSON()),
)

TABLES = ('task_events', 'task_latest')

INDEXES = (
    ('task_events', 'idx_airflow_dag_timestamp', ['airflow_dag_id', 'timestamp']),
    ('task_events', 'idx_airflow_run', ['airflow_run_id']),
    ('task_events', 'ix_task_events_airflow_dag_id', ['airflow_dag_id']),
    ('task_events', 'ix_task_events_airflow_task_id', ['airflow_task_id']),
    ('task_events', 'ix_task_events_airflow_run_id', ['airflow_run_id']),
    ('task_latest', 'idx_task_latest_airflow_dag_ts', ['airflow_dag_id', 'timestamp']),
    ('task_latest', 'idx_task_latest_airflow_run', ['airflow_run_id']),
    ('task_latest', 'ix_task_latest_airflow_dag_id', ['airflow_dag_id']),
    ('task_latest', 'ix_task_latest_airflow_task_id', ['airflow_task_id']),
    ('task_latest', 'ix_task_latest_airflow_run_id', ['airflow_run_id']),
)


def _existing_columns(table):
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return {column['name'] for column in inspector.get_columns(table)}


def _existing_indexes(table):
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return {index['name'] for index in inspector.get_indexes(table)}


def upgrade():
    for table in TABLES:
        present = _existing_columns(table)
        for name, column_type in AIRFLOW_COLUMNS:
            if name not in present:
                op.add_column(table, sa.Column(name, column_type, nullable=True))

    for table, index_name, columns in INDEXES:
        if index_name not in _existing_indexes(table):
            op.create_index(index_name, table, columns)


def downgrade():
    for table, index_name, _columns in INDEXES:
        if index_name in _existing_indexes(table):
            op.drop_index(index_name, table_name=table)

    for table in TABLES:
        present = _existing_columns(table)
        for name, _column_type in AIRFLOW_COLUMNS:
            if name in present:
                op.drop_column(table, name)
