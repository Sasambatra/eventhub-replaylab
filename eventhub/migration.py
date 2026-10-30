from alembic import context

from eventhub.db import Base, migration_engine
from eventhub.settings import Settings


def run(lab=False):
    from eventhub import models  # noqa: F401
    from lab.models import LabBase

    config = Settings()
    target = context.get_x_argument(as_dictionary=True).get("database", config.db_name)
    allowed = (
        {"eventhub_lab_a", "eventhub_lab_b", "eventhub_lab_a_test", "eventhub_lab_b_test"}
        if lab
        else {"eventhub", "eventhub_test"}
    )
    if target not in allowed:
        raise ValueError("Unregistered migration database")
    metadata = LabBase.metadata if lab else Base.metadata
    engine = migration_engine(config, target)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()
