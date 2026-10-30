"""Keep identity/hash tombstones after payload retention expires."""

from alembic import op
import sqlalchemy as sa

revision = "20261030_payload_retention"
down_revision = "20261030_delivery_constraints"
branch_labels = depends_on = None


def upgrade():
    op.alter_column("events", "payload", nullable=True)
    op.add_column("events", sa.Column("payload_expired_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    # Destructive reconstruction would invent data; require an explicit restore instead.
    raise RuntimeError("Retention migration requires restoring a pre-retention backup to downgrade")
