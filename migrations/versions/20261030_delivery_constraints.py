"""Constrain delivery state and counters."""

from alembic import op

revision = "20261030_delivery_constraints"
down_revision = "1498290b27dd"
branch_labels = None
depends_on = None


def upgrade():
    op.create_check_constraint(
        "delivery_positive_counters", "deliveries", "attempt_count >= 0 AND generation > 0"
    )
    op.create_check_constraint(
        "delivery_state",
        "deliveries",
        "state IN ('ready','in_flight','retry_wait','accepted','exhausted','cancelled')",
    )
    op.create_check_constraint(
        "delivery_lease",
        "deliveries",
        "state <> 'in_flight' OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)",
    )
    op.create_check_constraint("attempt_number", "attempts", "number > 0")


def downgrade():
    op.drop_constraint("attempt_number", "attempts")
    for name in ["delivery_lease", "delivery_state", "delivery_positive_counters"]:
        op.drop_constraint(name, "deliveries")
