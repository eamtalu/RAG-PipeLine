"""Whether a tenant has notifications switched on, as a predicate three queries can share.

The switch used to be `settings.notifications_enabled` - one boolean for the whole deployment, read
ONCE at process boot to decide whether the worker task was ever created (`app/background.py`). That
shape made a product-level toggle impossible: turning it on in a UI would write to Postgres, and
nothing would notice, because there was no task running to observe it. So the flag moved onto
`customers` and the check moved into the loop, where it costs one predicate on queries that already
run every tick.

Three places have to agree on the answer:

    rule loading      - a switched-off tenant's rules are not evaluated
    delivery drain    - and its already-queued alerts stop going out
    retention position- and its frozen cursors do not hold the disk hostage

They are one function rather than three copies because they must never disagree. If rule loading said
"off" while the retention position said "on", the effect would not be a wrong alert - it would be
partition retention pinned at a cursor that has stopped moving, on behalf of a tenant that is not
reading. That failure is silent, and it fills the disk.

Written as a predicate over a `customer_code` COLUMN rather than a Python check because all three call
sites are SQL, and pulling the rows back to filter them in Python would defeat the point at the one
that matters most (the drain, which locks rows with FOR UPDATE SKIP LOCKED).
"""

from sqlalchemy import select

from app.persistence.models.customer import Customer


def enabled(customer_code_column):
    """Predicate: the tenant named by this column has notifications switched on.

    An unmatched `customer_code` is NOT admitted. Rows can outlive their tenant row - a purge, a
    rename, a typo - and defaulting those to "on" would resume sending on behalf of a tenant that no
    longer exists. `IN (subquery)` gives that behaviour by construction rather than by a branch
    somebody has to remember.
    """
    return customer_code_column.in_(
        select(Customer.customer_code).where(Customer.notifications_enabled.is_(True)))
