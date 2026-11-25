"""The data an answer may use: the asker's customer, sites and data categories (step 45).

One door for the agent's data tools. A `DataScope` is built from a person's access record, and
`limit_session` makes a database session add that scope to every select of a classified table,
wherever the query is built. Features depend on this package; it depends only on access control
and the persistence layer.
"""

from app.services.scoped_data.registry import UnclassifiedTable
from app.services.scoped_data.scope import DataScope
from app.services.scoped_data.session import limit_session

__all__ = ["DataScope", "UnclassifiedTable", "limit_session"]
