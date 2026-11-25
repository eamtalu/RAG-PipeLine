"""The data an answer may use: the asker's customer, sites and data categories (step 45).

One door for the agent's data tools. A `DataScope` is built from a person's access record, and
`limit_session` makes a database session add that scope to every select of a classified table,
wherever the query is built. Features depend on this package; it depends only on access control
and the persistence layer.
"""

from app.services.scoped_data.registry import UnclassifiedTable, closed_tables
from app.services.scoped_data.places import scope_for
from app.services.scoped_data.scope import DataScope, Place
from app.services.scoped_data.session import limit_session, open_session, scope_of, tables_read

__all__ = ["DataScope", "Place", "UnclassifiedTable", "closed_tables", "limit_session",
           "open_session", "scope_for", "scope_of", "tables_read"]
