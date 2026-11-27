"""What a person wants to see first (layer 3 of the access model; program step 2).

A preference sets the default view and what is sent. It never blocks a question and never
widens access: every value is narrowed by the person's access record when it is read. One
table, one row per subject (a role or a person) and area; an area is a validated shape in
`areas.py`. This package depends on access control; access control never imports it.
"""

from app.services.preferences.areas import AREAS
from app.services.preferences.commands import KINDS, run
from app.services.preferences.mirror import mirror_user, sweep_once, user_payload
from app.services.preferences.store import line_search_days, own, reset, resolve, save

__all__ = ["AREAS", "KINDS", "line_search_days", "mirror_user", "own", "reset", "resolve", "run",
           "save", "sweep_once", "user_payload"]
