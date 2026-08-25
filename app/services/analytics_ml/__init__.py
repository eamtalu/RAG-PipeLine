"""M1. The ML pipeline: training sets pinned to a point in the fact ledger, and model outputs.

Deliberately a separate package from `services/analytics`. That one owns the projection every chart
reads and must stay boring; this one is where experiments live. Keeping them apart means a change here
cannot make a warehouse total wrong.
"""
