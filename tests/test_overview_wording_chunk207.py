"""Step 29 (chunk 207): the Teams app's Home is named Overview since edge step 23 (16 November 2026),
so no text the model reads names a "Home tab" a person can no longer find."""

from app.agent.profiles import load_profile
from app.services.analytics_agent.forecast_tools import FORECAST_TOOLS


def test_the_forecast_tool_names_the_overview_page_not_the_home_tab():
    text = FORECAST_TOOLS[0]["description"]
    assert "the Overview page's forecast card" in text
    assert "Home tab" not in text


def test_the_analytics_prompt_names_no_home_tab():
    assert "Home tab" not in load_profile("analytics").system_prompt
