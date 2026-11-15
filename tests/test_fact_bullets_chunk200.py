"""Step 22 (chunk 200): facts as bullets beside a table, and the order named as the tool sorted it.

The owner's request of 15 November: when an answer gives several facts, a lead sentence and one
bullet per fact read better than one paragraph. The table stripper used to remove any bullet with
two or more figures whenever a table was attached, so the owner's own example bullet
"FNACHONLEO … 105.8 seconds … 22 lines" would have vanished. Now up to four short bullets stay;
only a longer list, or a bullet that copies a whole row of the rendered table, is removed. The
live answer of 14 November also called the shortest median time "the longest"."""

from app.agent.presentation.evidence import strip_tables
from app.agent.profiles import load_profile

TABLE = (
    "From the data: 8 of 8 group(s), sorted by median duration asc.\n\n"
    "| user name | releases | median duration s |\n|---|---|---|\n"
    "| DBARNETT | 308 | 52.6 |\n| BSMALL | 48 | 53.2 |\n| SGIAMPORCA | 34 | 54.0 |\n| FNACHONLEO | 22 | 105.8 |"
)
ANSWER = (
    "DBARNETT had the shortest median time per line today, 52.6 seconds, across 654 releases picked by 8 users.\n\n"
    "- BSMALL followed at 53.2 seconds\n"
    "- SGIAMPORCA at 54.0 seconds\n"
    "- FNACHONLEO had the longest median, 105.8 seconds per line, though only across 22 lines\n"
    "  - few lines, so one slow pick weighs a lot"
)


def test_the_owners_example_bullets_all_survive_beside_the_table():
    assert strip_tables(ANSWER, TABLE) == ANSWER


def test_a_bullet_that_copies_a_whole_row_is_removed_and_its_neighbours_stay():
    answer = "Two pickers stand out.\n\n- DBARNETT 308 52.6\n- FNACHONLEO had the longest median, 105.8 seconds"
    assert strip_tables(answer, TABLE) == "Two pickers stand out.\n\n- FNACHONLEO had the longest median, 105.8 seconds"


def test_a_list_longer_than_four_points_is_a_row_dump_and_is_removed():
    answer = "Here are the pickers:\n\n" + "\n".join(f"- picker {i} at {50 + i}.1 seconds" for i in range(5)) + "\n\nSo the spread is narrow."
    assert strip_tables(answer, TABLE) == "Here are the pickers:\n\nSo the spread is narrow."
    # sub-points under a bullet do not count towards the four
    four = "Lead.\n\n- a 1\n  - a detail 2\n- b 3\n  - b detail 4\n- c 5\n- d 6"
    assert strip_tables(four, TABLE) == four


def test_without_a_table_short_lists_stay_and_the_models_own_pipe_table_still_goes():
    answer = "Lead.\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n- 100230 was short by 10 units\n- 104568 by 1 unit"
    assert strip_tables(answer) == "Lead.\n\n- 100230 was short by 10 units\n- 104568 by 1 unit"


def test_the_policy_asks_for_one_bullet_per_fact_and_names_the_sort_order():
    prompt = " ".join(load_profile("analytics").system_prompt.split())  # the fragment wraps its lines
    assert 'each fact as its own "- " bullet, at most four' in prompt
    assert "Never run several facts together in one paragraph" in prompt
    assert "Say the order the tool sorted in" in prompt
    assert "smallest first, the first is the lowest, shortest or fewest" in prompt
    assert "only when they help the reader through three or more points" not in prompt
