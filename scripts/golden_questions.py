"""Ask the real model the golden questions and check what does not depend on its words.

    PYTHONPATH=. .venv/bin/python scripts/golden_questions.py \
        --profile analytics --tenant golden --plant

Runs every case of tests/golden/<profile>.yaml through the gateway with the configured model
(ANALYTICS_AGENT_MODEL, with the topic gate), on the named tenant's data, and checks only the
expectations the model's exact wording cannot change: how the run stopped, why it declined, which
tools it must have called, and what the answer must not contain. `--plant` first plants the test
dataset for the tenant (tests/golden/plant.py), which is what the nightly CI job does on its empty
database. Exit code 1 when any case misses an expectation; one line per case either way.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid

from app.agent.capabilities.registry import ToolRegistry
from app.agent.contracts import AgentRequest, Principal
from app.agent.gateway import run_request
from app.agent.profiles import load_profile
from app.agent.runtime.grounded import GroundedAgent
from app.services.analytics_agent.namespaces import analytics_namespace, logs_namespace
from app.services.logspace_agent.namespaces import logspace_namespace
from tests.golden import plant
from tests.golden.harness import Case, load_cases, mismatches


def _request(case: Case, tenant: str) -> AgentRequest:
    """The case as a channel would ask it: the logspace profile gets the planted day and zone."""
    extra = (
        {"tz_name": plant.LOGSPACE_TZ, "today": plant.LOGSPACE_TODAY}
        if case.profile == "logspace"
        else {}
    )
    return AgentRequest(
        request_id=uuid.uuid4().hex,
        tenant=tenant,
        principal=Principal.for_service(tenant, "golden"),
        profile=case.profile,
        text=case.question,
        conversation_id=None,
        **extra,
    )


async def main(profile: str, tenant: str, do_plant: bool) -> int:
    # Step 1: Plant the dataset when asked, so that an empty database (the nightly job) has the
    # same rows the scripted cases run on.
    if do_plant:
        if profile == "analytics":
            await plant.plant_analytics(tenant)
        else:
            await plant.plant_logspace(tenant)
    # Step 2: One registry and one runtime, composed as the channels compose them, with the real
    # model and the gate from the context.
    registry = ToolRegistry([logs_namespace(), analytics_namespace(), logspace_namespace()])
    runtime = GroundedAgent(tenant, profile=load_profile(profile), registry=registry)
    failed = 0
    for case in load_cases(profile):
        result = await run_request(
            _request(case, tenant), runtime=runtime, prior_turns=case.history
        )
        missed = mismatches(case, result, scripted=False)
        called = [t.tool for t in result.tool_calls]
        status = "ok  " if not missed else "FAIL"
        print(f"{status} {profile}:{case.name} stop={result.stop_reason} tools={called}")
        for line in missed:
            print(f"       {line}")
        if missed:
            failed += 1
            print(f"       answer: {result.answer[:300]!r}")
    print(f"{profile}: {failed} of {len(load_cases(profile))} cases missed an expectation")
    return 1 if failed else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", choices=("analytics", "logspace"), required=True)
    parser.add_argument("--tenant", required=True, help="the customer code to ask about")
    parser.add_argument("--plant", action="store_true", help="plant the test dataset first")
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.profile, args.tenant, args.plant)))
