# CI and the golden questions

Since 10 November 2026 (step 15 of `docs/esmart-agent-execution-plan.md`) every push to `main` and every pull request runs the whole backend suite on GitHub Actions, and the agent has a golden-question file per profile that runs in that pipeline with the scripted model and nightly against the real model.

## The pipeline (`.github/workflows/tests.yml`)

| job | when | what |
| --- | --- | --- |
| `tests` | push to `main`, pull request, manual | a `pgvector/pgvector:pg17` service, Python 3.11, `pip install -r requirements.txt -r requirements-dev.txt`, `alembic upgrade head`, `pytest -q` |
| `golden-nightly` | 03:00 UTC daily, manual | the same Postgres and install, then `scripts/golden_questions.py` for both profiles against the real model; skipped with a notice when the Bedrock secrets are not set |

The runner's environment: `DATABASE_URL` to the service, `TZ=Europe/London` (the developer Mac's zone, so day-boundary tests behave the same), `LOG_OUTPUT=text`.
No `.env` is needed; no setting is required, and the vector extension is created by the app.
`requirements-dev.txt` pins pytest and its plugins at the versions in `.venv`, so the Mac and the runner run the same tools.
A newer push to the same branch cancels a run still in progress.
The first run (10 November 2026) found that two migrations read the live `partitioning.PARTITIONED` list, which had grown since they ran, and failed on an empty database; both now pin the tables they handled, so `alembic upgrade head` replays from scratch.

Seeing a run: `gh run list --workflow tests.yml`, `gh run watch <id>`, or the Actions tab of the repository.
Running the nightly job by hand: `gh workflow run tests.yml`.

## The golden questions

`tests/golden/analytics.yaml` and `tests/golden/logspace.yaml` list, as data a non-programmer can read, what each agent is known to do.
A case is:

| key | meaning |
| --- | --- |
| `name` | unique within the file; the test id is `<profile>:<name>` |
| `question` | what is asked |
| `history` | earlier turns, optional |
| `gate` | `true` when the topic gate runs; the first reply is then its one-word verdict |
| `replies` | the scripted model's turns in order: `{tool, args}` or `{text}` |
| `expect` | `stop_reason` and `tools` (the calls behind the final answer), and optionally `declined`, `retries`, `evidence`, `withheld_figures`, `answer_contains`, `answer_excludes`, `allow_failed_calls` |

The data every case runs on is `tests/golden/plant.py`: the release facts of chunk 124 (18 September 2026) and the log records of chunk 132 (30 September 2026), the same rows those tests have always planted.

`tests/test_golden_questions_chunk165.py` runs one test per case through the gateway with the scripted model and checks every expectation, plus that every recorded tool call succeeded unless the case allows a failed one.
Two more tests hold the files: unique names, and every tool a case names is offered by the profile's namespaces.
A change to the runtime, the guards, the tools or the presentation that alters an answer fails one named case.

`scripts/golden_questions.py --profile <name> --tenant <code> [--plant]` asks the real model (the configured `ANALYTICS_AGENT_MODEL`, with the gate) the same questions and checks only what the model's words cannot change: the stop reason, the decline, the tools as "must include", and the forbidden text.
With `--plant` it plants the test dataset for the tenant first, which is what the nightly job does on its empty database.
A prompt edit that makes the real model stop calling a tool, answer an off-topic question, or quote a secret fails the nightly run; that is the regression the snapshot test cannot see.

## The secrets the nightly job needs

Repository secrets `BEDROCK_AWS_ACCESS_KEY_ID` and `BEDROCK_AWS_SECRET_ACCESS_KEY` for an IAM user allowed to invoke Bedrock in `eu-west-2` and nothing else, and the repository variable `ANALYTICS_AGENT_MODEL` with the model string the server uses.
Until they exist the job prints a notice and passes.

## Adding a case

Write the question, the replies the scripted model should give, and the expectations; run `pytest tests/test_golden_questions_chunk165.py -k <name>`.
A case should describe behaviour the agent has, not behaviour you wish it had: the file is the record, the chunk tests are the proof.
