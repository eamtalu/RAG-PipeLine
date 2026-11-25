"""Microsoft Teams bot integration: the backend half.

The edge (repo `teams-agent-edge`, on AWS) receives Teams messages and queues QuestionJobs on SQS.
This package is everything that runs here, behind the firewall:

  contracts.py       the two messages shared with the edge (copied verbatim from the edge repo)
  command_outcome.py the line, the receipt and Refused that every command family answers with
  commands.py        routes a job's command to its family (Ask forms unwrapped here)
  memory.py          conversation memory: the last turns of a Teams thread, replayed to the agent
  consumer.py        the SQS consumer: runs the debugging agent per job and posts the answer back

Tenant bindings and who may do what live in app/services/access (access control), not here.
"""
