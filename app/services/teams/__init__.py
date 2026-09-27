"""Microsoft Teams bot integration: the backend half.

The edge (repo `teams-agent-edge`, on AWS) receives Teams messages and queues QuestionJobs on SQS.
This package is everything that runs here, behind the firewall:

  contracts.py       the two messages shared with the edge (copied verbatim from the edge repo)
  binding_mirror.py  pushes tenant bindings into the edge's DynamoDB table so the edge never calls us
  memory.py          conversation memory: the last turns of a Teams thread, replayed to the agent
  consumer.py        the SQS consumer: runs the debugging agent per job and posts the answer back
"""
