"""Check the topic gate against the real model (chunk 132).

    PYTHONPATH=. .venv/bin/python scripts/eval_topic_gate.py

Asks the gate of both agents about questions that must pass (warehouse questions, however short or
informal) and questions that must be declined (clearly unrelated), and prints what it got wrong.
Makes one small model call per question; run it after changing the gate's instructions or the model.
Live result on 2026-09-30 with Bedrock Qwen3 235B: 32/32 for both agents, twice.
"""

import asyncio

from app.services.agent_core.guards import OUT_OF_SCOPE, TopicGate
from app.services.analytics_agent.agent import DOMAIN as ANALYTICS_DOMAIN
from app.services.logspace_agent.agent import DOMAIN as LOGSPACE_DOMAIN, make_model

MUST_PASS = [
    "what's short right now?", "which deliveries leave incomplete today?", "who picked request 5581590?",
    "summarise yesterday for the ops meeting", "any stock problems today?", "how is BCHAM doing?",
    "which calls failed today?", "which deliveries had zero-picks today?", "who had the most zero-picks today?",
    "summarise today's soft results by method", "is the server ok?", "what's going on in the warehouse?",
    "has 29428 gone out?", "thanks", "hello, what can you do?", "why is PEVANS so slow?",
    "any errors on the handhelds?", "show me BRI04", "what about yesterday?", "is item 104607 in stock?",
]
MUST_DECLINE = [
    "write me a poem about the sea", "what's the weather in London?", "who won the world cup?",
    "write python code to sort a list", "tell me a joke", "what is the capital of France?",
    "how is the stock market doing?", "who is the CEO of Amazon?", "what time is it in Tokyo?",
    "recommend a good film for tonight", "translate hello into Spanish", "what's 17 times 23?",
]


async def main() -> None:
    model = make_model()
    for name, domain in (("logspace", LOGSPACE_DOMAIN), ("analytics", ANALYTICS_DOMAIN)):
        gate = TopicGate(model, domain)
        wrong = []
        for question in MUST_PASS + MUST_DECLINE:
            declined = await gate.classify(question) == OUT_OF_SCOPE
            if declined != (question in MUST_DECLINE):
                wrong.append(question)
        total = len(MUST_PASS) + len(MUST_DECLINE)
        print(f"{name}: {total - len(wrong)}/{total} right" + (f"; wrong: {wrong}" if wrong else ""))


if __name__ == "__main__":
    asyncio.run(main())
