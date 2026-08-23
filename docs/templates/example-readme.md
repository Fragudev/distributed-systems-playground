<!--
  Template every example/<name>/README.md follows, per the documentation standard in
  02-distributed-systems-playground.md. Delete this comment block when copying.
-->

# &lt;Example name&gt;

One sentence: what pattern this demonstrates and which interview question it answers.

## 1. Problem
What breaks, or what question is unanswered, without this pattern.

## 2. Naive solution
The obvious-but-wrong approach, concretely — ideally with a runnable/demonstrable failure, not just
a description.

## 3. Improved solution
What this example actually implements, and why it fixes the naive solution's failure.

## 4. Architecture
Diagram (link into `docs/diagrams/`) + brief walkthrough of the components involved.

## 5. Failure modes
The specific, reproducible failure scenario(s) this example demonstrates (see the project-level
`docs/adr` and the failure scenarios in the planning doc). How to trigger it with
`scripts/inject-failure.sh`, and what "recovered correctly" looks like.

## 6. Trade-offs
What this solution costs (complexity, latency, operational burden) against what it buys.

## 7. Testing
What's covered, at which level (unit / integration / broker / failure-path), and why those tests
prove the guarantee rather than just inflating coverage.

## 8. Operational concerns
What an on-call engineer would need to know: metrics to watch, dashboards, what a healthy vs.
unhealthy state looks like.

## 9. When not to use this pattern
The honest limits — when the added complexity isn't worth it.

## Running this example

```bash
../../scripts/run-example.sh <name>
```
