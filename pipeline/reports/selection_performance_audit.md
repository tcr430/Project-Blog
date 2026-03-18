# Selection Performance Audit

- Generated on: 2026-03-18T22:22:38Z
- Full fetched candidate count: 7227
- Full architecture concept count: 7188

## Key Finding

- The dominant bottleneck is concept validation inside `reject_invalid_and_duplicates()`.
- On a 200-candidate sample, `reject_invalid_and_duplicates()` took 0.108s.
- The rest of the same 200-candidate selection path was small: overlap 0.051s, history 0.027s, season 0.001s, scoring 0.005s.

## Validation Benchmarks

- 100 candidates: 0.373s (3.73 ms/candidate)
- 200 candidates: 0.112s (0.561 ms/candidate)
- Estimated full validation time for the current auto-candidate pool: about 4.1s.

## Likely Root Cause

- `validate_article_concept.py` rebuilds cluster maps via `load_content_clusters()` during candidate validation.
- `load_content_clusters()` is expensive because it reparses the persisted architecture and composes subtopic data.
- `load_content_constraints()` is cheap in comparison.

## Loader Costs

- `load_content_clusters`: 324.921 ms/call over 10 calls
- `load_content_constraints`: 7.169 ms/call over 10 calls

## Secondary Observations

- `build_content_plan_outputs()` took 2.89s, which is noticeable but not the main regression.
- `fetch_candidate_trends(source='auto')` took 2.782s.
- `build_article_concepts()` took 2.356s.
- `fetch_pinterest_trends()` took 0.021s.

## Safest Next Optimization Targets

- Cache or memoize architecture/constraint state inside `validate_article_concept.py`.
- Avoid rebuilding cluster maps for every candidate.
- Only after that, remeasure the full weekly selection path before touching scoring or planning logic.
