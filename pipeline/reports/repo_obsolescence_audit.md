# Repository Obsolescence Audit

- Generated at: `2026-03-18T00:19:12.877832+00:00`
- Scope: `D:\Project\Blog`

## Active Pipeline Flow

### Workflow entrypoints
- `.github/workflows/publish.yml -> pipeline/scripts/weekly_pipeline.py`
- `.github/workflows/deploy-site.yml -> pipeline/scripts/sync_shop_the_look.py`
- `.github/workflows/publish-pinterest-queue.yml -> pipeline/scripts/process_pinterest_queue.py`

### Source-of-truth modules
- `planning`: `pipeline/scripts/generate_content_plan.py`
- `article_generation`: `pipeline/scripts/generate_article.py`
- `publishing`: `pipeline/scripts/publish_post.py`
- `metadata_and_indexing`: `pipeline/scripts/generate_cluster_report.py`
- `architecture_schema`:
  - `pipeline/data/content_domains.json`
  - `pipeline/data/content_clusters.json`
  - `pipeline/data/content_subtopics.json`
  - `pipeline/data/content_angles.json`
  - `pipeline/data/content_constraints.json`
  - `pipeline/scripts/content_architecture.py`
- `cluster_pages`: `pipeline/scripts/generate_pillar_pages.py`
- `images`:
  - `pipeline/scripts/generate_image_prompts.py`
  - `pipeline/scripts/generate_images.py`
- `validation`:
  - `pipeline/scripts/validate_article_concept.py`
  - `pipeline/scripts/validate_article_seo.py`
  - `pipeline/scripts/validate_article_editorial.py`
- `pinterest`:
  - `pipeline/scripts/generate_pinterest_intelligence_report.py`
  - `pipeline/scripts/generate_pinterest_topic_signals.py`
  - `pipeline/scripts/generate_pin_metadata.py`
  - `pipeline/scripts/generate_pin_assets.py`
  - `pipeline/scripts/process_pinterest_queue.py`
  - `pipeline/scripts/publish_pins.py`

## How This Audit Distinguished Active vs Legacy Logic

- GitHub Actions workflow entrypoints
- Imports and direct function calls from weekly_pipeline.py
- Reads and writes to pipeline/data and pipeline/reports files
- Template references in _layouts, _includes, and generated pages

- If something is still referenced by a live workflow, script import, or site template, it is not treated as dead.
- If a file appears manual-only, it is marked unclear/manual utility unless evidence shows it is superseded.
- Legacy names are treated separately from obsolete behavior; several files are still active despite misleading pre-migration names.

## Findings

### pipeline/scripts/generate_cluster_pages.py

- Item type: `file`
- Status: `likely obsolete`
- Likely replaced by:
  - `pipeline/scripts/generate_pillar_pages.py`
  - `pipeline/scripts/weekly_pipeline.py`
- Still referenced: `no`
- Why flagged: Older flat cluster hub generator that builds simple cluster pages from article_cluster_index.json and keyword_cluster_report.json.
- Evidence:
  - generate_cluster_pages.py defines build_cluster_pages() and writes generated_cluster_page markdown pages.
  - weekly_pipeline.py imports build_pillar_pages from generate_pillar_pages.py, not build_cluster_pages.
  - generate_pillar_pages.py preserves the same /clusters/<slug>/ URLs but adds subtopic grouping, featured entry points, related clusters, and reading paths.
  - .github/workflows/publish.yml only runs weekly_pipeline.py, which triggers generate_pillar_pages.py through the live pipeline path.
- Recommendation: Review first in a future cleanup pass. It looks superseded by the active pillar-page generator.
- Risk if removed now: `low`

### pipeline/scripts/backfill_cluster_metadata.py

- Item type: `file`
- Status: `likely obsolete`
- Likely replaced by:
  - `pipeline/scripts/backfill_article_architecture.py`
  - `pipeline/scripts/backfill_historical_metadata.py`
- Still referenced: `no`
- Why flagged: Older backfill script that infers keyword-era metadata and rewrites post front matter using topic_clusters.json and topic_taxonomy.json assumptions.
- Evidence:
  - Imports load_default_topic_clusters() from topic_clusters.py and writes topical_cluster/search_intent/front matter fields.
  - backfill_historical_metadata.py imports backfill_article_architecture.py and generate_cluster_report.py, not backfill_cluster_metadata.py.
  - The newer backfill path focuses on cluster_id/subtopic_id/angle_id/canonical_cluster_name rather than keyword-only cluster inference.
- Recommendation: Keep untouched for now, but review as an early removal candidate once the newer backfill path is fully trusted.
- Risk if removed now: `low_to_moderate`

### pipeline/scripts/topic_clusters.py

- Item type: `file`
- Status: `partially replaced`
- Likely replaced by:
  - `pipeline/scripts/content_architecture.py`
  - `pipeline/data/content_clusters.json`
  - `pipeline/data/content_subtopics.json`
  - `pipeline/data/content_angles.json`
- Still referenced: `yes`
- Why flagged: Mixed compatibility module: still provides TopicCandidate helpers and normalization utilities, but also contains the older keyword-first taxonomy expansion system.
- Evidence:
  - content_architecture.py is now the persisted architecture source of truth for cluster/subtopic/angle concept generation.
  - fetch_trends.py still imports build_topic_candidate(), expand_clusters_to_candidates(), and load_default_topic_clusters() from topic_clusters.py as fallback paths.
  - weekly_pipeline.py, generate_article.py, and generate_content_plan.py still import TopicCandidate and build_manual_topic_candidate from topic_clusters.py.
- Recommendation: Do not remove as a whole. Split active compatibility helpers from legacy taxonomy-generation code in a later refactor.
- Risk if removed now: `high`

### pipeline/scripts/topic_clusters.py::build_feature_cluster_definition / build_material_room_cluster_definition / build_color_room_cluster_definition / build_small_space_keywords / build_seasonal_keywords / load_default_topic_clusters / expand_clusters_to_candidates

- Item type: `function`
- Status: `partially replaced`
- Likely replaced by:
  - `pipeline/scripts/content_architecture.py::build_article_concepts`
  - `pipeline/data/content_clusters.json`
  - `pipeline/data/content_subtopics.json`
  - `pipeline/data/content_constraints.json`
- Still referenced: `yes`
- Why flagged: Keyword-era cluster and candidate construction logic driven by room/feature/material/palette combinations and taxonomy expansion.
- Evidence:
  - topic_clusters.py still defines these taxonomy-style generators.
  - fetch_trends.py only reaches them through load_cluster_candidates() after Pinterest and architecture candidates fail or are unavailable.
  - The active planning/selection/publishing flow now uses cluster_id/subtopic_id/angle_id and architecture-derived concepts first.
- Recommendation: Keep as fallback/compatibility logic for now. Review whether the fallback path is still desired before deleting any of these functions.
- Risk if removed now: `moderate_to_high`

### pipeline/data/topic_clusters.json

- Item type: `data structure`
- Status: `legacy but still needed for backward compatibility`
- Likely replaced by:
  - `pipeline/data/content_clusters.json`
  - `pipeline/data/content_subtopics.json`
- Still referenced: `yes`
- Why flagged: Older flat cluster list used by topic_clusters.py for default keyword-era cluster loading.
- Evidence:
  - topic_clusters.py defines TOPIC_CLUSTERS_PATH and still loads topic_clusters.json.
  - fetch_trends.py still calls load_default_topic_clusters() through its fallback branch.
  - The new persisted architecture files now hold the primary cluster and subtopic definitions.
- Recommendation: Treat as transitional fallback data. Review only after fallback candidate generation is intentionally retired.
- Risk if removed now: `moderate`

### pipeline/data/topic_taxonomy.json

- Item type: `data structure`
- Status: `legacy but still needed for backward compatibility`
- Likely replaced by:
  - `pipeline/data/content_clusters.json`
  - `pipeline/data/content_subtopics.json`
  - `pipeline/data/content_constraints.json`
- Still referenced: `yes`
- Why flagged: Older taxonomy expansion source for generating room/feature/material/palette clusters.
- Evidence:
  - topic_clusters.py defines TOPIC_TAXONOMY_PATH and uses it in the legacy cluster-building path.
  - The new architecture stores explicit clusters/subtopics/constraints rather than deriving them from taxonomy combinations.
- Recommendation: Keep until the keyword-era fallback generator is deliberately removed.
- Risk if removed now: `moderate`

### pipeline/data/candidate_trends.json

- Item type: `data structure`
- Status: `legacy but still needed for backward compatibility`
- Likely replaced by:
  - `pipeline/scripts/content_architecture.py`
  - `pipeline/scripts/fetch_pinterest_trends.py`
- Still referenced: `yes`
- Why flagged: Small fallback candidate source used when Pinterest and architecture candidate generation are unavailable.
- Evidence:
  - fetch_trends.py defines DEFAULT_CANDIDATES_PATH and reads candidate_trends.json near the end of its fallback chain.
  - fetch_trends.py now prefers Pinterest candidates and architecture candidates first.
- Recommendation: Keep as a resilient fallback unless you explicitly decide the pipeline should hard-fail without richer candidate sources.
- Risk if removed now: `moderate`

### pipeline/scripts/fetch_trends.py::BUILTIN_DECOR_TRENDS / load_cluster_candidates / DEFAULT_CANDIDATES_PATH fallback branch

- Item type: `function`
- Status: `partially replaced`
- Likely replaced by:
  - `pipeline/scripts/content_architecture.py::build_article_concepts`
  - `pipeline/scripts/fetch_pinterest_trends.py`
- Still referenced: `yes`
- Why flagged: Candidate sourcing layer that now mixes active architecture-first logic with older mock, file, and keyword-cluster fallback paths.
- Evidence:
  - fetch_trends.py now loads architecture candidates with load_architecture_candidates() and merges Pinterest + architecture sources first.
  - The same file still keeps BUILTIN_DECOR_TRENDS, load_cluster_candidates(), and candidate_trends.json fallback handling.
- Recommendation: Review as a duplicate-logic hotspot. The file is active, but its older fallback branches should be assessed for continued value.
- Risk if removed now: `moderate`

### pipeline/scripts/validate_article.py

- Item type: `file`
- Status: `duplicated`
- Likely replaced by:
  - `pipeline/scripts/weekly_pipeline.py`
  - `pipeline/scripts/validate_article_seo.py`
  - `pipeline/scripts/validate_article_editorial.py`
- Still referenced: `no`
- Why flagged: Wrapper CLI that runs both validate_article_seo.py and validate_article_editorial.py and writes a combined report.
- Evidence:
  - weekly_pipeline.py imports validate_article_seo and validate_article_editorial directly and writes their reports separately.
  - No workflow runs validate_article.py directly.
  - validate_article.py is still useful as a manual CLI convenience wrapper.
- Recommendation: Keep as a manual utility if it is still helpful. Otherwise review as a cleanup candidate after confirming no one relies on the combined CLI.
- Risk if removed now: `low`

### pipeline/scripts/generate_pipeline_cost_audit.py

- Item type: `file`
- Status: `unclear / needs manual review`
- Likely replaced by: none identified
- Still referenced: `no`
- Why flagged: Standalone cost-audit utility for the pipeline.
- Evidence:
  - No workflow references this script.
  - The file includes a MISSING_SCRIPTS list with entries such as generate_cluster_hubs.py and generate_seo_report.py that do not match the current repo layout.
  - It still models prompt construction using older search_intent and mock-product assumptions, which may now lag behind the live generation flow.
- Recommendation: Review manually before trusting or deleting it. It may still be useful, but parts of its assumptions appear stale.
- Risk if removed now: `low`

### pipeline/reports/pipeline_cost_audit.json

- Item type: `report`
- Status: `unclear / needs manual review`
- Likely replaced by: none identified
- Still referenced: `no`
- Why flagged: Generated cost-audit output from generate_pipeline_cost_audit.py.
- Evidence:
  - No workflow reads this report.
  - The live pipeline writes and reads many other reports, but this one appears informational only.
- Recommendation: Treat as an informational artifact, not an authoritative runtime input.
- Risk if removed now: `low`

### pipeline/data/keyword_cluster_report.json

- Item type: `data structure`
- Status: `legacy but still needed for backward compatibility`
- Likely replaced by: none identified
- Still referenced: `yes`
- Why flagged: Active cluster report output, but with a pre-migration name that still suggests a keyword-first system.
- Evidence:
  - generate_cluster_report.py writes keyword_cluster_report.json as the main cluster-report output.
  - weekly_pipeline.py, generate_content_plan.py, validate_article_seo.py, and monthly/backfill scripts still read this file.
  - The contents now include architecture-aware fields such as cluster_id, subtopic coverage, angle distribution, and legacy cluster aliases.
- Recommendation: Keep for now. Rename only through a careful compatibility migration because it is still an active shared report.
- Risk if removed now: `high`

### front matter / metadata fields: topical_cluster and search_intent

- Item type: `data structure`
- Status: `legacy but still needed for backward compatibility`
- Likely replaced by:
  - `cluster_id`
  - `subtopic_id`
  - `angle_id`
  - `intent_id`
  - `canonical_cluster_name`
- Still referenced: `yes`
- Why flagged: Pre-architecture metadata fields that still support category derivation, layouts, legacy content, and compatibility with older reports and posts.
- Evidence:
  - publish_post.py still validates and writes topical_cluster and search_intent.
  - _layouts/post.html still uses page.topical_cluster to find cluster hub backlinks and same-cluster related posts at render time.
  - generate_article.py still emits topical_cluster and search_intent alongside the newer architecture fields.
  - backfill_historical_metadata.py and generate_cluster_report.py still preserve and reconcile these legacy fields.
- Recommendation: Keep until the site templates and all report consumers are fully migrated off these fields.
- Risk if removed now: `high`

### _layouts/post.html::topical_cluster-based related-post logic

- Item type: `template`
- Status: `partially replaced`
- Likely replaced by:
  - `pipeline/scripts/internal_linking.py`
  - `pipeline/scripts/generate_article.py`
  - `pipeline/scripts/publish_post.py`
- Still referenced: `yes`
- Why flagged: Runtime site template logic that builds cluster backlinks and related-post sections from page.topical_cluster.
- Evidence:
  - internal_linking.py now builds architecture-aware suggestions using cluster_id/subtopic_id/angle_id/intent_id.
  - generate_article.py feeds those suggestions into generation, and publish_post.py can append a Read Next block when internal links are sparse.
  - post.html still renders its own topical_cluster-based related content on the site, so the old and new systems currently coexist.
- Recommendation: Review as a duplicate-logic area, not an immediate deletion candidate. It still affects public pages.
- Risk if removed now: `high`

### pipeline/prompts/formats/trend_guide.md and pipeline/prompts/formats/styling_advice.md

- Item type: `template`
- Status: `partially replaced`
- Likely replaced by:
  - `pipeline/prompts/formats/ideas_article.md`
  - `pipeline/prompts/formats/how_to_guide.md`
  - `pipeline/prompts/formats/mistakes_and_fixes.md`
  - `pipeline/prompts/formats/best_options.md`
  - `ANGLE_STRUCTURE_GUIDANCE in pipeline/scripts/generate_article.py`
- Still referenced: `yes`
- Why flagged: Older format prompts from the broader pre-angle-intent model.
- Evidence:
  - generate_article.py still includes both files in FORMAT_FILE_MAP.
  - ANGLE_FORMAT_MAP now prioritizes ideas/how_to/mistakes/best_options for the main angle-sensitive flow.
  - search_intent still defaults to styling_advice in several compatibility paths, so these prompts are not clearly dead yet.
- Recommendation: Keep until search_intent-based fallback formatting is deliberately retired.
- Risk if removed now: `moderate`

### pipeline/data/article-package-*.json and pipeline/data/article_packages/

- Item type: `data structure`
- Status: `legacy but still needed for backward compatibility`
- Likely replaced by: none identified
- Still referenced: `yes`
- Why flagged: Generated article package artifacts and cache outputs used for debugging, caching, and manual inspection.
- Evidence:
  - weekly_pipeline.py saves temp article packages under pipeline/data/article-package-<timestamp>-<slug>.json.
  - generate_article.py uses pipeline/data/article_packages as a cache directory.
  - These files are not source-of-truth content, but they are part of the current generation and recovery workflow.
- Recommendation: Treat as operational artifacts rather than authoritative data. Review retention policy later instead of deleting blindly.
- Risk if removed now: `moderate`

### pipeline/reports/article_seo_validation_report.json and pipeline/reports/article_editorial_validation_report.json

- Item type: `report`
- Status: `legacy but still needed for backward compatibility`
- Likely replaced by: none identified
- Still referenced: `yes`
- Why flagged: Latest-run validation outputs for SEO and editorial checks.
- Evidence:
  - weekly_pipeline.py writes these reports through write_validation_report() and write_editorial_validation_report().
  - They are not authoritative planning data, but they remain useful operational outputs for the current validation flow.
- Recommendation: Keep as transient operational reports. Treat them as diagnostic outputs rather than durable state.
- Risk if removed now: `low_to_moderate`

### pipeline/scripts/list_pinterest_boards.py

- Item type: `file`
- Status: `unclear / needs manual review`
- Likely replaced by: none identified
- Still referenced: `no`
- Why flagged: Manual Pinterest utility for listing boards.
- Evidence:
  - No workflow references this script.
  - It appears to be a standalone operational helper rather than part of the automated publish path.
- Recommendation: Keep if it is still part of manual Pinterest operations; otherwise review later as a utility-script cleanup candidate.
- Risk if removed now: `low`

## High-Level Summary

### Likely safe future cleanup candidates

- pipeline/scripts/generate_cluster_pages.py
- pipeline/scripts/backfill_cluster_metadata.py
- pipeline/scripts/validate_article.py
- pipeline/scripts/list_pinterest_boards.py
- pipeline/reports/pipeline_cost_audit.json

### Keep for backward compatibility

- pipeline/scripts/topic_clusters.py (at least its shared types/helpers)
- pipeline/data/topic_clusters.json
- pipeline/data/topic_taxonomy.json
- pipeline/data/candidate_trends.json
- pipeline/data/keyword_cluster_report.json
- front matter / metadata fields: topical_cluster and search_intent

### Areas with duplicate logic

- _layouts/post.html topical_cluster-based related-post logic vs architecture-aware internal_linking.py / publish_post.py
- fetch_trends.py architecture-first candidate sourcing vs legacy keyword-cluster and mock/file fallbacks
- validate_article.py wrapper CLI vs weekly_pipeline.py direct validation calls
- generate_cluster_pages.py vs generate_pillar_pages.py

### Areas where old and new architectures coexist

- cluster_id/subtopic_id/angle_id/intent_id coexist with topical_cluster/search_intent and stable cluster display names
- content_architecture.py drives primary planning, but topic_clusters.py still provides fallback candidate generation and manual candidate helpers
- keyword_cluster_report.json remains active even though its contents are now architecture-aware
- prompt format selection is angle-sensitive first, but still retains styling_advice/trend_guide compatibility prompts

### Highest priority cleanup opportunities for later

- Split topic_clusters.py into active compatibility helpers vs removable keyword-era expansion logic.
- Decide whether fetch_trends.py still needs cluster/file/mock fallback branches once architecture and Pinterest candidate sources are trusted.
- Unify public related-link logic so _layouts/post.html and internal_linking.py are not maintaining parallel recommendation systems.
- Rename or wrap keyword_cluster_report.json behind a clearer architecture-first name only after downstream readers are updated.
