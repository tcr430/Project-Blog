# Title System Note

## What was wrong before
- The article generator effectively accepted one raw title and then only normalized or lightly warned on it.
- That made the site overuse a small set of rigid structures like `How to ...` and `Best ...`.
- SEO checks existed, but they were mostly defensive rather than generative.

## What changed
- Title generation now creates multiple title candidates from different title families.
- Candidates are scored for both SEO usefulness and editorial quality.
- The system now chooses:
  - a display title for the visible H1 and card surfaces
  - an SEO title for the post front matter and title tag path
- The publish step stores both so the post can be more natural on page without sacrificing search clarity.

## Title families
- `direct_guide`
- `outcome_first`
- `insight_led`
- `question_led`
- `comparison`
- `framework_process`
- `problem_mistake_led`
- `beginner_friendly`
- `editorial_analysis`

Different angle types prefer different families. For example:
- `how_to` leans toward direct, framework, and beginner-friendly titles
- `best_options` leans toward comparison and selection-led titles
- `mistakes` leans toward problem-led and question-led titles
- `ideas` leans toward insight-led and editorial-analysis titles

## How titles are scored
Each candidate is scored on:
- keyword/topic coverage
- clarity and length
- truncation risk
- naturalness
- similarity to existing titles
- overused openings
- repeated title skeletons
- fit for the article angle

A separate SEO score slightly favors tighter, clearer search-result phrasing.

## How repetition is prevented
The scorer checks the existing article metadata corpus and penalizes:
- overused openings like `How to` and `Best`
- overused title families
- repeated structural skeletons across the site
- repeated skeletons within the same cluster
- titles too similar to existing titles

## How H1 and SEO title are handled
- `title` in the generated package is now the display title used for the H1 and editorial surfaces.
- `seo_title` is stored separately.
- During publish, front matter `title` uses `seo_title` so the SEO tag path stays search-friendly.
- Front matter also stores `display_title`, and the post layout plus card includes prefer that value for visible titles.
