# Pinterest Pin System Note

## What was wrong before
- The old pin renderer relied on a few hardcoded card layouts with weak hierarchy.
- Pin copy leaned too heavily on article titles and generic descriptions.
- Variation was shallow, so many pins felt repetitive.
- The renderer had no meaningful quality gate beyond whether an image file was produced.

## What changed
- Pin rendering now uses a reusable design system from `pipeline/data/pinterest_pin_design_system.json`.
- Metadata generation now creates Pinterest-specific display copy: headline, subheadline, kicker, and CTA label.
- The renderer uses template families rather than a single generic visual pattern.
- Each render gets quality diagnostics, and the renderer can fall back to another template family when a layout scores poorly.

## Template families
- `editorial_split`: strong magazine-like lower panel with clear value-driven copy.
- `minimal_frame`: image-led premium layout with a deep readability band.
- `utility_stack`: more structured tutorial layout with stronger instructional clarity.
- `comparison_panel`: decision-support layout for recommendation and comparison content.
- `insight_band`: top-loaded editorial layout for sharper, insight-oriented framing.

## How templates are selected
- Variant planning still starts from the existing Pinterest strategy layer.
- `generate_pin_metadata.py` assigns a preferred `template_family` based on variant type.
- `generate_pin_assets.py` tries the preferred template first, then falls back through allowed templates for that variant if quality is weak.

## How headline adaptation works
- Pin headlines are no longer just copied from the article title.
- The system derives topic-aware copy from article title, keyword, cluster, angle, and intent.
- Variants are framed differently for evergreen, utility, decision, and curiosity-led pins.
- The copy stays aligned with the article and avoids clickbait.

## How quality validation works
- The renderer checks headline and subheadline fit before accepting a layout.
- It penalizes overflow, overly wide line lengths, and missing hierarchy.
- If the score falls below the threshold, another template family is tried.
- Render diagnostics are written back into the Pinterest metadata for inspection.
