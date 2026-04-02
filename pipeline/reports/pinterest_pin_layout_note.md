## Pinterest Pin Layout System Note

### What was causing the formatting failures

The previous renderer relied on rough character-count wrapping and fixed y-positions. That meant:

- titles were wrapped without measuring real rendered width
- text size was mostly fixed per template
- title, subtitle, and CTA positions did not adapt to actual content height
- layouts could still be accepted even when typography became cramped or unbalanced

As a result, long or dense copy could overflow, compress the card rhythm, or misalign the subtitle/CTA stack.

### How typography fitting works now

The renderer now measures text against the real box width and height before drawing.

For both headline and subheadline it:

- truncates to a hard upper bound only when necessary
- tries font sizes from the template maximum down to a configured minimum
- wraps lines using measured pixel width, not character counts
- respects max line counts and line-height rules from the design system
- accepts a layout only if the full text block fits inside the available box

### How long-title handling works

The system now handles copy density before rendering:

- short copy prefers short-density templates
- medium copy prefers medium-density templates
- long copy prefers long-density templates

There are dedicated long-title-safe template families in the design system:

- `editorial_split_long`
- `utility_stack_long`
- `minimal_frame_long`

The copy layer also tightens pin-facing copy before rendering:

- headlines are shortened by removing weak trailing guide phrases when possible
- subheadlines are reduced to a cleaner first sentence when needed

### How validation prevents bad renders

Every render is scored before output.

Validation checks include:

- headline fits within its box
- subheadline fits within its box
- typography does not fall below premium minimum sizes
- vertical content stack stays inside the safe content area
- layouts with excessive dead space or dropped supporting copy are penalized
- outputs below the quality threshold are rejected

If a candidate template fails, the renderer tries safer template alternatives before saving the pin.
