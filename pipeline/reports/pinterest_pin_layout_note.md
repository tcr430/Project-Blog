## Pinterest Pin Layout System Note

### Why the previous outputs still looked unformatted

The earlier renderer was already keeping text inside the card, but it still judged layouts too mechanically. A render could pass even when:

- the headline was too large relative to the card
- line breaks were visually clumsy
- the subtitle was too weak against the title
- the top label crowded the headline
- the card felt top-heavy or auto-packed instead of composed

So the problem was no longer overflow alone. It was composition quality.

### How visual quality is evaluated now

The renderer now scores layouts on more than simple fit. It evaluates:

- headline line-balance quality
- whether the headline block is too dominant for the card
- headline-to-subtitle hierarchy ratio
- dropped or truncated supporting copy
- vertical density and dead space
- whether the selected composition mode is appropriate for the content density

A render is only accepted if it clears the quality threshold after those checks.

### How title wrapping and sizing were improved

Headline fitting no longer just picks the largest font that technically fits.

It now:

- measures text in real pixel space
- tries a range of font sizes
- rebalances wrapped lines to reduce awkward width jumps between lines
- scores candidate layouts for line balance and comfort inside the card
- chooses the best-fitting headline layout, not merely the biggest one

This means longer titles are allowed to render smaller when that creates a cleaner composition.

### Composition classes

The system now selects composition classes based on copy density and template context, including modes such as:

- `spacious_editorial`
- `dense_title_safe`
- `minimal_copy`
- `long_title_premium`
- `image_forward`

These classes control whether the top label appears, how strongly the headline is scaled, how much space is left between text blocks, and how aggressive the subtitle support should be.

### How this prevents technically-valid but visually-bad pins

The renderer now rejects layouts that merely fit but still look weak, such as:

- cramped title stacks
- oversized headlines in shallow cards
- weak subtitle hierarchy
- crowded top-label areas
- long-title cards that feel top-heavy

If a candidate template produces that kind of result, the renderer tries safer alternatives before saving the output.
