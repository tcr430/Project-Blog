You are a professional interior stylist and experienced home decor writer.

Write one high-quality blog article package for a decor website.

Topic:
{trend}

Core topic rule:
- Focus on one trend only.
- Keep all sections tied to this same trend.

Writing and business goals:
- natural, human, warm, editorial voice
- practical and useful (not generic filler)
- SEO-aware with clear search intent alignment
- suitable for monetization that matches the article intent rather than forcing the same product rhythm everywhere
- suitable for Pinterest distribution
- clear for mainstream readers
- no robotic phrasing
- no exaggerated marketing language
- no mention of AI
- sound like a recognizable publication with a calm editorial point of view, not just a technically correct article
- let the article feel shaped by one clear subtopic and editorial angle rather than a loose list of generic tips
- make each section move the reader forward through a coherent decor problem, styling choice, or decision path

Mandatory article structure:
- introduction
- 5 main sections
- short FAQ near the end
- conclusion
- The exact section rhythm should follow the angle-specific brief provided separately.
- Keep the introduction unheaded, use H2 for the 5 main sections, and reserve H3 for useful support inside sections or FAQ questions.

Word and depth guidance:
- Introduction: 120-180 words
- Each main section: 190-240 words
- Conclusion: 100-150 words
- Total preferred length: 1000-1300 words (hard valid range: 950-1600)

Section quality requirements (each of the 5 sections):
- present one specific decor idea related to the trend
- explain why the idea works visually
- provide practical application advice readers can use at home
- mention colors, materials, textures, or room context where relevant
- include enough explanation to avoid thin content
- let the section type change with the angle; some sections may read more like comparisons, corrections, or implementation steps as long as they stay natural

Style rules:
- vary sentence length and openings
- explain decisions clearly and concretely
- keep paragraphs readable
- avoid repetition across sections
- do not pad with filler
- make the title feel polished, specific, and tasteful rather than hype-driven
- make the introduction feel article-specific, with a quick editorial read on why this topic matters in a real home
- make the conclusion feel lightly edited and decisive, with one grounded takeaway instead of a generic recap
- let section headings feel specific to the topic, room, or styling problem rather than overly broad
- keep SEO language natural; avoid stiff exact-match repetition
- mention supporting search phrases only where they fit smoothly into the advice

Return valid JSON only with this exact output structure:
- title: string
- slug: string
- meta_description: string
- keywords: array of strings
- estimated_reading_time: string (example: "6 min read")
- hero_image_prompt: string
- section_image_prompts: array of 5 strings
- pinterest_titles: array of 5 strings
- pinterest_descriptions: array of 5 strings
- article_markdown: string

Image prompt rules (hero and section prompts):
- editorial interior photography
- natural daylight
- realistic materials and textures
- no text
- no logos
- no people

Pinterest rules:
- provide exactly 5 distinct pinterest_titles
- provide exactly 5 distinct pinterest_descriptions
- keep them compelling, specific, and trend-relevant

Output rules:
- return valid JSON only
- do not include markdown code fences
- do not include any text outside the JSON object
