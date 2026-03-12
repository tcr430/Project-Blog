---
title: Authors
permalink: /authors/
---

<p class="page-intro">The site is powered by a small editorial cast of decor personas, each with a distinct design lens and writing style.</p>

<div class="author-grid">
  {% for item in site.data.authors %}
    {% assign author_id = item[0] %}
    {% include author_card.html author_id=author_id %}
  {% endfor %}
</div>