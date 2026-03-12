---
title: Authors
permalink: /authors/
---

<div class="author-grid">
  {% for item in site.data.authors %}
    {% assign author_id = item[0] %}
    {% include author_card.html author_id=author_id %}
  {% endfor %}
</div>
