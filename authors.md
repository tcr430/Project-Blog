---
layout: page
title: Authors
permalink: /authors/
---

<p class="page-intro">
  Meet the editorial voices behind the site. Each persona anchors a different design lens, so the generated articles still feel varied, grounded, and recognizably human.
</p>

<div class="author-grid">
  {% for item in site.data.authors %}
    {% assign author_id = item[0] %}
    {% include author_box.html author_id=author_id %}
  {% endfor %}
</div>
