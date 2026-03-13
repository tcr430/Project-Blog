---
title: Authors
permalink: /authors/
description: Meet the editorial voices behind The Livin' Edit and the perspectives shaping each decor story.
---
<div class="author-grid">
  {% for item in site.data.authors %}
    {% assign author_id = item[0] %}
    {% include author_card.html author_id=author_id %}
  {% endfor %}
</div>