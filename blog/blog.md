---
layout: page
title: Blog
permalink: /blog/
---

<div class="card-grid">
  {% for post in site.posts %}
    {% include post_card.html post=post %}
  {% endfor %}
</div>
