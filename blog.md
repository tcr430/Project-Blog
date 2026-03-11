---
layout: page
title: Blog
permalink: /blog/
---

<p class="page-intro">
  Every story published here comes from the same pipeline that generates trend-led decor content, rich metadata, and supporting imagery for the site.
</p>

<div class="story-grid archive-grid">
  {% for post in site.posts %}
    {% include post_card.html post=post %}
  {% endfor %}
</div>
