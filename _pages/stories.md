---
layout: default
title: Posts
permalink: /stories/
description: Browse the latest decor posts, styling guides, and trend-led editorial pieces from The Livin' Edit.
---
{% assign featured_popular_posts = site.posts | where: 'featured', true %}
{% assign popular_posts = featured_popular_posts %}
{% if popular_posts.size == 0 %}
  {% assign popular_posts = site.posts | slice: 2, 4 %}
{% endif %}
<section class="container page-shell stories-shell">
  <div class="section-heading page-heading stories-heading">
    <div>
      <h1>{{ page.title }}</h1>
    </div>
  </div>
  <div class="stories-layout">
    <div class="stories-main">
      <div class="stories-grid">
        {% for post in site.posts %}
          {% include story_card.html post=post %}
        {% endfor %}
      </div>
    </div>
    <aside class="stories-sidebar" aria-label="Posts sidebar">
      <section class="sidebar-panel stories-sidebar-panel">
        <div class="panel-head">
          <h2>Popular</h2>
        </div>
        <div class="popular-list">
          {% for post in popular_posts limit: 4 %}
            {% assign popular_rank = forloop.index %}
            {% include popular_item.html post=post index=popular_rank %}
          {% endfor %}
        </div>
      </section>
      <section class="sidebar-panel stories-sidebar-panel newsletter-sidebar-card">
        <div class="panel-head">
          <p class="eyebrow-label">Stay Updated</p>
          <h2>Subscribe</h2>
        </div>
        <p>Get fresh decor stories, trend notes, and styling ideas delivered when new editorial pieces go live.</p>
        {% include newsletter.html variant="sidebar" %}
      </section>
    </aside>
  </div>
</section>