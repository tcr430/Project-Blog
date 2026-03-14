---
layout: page
title: Clusters
permalink: /clusters/
description: Explore topic clusters from The Livin' Edit, including related decor articles grouped into practical hub pages.
---
{% assign cluster_pages = site.pages | where: 'layout', 'cluster' | sort: 'title' %}
<section class="cluster-listing-shell">
  <div class="section-heading page-heading cluster-listing-heading">
    <div>
      <p class="eyebrow-label">Topic Hubs</p>
      <h1>{{ page.title }}</h1>
      <p class="cluster-dek">Browse cluster guides built from related articles, shared search intent, and connected decor themes.</p>
    </div>
  </div>

  {% if cluster_pages.size > 0 %}
  <div class="cluster-listing-grid">
    {% for cluster in cluster_pages %}
    <article class="sidebar-panel cluster-card">
      <div class="panel-head">
        <p class="eyebrow-label">Cluster</p>
        <h2><a href="{{ cluster.url | relative_url }}">{{ cluster.title }}</a></h2>
      </div>
      {% if cluster.description %}
      <p>{{ cluster.description }}</p>
      {% endif %}
      <div class="entry-meta cluster-card-meta">
        {% if cluster.cluster_article_count %}<span>{{ cluster.cluster_article_count }} articles</span>{% endif %}
      </div>
    </article>
    {% endfor %}
  </div>
  {% else %}
  <section class="sidebar-panel cluster-empty-state">
    <div class="panel-head">
      <h2>Cluster guides are coming soon</h2>
    </div>
    <p>New cluster guides will appear here automatically as soon as articles are indexed into a topical cluster.</p>
  </section>
  {% endif %}
</section>
