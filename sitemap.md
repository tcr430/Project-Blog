---
layout: page
title: Sitemap
permalink: /sitemap/
description: Browse all main pages and published posts from The Livin' Edit in one place.
---

<section class="sitemap-section">
  <h2>Home</h2>
  <ul class="sitemap-list">
    <li><a href="{{ '/' | relative_url }}">The Livin' Edit homepage</a></li>
  </ul>
</section>

<section class="sitemap-section">
  <h2>Main Pages</h2>
  <ul class="sitemap-list">
    <li><a href="{{ '/' | relative_url }}">Home</a></li>
    <li><a href="{{ '/stories/' | relative_url }}">Posts</a></li>
    <li><a href="{{ '/authors/' | relative_url }}">Authors</a></li>
    <li><a href="{{ '/contact/' | relative_url }}">Contact</a></li>
    <li><a href="{{ '/privacy/' | relative_url }}">Privacy</a></li>
    <li><a href="{{ '/sitemap/' | relative_url }}">Sitemap</a></li>
  </ul>
</section>

<section class="sitemap-section">
  <h2>Posts</h2>
  <ul class="sitemap-list sitemap-post-list">
    {% for post in site.posts %}
      <li>
        <a href="{{ post.url | relative_url }}">{{ post.title }}</a>
        <span class="sitemap-date">{{ post.date | date: "%b %Y" }}</span>
      </li>
    {% endfor %}
  </ul>
</section>
