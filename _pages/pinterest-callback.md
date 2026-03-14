---
layout: page
title: Pinterest Connection
permalink: /pinterest/callback/
noindex: true
sitemap: false
---

<section class="page-body simple-message">
  <p class="eyebrow-label">Pinterest</p>
  <h1>Authorization received.</h1>
  <p>Pinterest should redirect you here after you approve the app.</p>
  <p>To finish the OAuth setup, copy the full URL from your browser address bar and extract the <code>code</code> value from the query string.</p>
  <p>Then run:</p>
</section>

```powershell
python pipeline/scripts/pinterest_oauth.py exchange-code --code YOUR_CODE
```

<section class="page-body simple-message">
  <p>If Pinterest returned an error instead, the address bar will include an <code>error</code> value you can use to debug the app configuration.</p>
  <p><a href="{{ '/' | relative_url }}">Return to the homepage</a></p>
</section>
