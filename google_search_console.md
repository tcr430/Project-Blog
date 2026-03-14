# Google Search Console Guide

## 1. Add the verification code

Open `_config.yml` and set:

```yml
google_site_verification: "YOUR_GOOGLE_VERIFICATION_CODE"
```

Use only the code value from Google Search Console, not the full meta tag.

## 2. Deploy the site

After updating the config, deploy the site so the verification meta tag is added to the shared page head.

The tag renders from `_layouts/default.html` as:

```html
<meta name="google-site-verification" content="...">
```

## 3. Verify the site in Google Search Console

1. Open [Google Search Console](https://search.google.com/search-console).
2. Add your property.
3. If you verify with the HTML tag method, copy the verification code.
4. Paste that code into `google_site_verification` in `_config.yml`.
5. Deploy the site.
6. Return to Search Console and click `Verify`.

If you verify the full domain with DNS instead, you can still keep this config field available for URL-prefix verification later.

## 4. Submit the sitemap

This site already uses `jekyll-sitemap`, so Jekyll generates:

- [https://thelivinedit.com/sitemap.xml](https://thelivinedit.com/sitemap.xml)

In Search Console:

1. Open your property.
2. Go to `Sitemaps`.
3. Submit:
   - `https://thelivinedit.com/sitemap.xml`

## 5. robots.txt status

The site already has `robots.txt`, and it:

- allows crawling
- points search engines to the sitemap

## 6. Request indexing for important pages

After publishing important pages, you can speed up discovery by using the `URL inspection` tool in Search Console and clicking `Request indexing` for:

- the homepage
- important posts
- cluster hub pages
- major evergreen pages

## Notes

- `sitemap.xml` is generated automatically during the Jekyll build.
- The verification meta tag only renders when `google_site_verification` is set.
- If the code is blank, nothing extra is added to the page head.
