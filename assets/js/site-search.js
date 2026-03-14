(function () {
  const doc = document;
  const html = doc.documentElement;
  const siteRoot = normalizeSiteRoot(html.dataset.siteRoot || "/");
  const pagefindCssUrl = buildAssetUrl("pagefind/pagefind-ui.css");
  const pagefindJsUrl = buildAssetUrl("pagefind/pagefind-ui.js");
  const modal = doc.querySelector("[data-search-modal]");
  const triggers = Array.from(doc.querySelectorAll("[data-search-trigger]"));
  const pageSurface = doc.querySelector("[data-search-page] [data-search-surface]");
  let lastTrigger = null;
  let pagefindLoader = null;

  function normalizeSiteRoot(root) {
    if (!root || root === "/") {
      return "/";
    }
    return root.endsWith("/") ? root : root + "/";
  }

  function buildAssetUrl(relativePath) {
    return siteRoot + relativePath.replace(/^\/+/, "");
  }

  function loadStylesheet(url) {
    if (doc.querySelector(`link[data-search-pagefind-css="${url}"]`)) {
      return;
    }
    const link = doc.createElement("link");
    link.rel = "stylesheet";
    link.href = url;
    link.dataset.searchPagefindCss = url;
    doc.head.appendChild(link);
  }

  function loadScript(url) {
    return new Promise((resolve, reject) => {
      const existing = doc.querySelector(`script[data-search-pagefind-js="${url}"]`);
      if (existing) {
        if (window.PagefindUI) {
          resolve(window.PagefindUI);
          return;
        }
        existing.addEventListener("load", () => resolve(window.PagefindUI), { once: true });
        existing.addEventListener("error", () => reject(new Error("Pagefind script failed to load.")), { once: true });
        return;
      }

      const script = doc.createElement("script");
      script.src = url;
      script.async = true;
      script.dataset.searchPagefindJs = url;
      script.addEventListener("load", () => resolve(window.PagefindUI), { once: true });
      script.addEventListener("error", () => reject(new Error("Pagefind script failed to load.")), { once: true });
      doc.head.appendChild(script);
    });
  }

  function loadPagefind() {
    if (window.PagefindUI) {
      return Promise.resolve(window.PagefindUI);
    }
    if (!pagefindLoader) {
      loadStylesheet(pagefindCssUrl);
      pagefindLoader = loadScript(pagefindJsUrl);
    }
    return pagefindLoader;
  }

  function initSearchSurface(surface) {
    if (!surface || surface.dataset.searchReady === "true") {
      return Promise.resolve();
    }

    return loadPagefind().then((PagefindUI) => {
      if (!PagefindUI) {
        throw new Error("Pagefind UI is unavailable.");
      }
      new PagefindUI({
        element: surface,
        bundlePath: buildAssetUrl("pagefind/"),
        showSubResults: false,
        excerptLength: 24,
        resetStyles: false,
        translations: {
          placeholder: "Search posts, pages, rooms, materials, and trends",
        },
      });
      surface.dataset.searchReady = "true";
    });
  }

  function getFocusableElements(container) {
    return Array.from(
      container.querySelectorAll(
        'a[href], button:not([disabled]), textarea, input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])'
      )
    ).filter((element) => !element.hasAttribute("hidden") && element.getAttribute("aria-hidden") !== "true");
  }

  function focusSearchInput(container) {
    window.setTimeout(() => {
      const input = container.querySelector("input[type='search'], .pagefind-ui__search-input");
      if (input) {
        input.focus();
      }
    }, 60);
  }

  function openModal(trigger) {
    if (!modal) {
      if (trigger && trigger.href) {
        window.location.href = trigger.href;
      }
      return;
    }

    const surface = modal.querySelector("[data-search-surface]");
    lastTrigger = trigger || null;
    modal.hidden = false;
    doc.body.classList.add("search-open");
    initSearchSurface(surface)
      .then(() => focusSearchInput(modal))
      .catch(() => {
        if (trigger && trigger.href) {
          window.location.href = trigger.href;
        }
      });
  }

  function closeModal() {
    if (!modal || modal.hidden) {
      return;
    }
    modal.hidden = true;
    doc.body.classList.remove("search-open");
    if (lastTrigger) {
      lastTrigger.focus();
    }
  }

  triggers.forEach((trigger) => {
    trigger.addEventListener("click", (event) => {
      event.preventDefault();
      openModal(trigger);
    });
  });

  if (modal) {
    modal.addEventListener("click", (event) => {
      if (event.target instanceof HTMLElement && event.target.hasAttribute("data-search-close")) {
        closeModal();
      }
    });

    modal.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeModal();
        return;
      }
      if (event.key !== "Tab") {
        return;
      }
      const focusable = getFocusableElements(modal);
      if (!focusable.length) {
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && doc.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && doc.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    });
  }

  if (pageSurface) {
    initSearchSurface(pageSurface).catch(() => {
      pageSurface.innerHTML = '<p class="search-error">Search is still building for this deployment. Please refresh in a moment.</p>';
    });
  }
})();
