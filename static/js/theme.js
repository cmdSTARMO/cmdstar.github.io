(function () {
  var storageKey = "cmdstar-theme";
  var root = document.documentElement;

  function getPreferredTheme() {
    var savedTheme = localStorage.getItem(storageKey);
    if (savedTheme === "light" || savedTheme === "dark") {
      return savedTheme;
    }
    if (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) {
      return "dark";
    }
    return "light";
  }

  function syncThemeButtons(theme) {
    var currentTheme = theme || root.getAttribute("data-theme") || getPreferredTheme();
    document.querySelectorAll("[data-theme-toggle]").forEach(function (button) {
      var isDark = currentTheme === "dark";
      button.setAttribute("aria-pressed", String(isDark));
      button.setAttribute("title", isDark ? "切换到白天模式" : "切换到夜晚模式");
      button.setAttribute("aria-label", isDark ? "切换到白天模式" : "切换到夜晚模式");
      var icon = button.querySelector("[data-theme-icon]");
      if (icon) {
        icon.textContent = isDark ? "☀" : "☾";
      }
    });
  }

  function parseRgbColor(value) {
    if (!value || value === "transparent") {
      return null;
    }

    var namedColors = {
      black: { r: 0, g: 0, b: 0, a: 1 },
      white: { r: 255, g: 255, b: 255, a: 1 }
    };
    var normalizedValue = value.trim().toLowerCase();
    if (namedColors[normalizedValue]) {
      return namedColors[normalizedValue];
    }

    var hex = value.match(/#([0-9a-f]{3}|[0-9a-f]{6})/i);
    if (hex) {
      var raw = hex[1];
      if (raw.length === 3) {
        raw = raw.split("").map(function (char) { return char + char; }).join("");
      }
      return {
        r: parseInt(raw.slice(0, 2), 16),
        g: parseInt(raw.slice(2, 4), 16),
        b: parseInt(raw.slice(4, 6), 16),
        a: 1
      };
    }

    var rgb = value.match(/rgba?\(([^)]+)\)/i);
    if (!rgb) {
      return null;
    }
    var parts = rgb[1].split(",").map(function (part) { return part.trim(); });
    return {
      r: Number(parts[0]),
      g: Number(parts[1]),
      b: Number(parts[2]),
      a: parts.length > 3 ? Number(parts[3]) : 1
    };
  }

  function getLuminance(color) {
    function channel(value) {
      var normalized = value / 255;
      return normalized <= 0.03928
        ? normalized / 12.92
        : Math.pow((normalized + 0.055) / 1.055, 2.4);
    }
    return 0.2126 * channel(color.r) + 0.7152 * channel(color.g) + 0.0722 * channel(color.b);
  }

  function markPreservedSurfaces() {
    var candidates = document.querySelectorAll(
      ".bg-bottom, .bg-black, .bg-dark, .text-bg-dark, [style*='background'], [style*='background-color']"
    );

    candidates.forEach(function (element) {
      if (element.closest("#mainNav, .top-section, footer, .bg-bottom") || element.matches(".bg-bottom, .bg-black, .bg-dark, .text-bg-dark")) {
        element.classList.add("theme-preserve-surface");
        return;
      }

      var inlineBg = element.style.backgroundColor || element.style.background;
      var color = parseRgbColor(inlineBg);
      if (!color) {
        return;
      }

      if (color.a === 0) {
        return;
      }

      if (getLuminance(color) < 0.28) {
        element.classList.add("theme-preserve-surface");
      }
    });
  }

  function watchDynamicSurfaces() {
    if (!window.MutationObserver || !document.body) {
      return;
    }

    var scheduled = false;
    var observer = new MutationObserver(function (mutations) {
      var hasElementChanges = mutations.some(function (mutation) {
        return Array.prototype.some.call(mutation.addedNodes, function (node) {
          return node.nodeType === 1;
        });
      });

      if (!hasElementChanges || scheduled) {
        return;
      }

      scheduled = true;
      window.requestAnimationFrame(function () {
        markPreservedSurfaces();
        scheduled = false;
      });
    });

    observer.observe(document.body, {
      childList: true,
      subtree: true
    });
  }

  function applyTheme(theme, persist) {
    root.classList.add("theme-switching");
    root.setAttribute("data-theme", theme);
    root.style.colorScheme = theme;
    if (persist) {
      localStorage.setItem(storageKey, theme);
    }
    syncThemeButtons(theme);

    window.requestAnimationFrame(function () {
      window.requestAnimationFrame(function () {
        root.classList.remove("theme-switching");
      });
    });
  }

  function toggleTheme() {
    var currentTheme = root.getAttribute("data-theme") || getPreferredTheme();
    applyTheme(currentTheme === "dark" ? "light" : "dark", true);
  }

  applyTheme(getPreferredTheme(), false);

  document.addEventListener("click", function (event) {
    var button = event.target.closest("[data-theme-toggle]");
    if (button) {
      toggleTheme();
    }
  });

  document.addEventListener("DOMContentLoaded", function () {
    markPreservedSurfaces();
    watchDynamicSurfaces();
    syncThemeButtons();
  });

  if (window.matchMedia) {
    var darkSchemeQuery = window.matchMedia("(prefers-color-scheme: dark)");
    var handleSchemeChange = function (event) {
      if (!localStorage.getItem(storageKey)) {
        applyTheme(event.matches ? "dark" : "light", false);
      }
    };
    if (typeof darkSchemeQuery.addEventListener === "function") {
      darkSchemeQuery.addEventListener("change", handleSchemeChange);
    } else if (typeof darkSchemeQuery.addListener === "function") {
      darkSchemeQuery.addListener(handleSchemeChange);
    }
  }

  window.cmdstarTheme = {
    apply: applyTheme,
    toggle: toggleTheme,
    markPreservedSurfaces: markPreservedSurfaces,
    syncButtons: syncThemeButtons
  };
})();
