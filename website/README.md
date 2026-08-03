# Agent World marketing site

This directory is the source for the standalone Agent World product site.
It is intentionally dependency-free: semantic HTML, modular CSS, and a small
vanilla-JavaScript enhancement layer.

## Build locally

```powershell
python website/build.py --output _site
python -m http.server 8000 --directory _site
```

The GitHub Pages workflow copies the existing `docs/` assets and build-in-public
page into `_site`, then builds this homepage over them. Product claims in the page
must remain no stronger than the repository evidence. In particular, the published
resource numbers are labelled as the historical Phase-1 baseline until the current
list-first executable is measured on Windows.
