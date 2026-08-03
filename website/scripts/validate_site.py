#!/usr/bin/env python3
"""Dependency-free validation for the Agent World static marketing site."""

from __future__ import annotations

import gzip
import json
import re
import struct
import sys
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "index.html"
CSS = ROOT / "styles.css"
CAPTURE_GATE = ROOT / "CAPTURE_GATE.md"
CANONICAL = "https://viseriontarg.github.io/agent-world/"
SOCIAL_URL = f"{CANONICAL}assets/social-preview.png"

errors: list[str] = []


def fail(message: str) -> None:
    errors.append(message)


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


class SiteAudit(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.ids: set[str] = set()
        self.duplicate_ids: set[str] = set()
        self.fragment_links: list[str] = []
        self.local_refs: list[tuple[str, str]] = []
        self.label_refs: list[tuple[str, str]] = []
        self.headings: list[int] = []
        self.images: list[dict[str, str | None]] = []
        self.links: list[dict[str, str | None]] = []
        self.summary_aria_labels: list[str] = []
        self.metas_by_name: dict[str, str] = {}
        self.metas_by_property: dict[str, str] = {}
        self.link_rels: dict[str, str] = {}
        self.script_types: list[tuple[str | None, str | None]] = []
        self.landmarks = {"header": 0, "main": 0, "footer": 0, "nav": 0}
        self.lang: str | None = None
        self.first_anchor_href: str | None = None
        self.text: list[str] = []
        self.event_attributes: list[str] = []

    def handle_starttag(self, tag: str, attrs_list: list[tuple[str, str | None]]) -> None:
        attrs = dict(attrs_list)
        if tag == "html":
            self.lang = attrs.get("lang")
        if tag in self.landmarks:
            self.landmarks[tag] += 1
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self.headings.append(int(tag[1]))

        element_id = attrs.get("id")
        if element_id:
            if element_id in self.ids:
                self.duplicate_ids.add(element_id)
            self.ids.add(element_id)

        for attribute in ("aria-labelledby", "aria-describedby"):
            value = attrs.get(attribute)
            if value:
                for target in value.split():
                    self.label_refs.append((attribute, target))

        for name in attrs:
            if name.lower().startswith("on"):
                self.event_attributes.append(f"<{tag} {name}>")

        if tag == "a":
            self.links.append(attrs)
            href = attrs.get("href")
            if self.first_anchor_href is None:
                self.first_anchor_href = href
            if href:
                if href.startswith("#"):
                    self.fragment_links.append(href[1:])
                self.local_refs.append(("href", href))

        if tag == "summary" and attrs.get("aria-label"):
            self.summary_aria_labels.append(attrs["aria-label"] or "")

        if tag == "img":
            self.images.append(attrs)
            src = attrs.get("src")
            if src:
                self.local_refs.append(("src", src))

        if tag == "link":
            href = attrs.get("href")
            rel = attrs.get("rel", "") or ""
            if href:
                self.local_refs.append(("href", href))
                for token in rel.split():
                    self.link_rels[token] = href

        if tag == "meta":
            content = attrs.get("content", "") or ""
            if attrs.get("name"):
                self.metas_by_name[attrs["name"] or ""] = content
            if attrs.get("property"):
                self.metas_by_property[attrs["property"] or ""] = content

        if tag == "script":
            script_type = attrs.get("type")
            src = attrs.get("src")
            self.script_types.append((script_type, src))
            if src:
                self.local_refs.append(("src", src))

    def handle_data(self, data: str) -> None:
        value = data.strip()
        if value:
            self.text.append(value)


def local_target(ref: str) -> Path | None:
    parsed = urlsplit(ref)
    if parsed.scheme or parsed.netloc or ref.startswith("#"):
        return None
    if ref.startswith("/"):
        fail(f"Root-relative reference breaks project Pages paths: {ref}")
        return None
    relative_path = unquote(parsed.path)
    if not relative_path:
        return None
    target = (ROOT / relative_path).resolve()
    try:
        target.relative_to(ROOT.resolve())
    except ValueError:
        fail(f"Reference escapes the published website directory: {ref}")
        return None
    if target.is_dir():
        target = target / "index.html"
    return target


def channel(value: int) -> float:
    normalized = value / 255
    return normalized / 12.92 if normalized <= 0.04045 else ((normalized + 0.055) / 1.055) ** 2.4


def luminance(hex_color: str) -> float:
    value = hex_color.removeprefix("#")
    red, green, blue = (int(value[index:index + 2], 16) for index in (0, 2, 4))
    return 0.2126 * channel(red) + 0.7152 * channel(green) + 0.0722 * channel(blue)


def contrast(first: str, second: str) -> float:
    bright, dark = sorted((luminance(first), luminance(second)), reverse=True)
    return (bright + 0.05) / (dark + 0.05)


def png_size(path: Path) -> tuple[int, int] | None:
    try:
        with path.open("rb") as image:
            signature = image.read(24)
    except FileNotFoundError:
        return None
    if len(signature) != 24 or signature[:8] != b"\x89PNG\r\n\x1a\n" or signature[12:16] != b"IHDR":
        fail(f"{path.relative_to(ROOT)} is not a valid PNG with an IHDR header")
        return None
    return struct.unpack(">II", signature[16:24])


for required in (
    INDEX,
    CSS,
    ROOT / "assets/favicon.svg",
    ROOT / "assets/social-preview.png",
    ROOT / "assets/fonts/bebas-neue-latin.woff2",
    ROOT / "assets/fonts/source-serif-4-latin.woff2",
    ROOT / "assets/fonts/ibm-plex-mono-regular-latin.woff2",
    ROOT / "assets/fonts/ibm-plex-mono-semibold-latin.woff2",
    ROOT / "assets/fonts/README.md",
    ROOT / "site.webmanifest",
    ROOT / "robots.txt",
    ROOT / "sitemap.xml",
    CAPTURE_GATE,
):
    require(required.exists(), f"Missing required site file: {required.relative_to(ROOT)}")

if INDEX.exists():
    html = INDEX.read_text(encoding="utf-8")
    audit = SiteAudit()
    audit.feed(html)

    require(audit.lang == "en", "The root <html> element must declare lang=\"en\"")
    require(audit.headings.count(1) == 1, "The page must contain exactly one <h1>")
    for previous, current in zip(audit.headings, audit.headings[1:]):
        require(current <= previous + 1, f"Heading order skips from h{previous} to h{current}")
    require(not audit.duplicate_ids, f"Duplicate ids: {sorted(audit.duplicate_ids)}")
    require(audit.first_anchor_href == "#main-content", "The skip link must be the first anchor")
    require(audit.landmarks["header"] >= 1, "A page header landmark is required")
    require(audit.landmarks["main"] == 1, "Exactly one main landmark is required")
    require(audit.landmarks["footer"] >= 1, "A page footer landmark is required")
    require(audit.landmarks["nav"] >= 2, "Primary and footer navigation landmarks are required")
    require(
        not audit.summary_aria_labels,
        "Native summary controls must use their visible text as the accessible name",
    )
    require(not audit.event_attributes, f"Inline event handlers are not allowed: {audit.event_attributes}")

    for fragment in audit.fragment_links:
        require(fragment in audit.ids, f"Anchor points to missing id: #{fragment}")
    for attribute, target in audit.label_refs:
        require(target in audit.ids, f"{attribute} points to missing id: {target}")

    for kind, ref in audit.local_refs:
        target = local_target(ref)
        if target is not None:
            require(target.exists(), f"Broken local {kind}: {ref}")

    for image in audit.images:
        require("alt" in image, f"Image is missing alt text: {image.get('src')}")
        require(bool(image.get("width")) and bool(image.get("height")), f"Image lacks intrinsic dimensions: {image.get('src')}")

    for link in audit.links:
        if link.get("target") == "_blank":
            rel_tokens = set((link.get("rel") or "").split())
            require({"noopener", "noreferrer"}.issubset(rel_tokens), f"New-tab link lacks noopener/noreferrer: {link.get('href')}")

    require(audit.metas_by_name.get("viewport") == "width=device-width, initial-scale=1", "Viewport metadata is missing or incorrect")
    require(bool(audit.metas_by_name.get("description")), "A meta description is required")
    require(audit.metas_by_name.get("twitter:card") == "summary_large_image", "Twitter large-card metadata is required")
    require(audit.metas_by_name.get("twitter:image") == SOCIAL_URL, "Twitter image must use the published PNG")
    require(audit.metas_by_property.get("og:type") == "website", "Open Graph type must be website")
    require(audit.metas_by_property.get("og:url") == CANONICAL, "Open Graph URL must match the canonical URL")
    require(audit.metas_by_property.get("og:image") == SOCIAL_URL, "Open Graph image must use the published PNG")
    require(audit.link_rels.get("canonical") == CANONICAL, "Canonical URL is missing or incorrect")
    require(audit.link_rels.get("stylesheet") == "./styles.css", "The local stylesheet must be linked with a project-safe path")
    require(audit.link_rels.get("manifest") == "./site.webmanifest", "The web manifest must be linked")

    executable_scripts = [item for item in audit.script_types if item[0] != "application/ld+json"]
    require(not executable_scripts, f"The first slice must not require executable JavaScript: {executable_scripts}")
    require(all(src is None for _, src in audit.script_types), "External or local script sources are not allowed")

    normalized_text = " ".join(" ".join(audit.text).split())
    for required_phrase in (
        "Control the work. Keep the evidence.",
        "Event ledger",
        "There is no signed release yet.",
        "not a product screenshot or an authenticated turn record.",
        "Durable before action.",
        "at most one active turn globally",
        "Exactly Codex CLI 0.146.0",
        "Broad read, no write.",
        "No authenticated model turn—or authoritative real-Windows sandbox, read-scope, network, process-tree, or reopened-SQLite proof—has been run for promotion.",
        "the prompt and model-selected readable context go to the configured Codex service",
        "apply_patch may remain model-visible",
        "Claude, streaming, approvals, interrupt, resume, fork, edit workflow, review, merge, signed install, and updates remain gated.",
    ):
        require(required_phrase in normalized_text, f"Missing claim-boundary copy: {required_phrase}")

    require("docs/assets/agent-world-ui.png" not in html, "The stale spatial screenshot must not be referenced by the site")
    require("Live provider turns are the next proof gate." not in normalized_text, "The pre-live source boundary is stale")
    require("admits one Codex prompt globally" not in normalized_text, "Global turn admission must be described as a concurrency limit")
    require("does not start a model turn" not in normalized_text, "The site must not claim all live turns are absent")
    require("Download" not in normalized_text, "A download CTA is not allowed before a signed release exists")
    require("http://" not in html, "Insecure HTTP references are not allowed")
    require("googletagmanager" not in html.lower() and "analytics" not in html.lower(), "Trackers are not allowed in the first site slice")

if CAPTURE_GATE.exists():
    capture_gate = CAPTURE_GATE.read_text(encoding="utf-8")
    for required_phrase in (
        "one globally admitted, final-result-only Codex path",
        "This is a source-and-fixture claim.",
        "No authenticated model turn or authoritative real-Windows sandbox, read-scope, network, process-tree, or reopened-SQLite evidence bundle has been run for promotion.",
        "not a worktree-only read or host-secret boundary",
        "the prompt and model-selected readable context go to the configured Codex service",
        "apply_patch` may remain model-visible",
        "Claude, streaming, approvals, interrupt, resume, fork, and edit authority remain gated",
    ):
        require(required_phrase in capture_gate, f"Capture gate is missing claim-boundary copy: {required_phrase}")
    require("does not start a model turn" not in capture_gate, "The capture gate must not claim all live turns are absent")

if CSS.exists():
    css = CSS.read_text(encoding="utf-8")
    for required_pattern in (
        ":focus-visible",
        "min-height: 44px",
        "minmax(0, 1fr)",
        "min-width: 0",
        "prefers-reduced-motion: reduce",
        "forced-colors: active",
    ):
        require(required_pattern in css, f"Required responsive/accessibility rule is missing: {required_pattern}")
    require("http://" not in css and "https://" not in css, "CSS must not make remote font or asset requests")
    font_urls = re.findall(r"url\([\"']?(?P<path>[^\"')]+)", css)
    for font_url in font_urls:
        require(font_url.startswith("./assets/fonts/"), f"CSS asset must be a bundled font: {font_url}")
        target = local_target(font_url)
        if target is not None:
            require(target.exists(), f"Bundled CSS font is missing: {font_url}")

    root_block = re.search(r":root\s*\{(?P<body>.*?)\n\}", css, re.DOTALL)
    require(root_block is not None, "CSS must expose canonical semantic tokens in :root")
    tokens: dict[str, str] = {}
    if root_block:
        tokens = dict(re.findall(r"--([a-z-]+):\s*(#[0-9a-fA-F]{6})", root_block.group("body")))
    expected_tokens = {
        "paper": "#f9f8f3",
        "ink": "#0d131a",
        "ink-soft": "#343b3f",
        "accent": "#0b6865",
        "accent-bright": "#65d7c4",
        "night": "#09111a",
        "night-text": "#f4f0e8",
        "night-muted": "#c4c4bd",
        "focus": "#006fbd",
    }
    for name, expected in expected_tokens.items():
        require(tokens.get(name, "").lower() == expected, f"Canonical token --{name} must be {expected}")

    contrast_pairs = (
        ("ink", "paper"),
        ("ink-soft", "paper"),
        ("accent", "paper"),
        ("night-text", "night"),
        ("night-muted", "night"),
        ("accent-bright", "night"),
    )
    for foreground, background in contrast_pairs:
        if foreground in tokens and background in tokens:
            ratio = contrast(tokens[foreground], tokens[background])
            require(ratio >= 4.5, f"Token contrast {foreground}/{background} is only {ratio:.2f}:1")

manifest_path = ROOT / "site.webmanifest"
if manifest_path.exists():
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        fail(f"Invalid site.webmanifest: {error}")
    else:
        require(manifest.get("start_url") == "./", "Manifest start_url must be project-path safe")
        require(manifest.get("scope") == "./", "Manifest scope must be project-path safe")
        for icon in manifest.get("icons", []):
            target = local_target(icon.get("src", ""))
            if target is not None:
                require(target.exists(), f"Manifest icon is missing: {icon.get('src')}")

sitemap_path = ROOT / "sitemap.xml"
if sitemap_path.exists():
    try:
        sitemap = ET.parse(sitemap_path)
    except ET.ParseError as error:
        fail(f"Invalid sitemap.xml: {error}")
    else:
        locations = [element.text for element in sitemap.iter() if element.tag.endswith("loc")]
        require(CANONICAL in locations, "Sitemap must contain the canonical homepage")

robots_path = ROOT / "robots.txt"
if robots_path.exists():
    robots = robots_path.read_text(encoding="utf-8")
    require("User-agent: *" in robots and "Allow: /" in robots, "robots.txt must allow indexing")
    require(f"Sitemap: {CANONICAL}sitemap.xml" in robots, "robots.txt must reference the canonical sitemap")

social_path = ROOT / "assets/social-preview.png"
dimensions = png_size(social_path)
if dimensions is not None:
    require(dimensions == (1280, 640), f"Social preview must be 1280x640, got {dimensions[0]}x{dimensions[1]}")
    require(social_path.stat().st_size <= 300_000, f"Social preview exceeds 300 KB: {social_path.stat().st_size} bytes")

font_files = list((ROOT / "assets/fonts").glob("*.woff2"))
font_bytes = sum(path.stat().st_size for path in font_files)
require(len(font_files) == 4, f"Expected four bundled Latin font files, got {len(font_files)}")
require(font_bytes <= 230_000, f"Bundled font payload exceeds 230 KB: {font_bytes} bytes")

if INDEX.exists() and CSS.exists():
    html_bytes = INDEX.read_bytes()
    css_bytes = CSS.read_bytes()
    require(len(html_bytes) <= 45_000, f"index.html exceeds 45 KB: {len(html_bytes)} bytes")
    require(len(css_bytes) <= 40_000, f"styles.css exceeds 40 KB: {len(css_bytes)} bytes")
    initial_gzip = len(gzip.compress(html_bytes)) + len(gzip.compress(css_bytes))
    require(initial_gzip <= 35_000, f"HTML + CSS exceed the 35 KB gzip budget: {initial_gzip} bytes")
else:
    initial_gzip = 0

javascript_files = list(ROOT.rglob("*.js"))
require(not javascript_files, f"Executable JavaScript is not needed in this slice: {[str(path.relative_to(ROOT)) for path in javascript_files]}")

if errors:
    print("Agent World site validation failed:")
    for error in errors:
        print(f"  - {error}")
    sys.exit(1)

social_bytes = social_path.stat().st_size if social_path.exists() else 0
print("Agent World site validation passed")
print(f"  HTML: {INDEX.stat().st_size} bytes")
print(f"  CSS: {CSS.stat().st_size} bytes")
print(f"  HTML + CSS gzip: {initial_gzip} bytes")
print(f"  Social preview: {social_bytes} bytes at 1280x640")
print(f"  Bundled fonts: {font_bytes} bytes")
print("  JavaScript: 0 bytes")
