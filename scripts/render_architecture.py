#!/usr/bin/env python3
"""Rebuild README architecture SVGs and the source-backed feature catalog.

Run from the repository root: python3 scripts/render_architecture.py
Only Python's standard library is required. This does not import or run Core.
Edit docs/architecture/model.json to change a diagram. The Graphify summary is
an input snapshot; the commands in docs/architecture/README.md recreate its source.
"""

import html
import json
import math
import re
import unicodedata
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs" / "architecture"
PAPER, INK, MUTED, ACCENT, LINK = "#f5f5f5", "#2d3142", "#4f5d75", "#bf4520", "#2e5aa8"
KINDS = {"internal": "MODULE", "external": "EXTERNAL", "store": "STORE",
         "gate": "GATE", "foundation": "FOUNDATION"}
FONT = "Arial, Helvetica, sans-serif"
MONO = "'DejaVu Sans Mono', Consolas, monospace"

def esc(value):
    return html.escape(str(value), quote=True)


def number(value):
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("Coordinates must be finite")
    return f"{value:g}"


def slug(value):
    value = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(value)).strip("-")
    if not value:
        raise ValueError("Diagram and node IDs need a nonempty ASCII identifier")
    return value


def text_width(text, size, mono=True):
    units = sum(0 if unicodedata.combining(c) else
                1 if unicodedata.east_asian_width(c) in "WF" else
                .62 if mono else .60 for c in text)
    return units * size


def edge_geometry(raw_points):
    """Keep supplied routes, rounding only orthogonal turns."""
    points = []
    for raw in raw_points:
        point = tuple(float(v) for v in raw)
        if len(point) != 2 or not all(math.isfinite(v) for v in point):
            raise ValueError(f"Invalid edge point: {raw!r}")
        if not points or point != points[-1]:
            points.append(point)
    if len(points) < 2:
        raise ValueError("An edge needs two distinct points")
    for a, b in zip(points, points[1:]):
        if a[0] != b[0] and a[1] != b[1]:
            raise ValueError(f"Diagonal route segment: {a} -> {b}")
    simplified = [points[0]]
    for p, q in zip(points[1:-1], points[2:]):
        a = simplified[-1]
        incoming = (p[0] - a[0], p[1] - a[1])
        outgoing = (q[0] - p[0], q[1] - p[1])
        if incoming[0] * outgoing[1] == incoming[1] * outgoing[0]:
            if incoming[0] * outgoing[0] + incoming[1] * outgoing[1] < 0:
                raise ValueError(f"Route reverses over itself at {p}")
            continue
        simplified.append(p)
    points = simplified + [points[-1]]
    if len(points) == 2:
        a, b = points
        return "line", f'x1="{number(a[0])}" y1="{number(a[1])}" x2="{number(b[0])}" y2="{number(b[1])}"'
    path = [f"M {number(points[0][0])} {number(points[0][1])}"]
    for a, p, b in zip(points, points[1:], points[2:]):
        before = abs(p[0] - a[0]) + abs(p[1] - a[1])
        after = abs(b[0] - p[0]) + abs(b[1] - p[1])
        radius = min(8, before / 2, after / 2)
        if radius < 6:
            raise ValueError(f"Route turn at {p} is too tight for a 6px elbow")
        incoming = ((p[0] - a[0]) / before, (p[1] - a[1]) / before)
        outgoing = ((b[0] - p[0]) / after, (b[1] - p[1]) / after)
        start = (p[0] - radius * incoming[0], p[1] - radius * incoming[1])
        end = (p[0] + radius * outgoing[0], p[1] + radius * outgoing[1])
        sweep = int(incoming[0] * outgoing[1] - incoming[1] * outgoing[0] > 0)
        path += [f"L {number(start[0])} {number(start[1])}",
                 f"A {number(radius)} {number(radius)} 0 0 {sweep} {number(end[0])} {number(end[1])}"]
    path.append(f"L {number(points[-1][0])} {number(points[-1][1])}")
    return "path", f'd="{" ".join(path)}"'


def render_edge(edge, prefix):
    kind = edge.get("kind", "data")
    if kind not in {"data", "api", "conditional"}:
        raise ValueError(f"Unknown edge kind: {kind}")
    color = LINK if kind == "api" else MUTED
    marker = f"{prefix}-arrow-link" if kind == "api" else f"{prefix}-arrow"
    tag, geometry = edge_geometry(edge["points"])
    extra = ' stroke-dasharray="5,4"' if kind == "conditional" else ""
    if edge.get("bidir"):
        extra += f' marker-start="url(#{marker})"'
    result = [f'<{tag} {geometry} fill="none" stroke="{color}" stroke-width="1.5" marker-end="url(#{marker})"{extra}/>']
    if edge.get("label"):
        label = str(edge["label"])
        x, y = float(edge["label_x"]), float(edge["label_y"])
        cjk = any(unicodedata.east_asian_width(c) in "WF" for c in label)
        width = math.ceil((text_width(label, 12, not cjk) + 16) / 4) * 4
        result += [f'<rect x="{number(x-width/2)}" y="{number(y-11)}" width="{width}" height="14" rx="2" fill="{PAPER}"/>',
                   f'<text class="edge-label{" cjk" if cjk else ""}" x="{number(x)}" y="{number(y)}" text-anchor="middle" fill="{color}" font-size="12">{esc(label)}</text>']
    return "\n".join(result)


def render_node(node, prefix):
    x, y, w, h = (float(node[key]) for key in ("x", "y", "w", "h"))
    if not (w > 0 and h > 0 and x >= 0 and y >= 0 and x+w <= 1280 and y+h < 660):
        raise ValueError(f"Node {node['id']} is outside the content area")
    kind = node.get("kind", "internal")
    if kind not in KINDS:
        raise ValueError(f"Unknown node kind: {kind}")
    title = str(node["title"]).split("\n")
    sub = [line for item in node.get("sub", []) for line in str(item).split("\n")]
    text_height = len(title)*20 + len(sub)*17
    if text_height > h-32:
        raise ValueError(f"Node {node['id']} is too short for its text")
    stroke = ACCENT if node.get("accent") else LINK if kind == "external" else MUTED if kind in {"store", "foundation"} else INK
    fill = "#f2e7e3" if node.get("accent") else "#ececee" if kind == "store" else "#f0f0f1" if kind in {"external", "foundation"} else "#ffffff"
    dashed = ' stroke-dasharray="5,4"' if kind == "foundation" else ""
    attrs = f'x="{number(x)}" y="{number(y)}" width="{number(w)}" height="{number(h)}" rx="6"'
    tag_width = math.ceil((text_width(KINDS[kind], 8) + 16)/4)*4
    result = [f'<g id="{prefix}-node-{slug(node["id"])}">',
              f'<rect {attrs} fill="{PAPER}"/>',
              f'<rect {attrs} fill="{fill}" stroke="{stroke}" stroke-width="1.3"{dashed}/>',
              f'<rect x="{number(x+12)}" y="{number(y+8)}" width="{tag_width}" height="14" rx="2" fill="{fill}" stroke="{stroke}" stroke-width=".8"/>',
              f'<text class="node-tag" x="{number(x+12+tag_width/2)}" y="{number(y+18)}" text-anchor="middle" fill="{stroke}" font-size="8" letter-spacing=".5">{KINDS[kind]}</text>']
    baseline = y+30+(h-30-text_height)/2+15
    for line in title:
        result.append(f'<text class="node-title" x="{number(x+w/2)}" y="{number(baseline)}" text-anchor="middle" fill="{INK}" font-size="16" font-weight="600">{esc(line)}</text>')
        baseline += 20
    for line in sub:
        result.append(f'<text class="node-sub" x="{number(x+w/2)}" y="{number(baseline-2)}" text-anchor="middle" fill="{MUTED}" font-size="12">{esc(line)}</text>')
        baseline += 17
    return "\n".join(result+["</g>"])


def repo_url(meta):
    repo = str(meta["repo"]).removesuffix(".git").rstrip("/")
    if repo.startswith("https://github.com/"):
        return repo
    if "://" in repo:
        raise ValueError("Pinned sources require a GitHub repository URL")
    return "https://github.com/" + (repo if "/" in repo else "kiseyno-AI/"+repo)


def source_url(meta, path, start=None, end=None):
    url = f'{repo_url(meta)}/blob/{quote(str(meta["commit"]), safe="")}/{quote(path, safe="/")}'
    if start is not None:
        url += f"#L{int(start)}"
        if end is not None and int(end) != int(start):
            url += f"-L{int(end)}"
    return url


def svg_start(identifier, title, description):
    prefix = "fig-" + slug(identifier)
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" id="{prefix}" viewBox="0 0 1280 720" role="img" aria-labelledby="{prefix}-title {prefix}-desc">',
        f'<title id="{prefix}-title">{esc(title)}</title>',
        f'<desc id="{prefix}-desc">{esc(description)}</desc>',
        f'<style>#{prefix} text{{font-family:{FONT}}} '
        f'#{prefix} .node-sub,#{prefix} .edge-label,#{prefix} .node-tag{{font-family:{MONO}}}</style>',
        f'<rect width="1280" height="720" fill="{PAPER}"/>',
    ]


def render_svg(diagram):
    prefix = "fig-" + slug(diagram["id"])
    ids = [node["id"] for node in diagram["nodes"]]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Duplicate node IDs in {diagram['id']}")
    if len(ids) > 9 or len(diagram["edges"]) > 12:
        raise ValueError(f"Split the diagram: {diagram['id']}")
    for edge in diagram["edges"]:
        if edge["from"] not in ids or edge["to"] not in ids:
            raise ValueError(f"Unknown edge endpoint in {diagram['id']}")
    result = svg_start(diagram["id"], diagram["title"], diagram["summary"])
    result.append("<defs>")
    for suffix, color in (("arrow", MUTED), ("arrow-accent", ACCENT), ("arrow-link", LINK)):
        result.append(f'<marker id="{prefix}-{suffix}" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto-start-reverse" markerUnits="userSpaceOnUse"><polygon points="0 0, 8 3, 0 6" fill="{color}"/></marker>')
    result.append("</defs>")
    result.extend(render_edge(edge, prefix) for edge in diagram["edges"])
    result.extend(render_node(node, prefix) for node in diagram["nodes"])
    legend = []
    if any(e.get("kind") != "conditional" and not e.get("bidir") for e in diagram["edges"]):
        legend.append(("Data / call", False, False))
    if any(e.get("kind") == "conditional" for e in diagram["edges"]):
        legend.append(("Conditional / compatibility", True, False))
    if any(e.get("bidir") for e in diagram["edges"]):
        legend.append(("Request and response / readback", False, True))
    result.append('<g aria-hidden="true"><line x1="32" y1="668" x2="1248" y2="668" stroke="#bfc0c0" stroke-width=".8"/>')
    for i, (label, dashed, bidir) in enumerate(legend):
        x = 40 + i * 400
        marker = f"{prefix}-arrow-link" if bidir else f"{prefix}-arrow"
        color = LINK if bidir else MUTED
        extra = ' stroke-dasharray="5,4"' if dashed else ""
        if bidir:
            extra += f' marker-start="url(#{marker})"'
        result.append(f'<line x1="{x}" y1="694" x2="{x+48}" y2="694" stroke="{color}" stroke-width="1.5" marker-end="url(#{marker})"{extra}/>')
        result.append(f'<text x="{x+64}" y="698" fill="{MUTED}" font-size="12">{esc(label)}</text>')
    return "\n".join(result + ["</g>", "</svg>", ""])


def matrix_foreground(opacity):
    def rgb(color):
        return [int(color[i:i+2], 16) / 255 for i in (1, 3, 5)]

    def luminance(channels):
        linear = [c / 12.92 if c <= .04045 else ((c + .055) / 1.055) ** 2.4 for c in channels]
        return sum(c * weight for c, weight in zip(linear, (.2126, .7152, .0722)))

    background = luminance([i * opacity + p * (1-opacity) for i, p in zip(rgb(INK), rgb(PAPER))])
    for color in (INK, PAPER, "#000000", "#ffffff"):
        foreground = luminance(rgb(color))
        if (max(background, foreground) + .05) / (min(background, foreground) + .05) >= 4.5:
            return color
    raise ValueError("No accessible foreground for matrix cell")


def render_graphify_matrix(summary):
    groups = summary["groups"]
    counts = {tuple(sorted((e["a"], e["b"]))): e["count"] for e in summary["links"]}
    max_value = max(counts.values())
    short = ["Entry", "Store", "EOD", "Agents", "Orders", "Markets", "Evidence"]
    result = svg_start("graphify-modules", "Graphify: static module relationships",
                       "Counts of extracted source relationships between seven application module groups; the symmetric matrix is not runtime call frequency.")
    result += [
        f'<text x="40" y="44" font-size="24" font-weight="600" fill="{INK}">Static module relationships</text>',
        f'<text x="40" y="72" font-size="14" fill="{MUTED}">Graphify source graph · extracted links only · undirected</text>',
    ]
    for j, label in enumerate(short):
        result.append(f'<text x="{260+j*92}" y="118" text-anchor="middle" font-size="13" fill="{MUTED}">{label}</text>')
    for i, group in enumerate(groups):
        y = 136 + i * 60
        result.append(f'<text x="200" y="{y+33}" text-anchor="end" font-size="14" fill="{INK}">{esc(group["label"])}</text>')
        for j, other in enumerate(groups):
            x = 216 + j * 92
            value = counts.get(tuple(sorted((group["id"], other["id"]))), 0)
            opacity = max(.05, math.sqrt(value / max_value) * .7)
            result.append(f'<rect x="{x}" y="{y}" width="88" height="56" fill="{PAPER}"/>')
            result.append(f'<rect data-row="{group["id"]}" data-col="{other["id"]}" data-value="{value}" x="{x}" y="{y}" width="88" height="56" fill="{INK}" fill-opacity="{opacity:.5f}"/>')
            label = "—" if i == j else str(value)
            color = matrix_foreground(opacity)
            result.append(f'<text x="{x+44}" y="{y+34}" text-anchor="middle" font-size="15" fill="{color}">{label}</text>')
    metrics = summary["metrics"]
    result += [
        f'<text x="916" y="154" font-size="14" font-weight="600" fill="{INK}">Full source graph</text>',
        f'<text x="916" y="184" font-size="14" fill="{MUTED}">{metrics["nodes"]:,} nodes</text>',
        f'<text x="916" y="210" font-size="14" fill="{MUTED}">{metrics["edges"]:,} links</text>',
        f'<text x="916" y="236" font-size="14" fill="{MUTED}">{metrics["communities"]:,} communities</text>',
        f'<text x="916" y="288" font-size="14" font-weight="600" fill="{INK}">This matrix</text>',
        f'<text x="916" y="318" font-size="14" fill="{MUTED}">{metrics["application_nodes"]:,} application nodes</text>',
        f'<text x="916" y="344" font-size="14" fill="{MUTED}">{metrics["cross_group_extracted_edges"]:,} cross-group links</text>',
        f'<text x="916" y="386" font-size="13" fill="{MUTED}">Each pair is counted once.</text>',
        f'<text x="916" y="408" font-size="13" fill="{MUTED}">Mirrored cells repeat that count.</text>',
        f'<text x="916" y="450" font-size="13" fill="{MUTED}">Tests, migrations, scripts,</text>',
        f'<text x="916" y="472" font-size="13" fill="{MUTED}">inferred and within-group links</text>',
        f'<text x="916" y="494" font-size="13" fill="{MUTED}">are excluded from the matrix.</text>',
        f'<text x="216" y="600" font-size="12" fill="{MUTED}">LOW</text>',
    ]
    for i in range(6):
        result.append(f'<rect x="{260+i*36}" y="584" width="32" height="20" fill="{INK}" fill-opacity="{.05+i*.13:.2f}"/>')
    result += [
        f'<text x="488" y="600" font-size="12" fill="{MUTED}">HIGH</text>',
        f'<text x="216" y="634" font-size="12" fill="{MUTED}">0–{max_value} links · square-root shading · numbers are exact counts · — = within-group omitted</text>',
        f'<line x1="40" y1="668" x2="1240" y2="668" stroke="#bfc0c0" stroke-width=".8"/>',
        f'<text x="40" y="698" font-size="12" fill="{MUTED}">Source snapshot: {esc(summary["source_commit"][:12])} · Graphify {esc(summary["graphify_version"])} · see graphify-summary.json for module membership and counts</text>',
        "</svg>", "",
    ]
    return "\n".join(result)


def render_catalog(model, summary):
    meta = model["meta"]
    sha = meta["commit"]
    diagrams = model["diagrams"]
    lines = [
        "# System and feature architecture", "",
        f"Source-verified English diagrams for `kai-trading-core`, based on [{sha[:12]}]({repo_url(meta)}/tree/{sha}) ({meta['date']}).",
        "",
        "Core is a FastAPI modular monolith: HTTP handlers, operator commands, and background jobs use domain services backed by PostgreSQL. OpenClaw supplies agent responses; the separate Exec service owns delegated broker execution. Each diagram below describes one responsibility or flow and links to the implementation that establishes its boundaries.",
        "",
        "[Back to the main README](../../README.md#architecture-and-data-flow) · [API inventory](../API_BUILD_ORDER.md) · [Operations](../../RUNBOOK.md)",
        "",
        "## Reading the diagrams", "",
        "- Solid arrows show data dependencies or calls. Blue connectors identify external/API relationships.",
        "- Dashed arrows show conditional or compatibility paths; individual notes identify the uninspected Exec-to-broker contract boundary.",
        "- A two-ended connector combines a request with its response or later readback. It does not imply a callback API.",
        "- Module, store, external, gate, and foundation tags distinguish responsibilities; boxes are not deployed servers.",
        "",
        *[f"- {item}" for item in meta["limitations"]],
        "",
        "## Feature index", "",
        "| Diagram | Responsibility |", "| --- | --- |",
    ]
    for d in diagrams:
        lines.append(f'| [{d["title"]}](#{d["id"]}) | {d["summary"]} |')
    for d in diagrams:
        lines.extend(["", f'<a id="{d["id"]}"></a>', "", f'## {d["title"]}', "", d["summary"], "",
                      f'![{d["title"]}](diagrams/{d["id"]}.svg)', ""])
        lines.extend(f'- {note}' for note in d.get("notes", []))
        if d.get("coverage"):
            lines.extend(["", "**HTTP coverage:** " + ", ".join(f'`/{c.strip("/")}`' for c in d["coverage"]) + "."])
        lines.extend(["", "**Implementation evidence**", ""])
        for s in d["sources"]:
            url = source_url(meta, s["path"], s.get("start"), s.get("end"))
            detail = f' — {s["symbol"]}' if s.get("symbol") else ""
            lines.append(f'- [`{s["path"]}`]({url}){detail}')
    lines.extend(["", "## Graphify source graph", "",
                  "![Graphify static module relationship counts](diagrams/graphify-modules.svg)", "",
                  summary["method"], "",
                  "The full graph includes test and migration symbols; its node count is not a count of production components. Graphify's raw graph is undirected. Arrow directions in the feature diagrams were assigned by reading the cited code, rather than inferred from this matrix.", "",
                  "| Measure | Value | Definition |", "| --- | ---: | --- |"])
    definitions = {
        "nodes": "Entries in graph.json nodes", "edges": "Entries in graph.json links",
        "communities": "Distinct node community IDs after clustering",
        "extracted_edges": "Links whose confidence is EXTRACTED",
        "inferred_edges": "Links whose confidence is INFERRED",
        "self_loops": "Links with identical source and target IDs",
        "dangling_endpoints": "Links with a source or target absent from the node set",
        "application_nodes": "Nodes whose source_file starts with apps/api/app/",
        "cross_group_extracted_edges": "Extracted application links with endpoints in different groups; counted once",
    }
    for key, definition in definitions.items():
        lines.append(f'| {key} | {summary["metrics"][key]:,} | {definition} |')
    lines.extend(["", "### Module grouping", "", "| Group | Immediate application modules | Nodes |", "| --- | --- | ---: |"])
    for group in summary["groups"]:
        lines.append(f'| {group["label"]} | {", ".join(f"`{m}`" for m in group["modules"])} | {group["nodes"]:,} |')
    lines.extend(["", "Counts and extraction metadata are stored in [graphify-summary.json](graphify-summary.json). Graph links count static relationships, not traffic, latency, defects, or trading performance.",
                  "", "## HTTP surface coverage", "",
                  "The inventory below counts decorators in the 18 registered router modules. It excludes `/health` in `main.py` and FastAPI's generated documentation routes.", "",
                  "| Router prefix | Routes | Feature diagrams |", "| --- | ---: | --- |"])
    for item in model["api_inventory"]:
        covered = [d for d in diagrams if item["prefix"].strip("/") in [c.strip("/") for c in d.get("coverage", [])]]
        links = ", ".join(f'[{d["title"]}](#{d["id"]})' for d in covered)
        lines.append(f'| [`{item["prefix"]}`]({source_url(meta, item["file"])}) | {item["count"]} | {links} |')
    total = sum(item["count"] for item in model["api_inventory"])
    lines.extend([f"| **Total** | **{total}** | |", "", "## Updating these diagrams", "",
                  "The editable source is [model.json](model.json); it contains node labels, explicit connector routes, notes, source anchors, and the API inventory. SVGs use local system fonts and no external scripts or images.", "",
                  "1. Verify affected source paths and refresh their anchors against the intended commit.",
                  "2. Edit the relevant diagram in `docs/architecture/model.json`. Keep each view focused on one responsibility.",
                  "3. From the repository root, regenerate the SVGs and this catalog:", "",
                  "```bash", "python3 scripts/render_architecture.py", "```", "",
                  "4. Check the rendered SVGs, local Markdown links, and `git diff --check` before committing.", "",
                  "To refresh source discovery, use Graphify's local, code-only backend:", "", "```bash",
                  *meta["graph_commands"], "```", "",
                  "The raw graph is a local analysis artifact and is not committed. To rebuild the compact summary after extraction, run:", "", "```bash",
                  "python3 scripts/render_architecture.py --refresh-graph", "```", "",
                  "Refresh the model's source commit and verify changed behavior as well; a new graph alone does not verify diagram semantics. The renderer never imports Core, opens a database, contacts a provider, dispatches an agent, or places an order.", ""])
    return "\n".join(lines)


def refresh_graph():
    import collections
    import hashlib

    graph_path = ROOT / "graphify-out" / "graph.json"
    raw = graph_path.read_bytes()
    graph = json.loads(raw)
    summary_path = DOCS / "graphify-summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    group_for = {name: g["id"] for g in summary["groups"] for name in g["modules"]}
    membership = {}
    for node in graph["nodes"]:
        path = node.get("source_file", "")
        if path.startswith("apps/api/app/"):
            module = path.removeprefix("apps/api/app/").split("/")[0].removesuffix(".py")
            if module not in group_for:
                raise ValueError(f"Add the new application module to graphify-summary.json: {module}")
            membership[node["id"]] = group_for[module]
    pairs = collections.Counter()
    for edge in graph["links"]:
        a, b = membership.get(edge["source"]), membership.get(edge["target"])
        if a and b and a != b and edge.get("confidence") == "EXTRACTED":
            pairs[tuple(sorted((a, b)))] += 1
    ids = {n["id"] for n in graph["nodes"]}
    counts = collections.Counter(membership.values())
    summary.update(source_commit=graph["built_at_commit"],
                   graph_sha256=hashlib.sha256(raw).hexdigest(),
                   graphify_version=graph["graph"]["graphify_version"])
    summary["metrics"] = {
        "nodes": len(graph["nodes"]), "edges": len(graph["links"]),
        "communities": len({n["community"] for n in graph["nodes"]}),
        "extracted_edges": sum(e.get("confidence") == "EXTRACTED" for e in graph["links"]),
        "inferred_edges": sum(e.get("confidence") == "INFERRED" for e in graph["links"]),
        "self_loops": sum(e["source"] == e["target"] for e in graph["links"]),
        "dangling_endpoints": sum(e["source"] not in ids or e["target"] not in ids for e in graph["links"]),
        "application_nodes": len(membership), "cross_group_extracted_edges": sum(pairs.values()),
    }
    for group in summary["groups"]:
        group["nodes"] = counts[group["id"]]
    summary["links"] = [{"a": a, "b": b, "count": count} for (a, b), count in sorted(pairs.items())]
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh-graph", action="store_true", help="Recount the local Graphify extraction before rendering")
    args = parser.parse_args()
    if args.refresh_graph:
        refresh_graph()
    model = json.loads((DOCS / "model.json").read_text(encoding="utf-8"))
    summary = json.loads((DOCS / "graphify-summary.json").read_text(encoding="utf-8"))
    if model["meta"]["commit"] != summary["source_commit"]:
        raise ValueError("The model and Graphify summary must reference the same source commit")
    diagrams = DOCS / "diagrams"
    diagrams.mkdir(exist_ok=True)
    for diagram in model["diagrams"]:
        (diagrams / f'{diagram["id"]}.svg').write_text(render_svg(diagram), encoding="utf-8")
    (diagrams / "graphify-modules.svg").write_text(render_graphify_matrix(summary), encoding="utf-8")
    (DOCS / "README.md").write_text(render_catalog(model, summary), encoding="utf-8")
    print(f'Rendered {len(model["diagrams"])} feature diagrams, one Graphify matrix, and the architecture catalog.')


if __name__ == "__main__":
    main()
