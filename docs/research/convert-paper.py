#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pymupdf4llm",
# ]
# ///
"""Convert a research-paper PDF to searchable Markdown plus figure images.

Usage:
    uv run convert-paper.py <paper.pdf | https://host/paper.pdf> <slug> \
        [--title "Paper title"] [--source "https://host/paper.pdf"] [--no-cover]
        [--no-figures]

Creates next to this script:
    <slug>.md          per-page extracted text delimited by <!-- pdf page N -->
                       markers, with figure links at the top of each page block
    <slug>.figures/    figure images, one PNG per embedded figure
    <slug>.pdf         compressed copy of the PDF: cover-page banner art is
                       blanked out and embedded images are downsampled to
                       ~150 dpi (via Ghostscript when available). Kept as the
                       canonical fallback; page numbering is unchanged so
                       `pdf page N` markers still match.

Figures are clip-rendered from their display region on the page, so the PNG
matches the paper exactly (alpha masks and vector overlays like arrows are
handled). Embedded images smaller than ~70x70 pt are skipped as icons or
logos. If page 1 holds very little text it is treated as a conference cover
page: its images are skipped for extraction and blanked in the compressed
PDF (they are banner art, not paper figures). Papers without a cover keep
all of their page-1 images and figures. If the check misfires and a real
first page is treated as a cover, pass --no-cover to keep its images.
Pass --no-figures for a text-only conversion: no .figures directory is
created and the Markdown contains no figure links.
"""

import argparse
import pathlib
import shutil
import subprocess
import tempfile
import urllib.request

import pymupdf
import pymupdf4llm

OUT_DIR = pathlib.Path(__file__).resolve().parent
FIGURE_DPI = 150
MIN_FIGURE_AREA_PT2 = 5000
# A first page with this little text is a proceedings cover, not paper
# content. Real first pages carry the abstract and intro (thousands of
# characters). When in doubt the page is not a cover and its content is kept.
COVER_MAX_CHARS = 1500


def fetch_pdf(source: str) -> str:
    if not source.startswith(("http://", "https://")):
        return source
    req = urllib.request.Request(source, headers={"User-Agent": "curl/8"})
    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    with urllib.request.urlopen(req) as resp:
        tmp.write(resp.read())
    tmp.close()
    return tmp.name


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf", help="local PDF path or URL")
    parser.add_argument("slug", help="output basename, e.g. atc23-idziorek")
    parser.add_argument("--title", help="paper title for the Markdown header")
    parser.add_argument("--source", help="canonical URL of the PDF for the header")
    parser.add_argument(
        "--no-cover",
        action="store_true",
        help="treat page 1 as content: never skip or blank its images",
    )
    parser.add_argument(
        "--no-figures",
        action="store_true",
        help="text-only conversion: no .figures directory or figure links",
    )
    args = parser.parse_args()

    pdf_path = fetch_pdf(args.pdf)
    doc = pymupdf.open(pdf_path)
    cover = not args.no_cover and len(doc[0].get_text()) < COVER_MAX_CHARS
    fig_dir = OUT_DIR / f"{args.slug}.figures"
    if not args.no_figures:
        fig_dir.mkdir(exist_ok=True)
        for stale in fig_dir.glob("page-*-fig-*.png"):
            stale.unlink()

    header = f"# {args.title or args.slug}\n\n"
    if args.source:
        header += f"- Source: {args.source}\n"
    header += (
        "- Note for readers: text below is extracted per PDF page, delimited by\n"
        "  `<!-- pdf page N -->` markers."
    )
    if not args.no_figures:
        header += (
            " Figure images embedded in the PDF are\n"
            f"  extracted under `{args.slug}.figures/` and linked at the top of the\n"
            "  page they appear on, ordered top to bottom. Open an image file to see\n"
            "  the figure; captions stay in the page text."
        )
    header += (
        " Code listings and fine\n"
        "  print may render imperfectly as text; fall back to the local PDF.\n\n"
        "---\n\n"
    )

    parts = [header]
    nfigs = 0
    for i, page in enumerate(doc):
        fig_links = []
        if not args.no_figures and (i > 0 or not cover):
            seen = set()
            imgs = []
            for img in page.get_images(full=True):
                xref = img[0]
                if xref in seen:
                    continue
                seen.add(xref)
                rects = page.get_image_rects(xref)
                if not rects:
                    continue
                rect = rects[0]
                for extra in rects[1:]:
                    rect |= extra
                if rect.get_area() < MIN_FIGURE_AREA_PT2:
                    continue
                imgs.append((rect.y0, rect))
            for n, (_, rect) in enumerate(sorted(imgs), start=1):
                clip = (rect + (-8, -8, 8, 8)) & page.rect
                fname = f"page-{i + 1:02d}-fig-{n}.png"
                page.get_pixmap(clip=clip, dpi=FIGURE_DPI).save(fig_dir / fname)
                fig_links.append(
                    f"![page {i + 1} figure {n}]({args.slug}.figures/{fname})"
                )
                nfigs += 1

        page_md = pymupdf4llm.to_markdown(doc, pages=[i])
        block = f"<!-- pdf page {i + 1} -->\n\n"
        if fig_links:
            block += f"**Figures on page {i + 1}:**\n\n" + "\n\n".join(fig_links) + "\n\n"
        parts.append(block + page_md)

    out_path = OUT_DIR / f"{args.slug}.md"
    out_path.write_text("\n".join(parts), encoding="utf-8")

    pdf_out = OUT_DIR / f"{args.slug}.pdf"
    if pathlib.Path(pdf_path).resolve() != pdf_out.resolve():
        # Blank cover-page images (often multi-MB banner art). Pages are not
        # deleted, so `pdf page N` markers still match.
        if cover:
            page1 = doc[0]
            blank = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 8, 8))
            blank.clear_with(255)
            for img in page1.get_images(full=True):
                page1.replace_image(img[0], pixmap=blank)
        if shutil.which("gs"):
            # Ghostscript downsamples embedded images to ~150 dpi and
            # recompresses; text stays text. Without gs, deflate only.
            with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp:
                doc.save(tmp.name, garbage=4, deflate=True)
                doc.close()
                subprocess.run(
                    [
                        "gs",
                        "-sDEVICE=pdfwrite",
                        "-dCompatibilityLevel=1.5",
                        "-dPDFSETTINGS=/ebook",
                        "-dNOPAUSE",
                        "-dQUIET",
                        "-dBATCH",
                        f"-sOutputFile={pdf_out}",
                        tmp.name,
                    ],
                    check=True,
                )
                doc = pymupdf.open(pdf_out)  # keep doc valid for print below
        else:
            doc.save(pdf_out, garbage=4, deflate=True)

    print(
        f"wrote {out_path} ({doc.page_count} pages, {nfigs} figures), "
        f"{pdf_out} ({pdf_out.stat().st_size // 1024}KB)"
    )
    doc.close()


if __name__ == "__main__":
    main()
