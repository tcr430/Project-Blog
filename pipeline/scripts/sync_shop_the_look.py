from __future__ import annotations

import argparse
from pathlib import Path

from publish_post import sync_shop_the_look


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild Shop the Look blocks from inline affiliate links in published posts."
    )
    parser.add_argument(
        "--post",
        type=str,
        default=None,
        help="Optional single post path to sync. If omitted, all posts are synced.",
    )
    return parser.parse_args()


def metadata_path_for_post(project_root: Path, post_path: Path) -> Path:
    return project_root / "_data" / "article_metadata" / f"{post_path.stem}.json"


def sync_all_posts(project_root: Path) -> int:
    post_dir = project_root / "_posts"
    synced_count = 0
    for post_path in sorted(post_dir.glob("*.md")):
        metadata_path = metadata_path_for_post(project_root, post_path)
        sync_shop_the_look(post_path=post_path, metadata_path=metadata_path)
        print(f"[shop] synced: {post_path.name}")
        synced_count += 1
    return synced_count


def main() -> int:
    args = parse_args()
    project_root = Path(__file__).resolve().parents[2]

    if args.post:
        post_path = Path(args.post)
        if not post_path.is_absolute():
            post_path = project_root / post_path
        metadata_path = metadata_path_for_post(project_root, post_path)
        sync_shop_the_look(post_path=post_path, metadata_path=metadata_path)
        print(f"[shop] synced: {post_path.name}")
        return 0

    synced_count = sync_all_posts(project_root)
    print(f"[shop] synced posts: {synced_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
