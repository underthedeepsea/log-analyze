#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE="$ROOT/frontend/dist"
DESTINATION="$ROOT/src/logrisk_django/static/logrisk"

if [[ ! -f "$SOURCE/index.html" || ! -f "$SOURCE/config.js" ]]; then
  echo "frontend/dist 缺少已提交的静态资源" >&2
  exit 1
fi

STAGING="$(mktemp -d "$ROOT/.logrisk-django-static.XXXXXX")"
cleanup() { rm -rf "$STAGING"; }
trap cleanup EXIT
mkdir -p "$STAGING/logrisk"

while IFS= read -r -d '' source; do
  relative="${source#"$SOURCE"/}"
  case "$relative" in
    *".."*|/*) echo "拒绝不安全的静态资源路径" >&2; exit 1 ;;
  esac
  target="$STAGING/logrisk/$relative"
  mkdir -p "$(dirname "$target")"
  cp "$source" "$target"
done < <(find "$SOURCE" -type f -print0)

rm -rf "$DESTINATION"
mkdir -p "$(dirname "$DESTINATION")"
mv "$STAGING/logrisk" "$DESTINATION"
