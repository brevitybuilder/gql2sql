#!/usr/bin/env bash
set -e

cargo fmt
git update-index -q --refresh
if ! git diff-index --quiet HEAD --; then
  echo "Working directory not clean, please commit your changes first"
  exit
fi

bump=$1

cargo release "$bump" --no-publish --no-push --no-tag --execute --no-confirm

cd gql2sql_node
npm --no-git-tag-version version "$bump"
npm pkg get version | jq -r '.' > ../version.txt
cd ..

git add .
git commit -m "$(< version.txt)"
git tag -a "v$(< version.txt)" -m "$(< version.txt)"
git push origin main --tags -f

rm version.txt
