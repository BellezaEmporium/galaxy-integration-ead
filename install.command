#!/bin/sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAC_ZIP="$SCRIPT_DIR/mac.zip"
PLUGIN_PATH="$HOME/Library/Application Support/GOG.com/Galaxy/plugins/installed/origin_7f53219b-4e2b-4591-9f4f-dfc5f4ba9eb0"

rm -rf "$PLUGIN_PATH"
mkdir -p "$PLUGIN_PATH"
unzip "$MAC_ZIP" -d "$PLUGIN_PATH"
find "$PLUGIN_PATH" -name "*.so" -exec xattr -c {} \;