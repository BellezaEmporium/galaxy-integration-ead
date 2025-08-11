#!/bin/sh

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MAC_ZIP="$SCRIPT_DIR/origin_v0.44.4.zip"
PLUGIN_PATH="$HOME/Library/Application Support/GOG.com/Galaxy/plugins/installed/origin_7f53219b-4e2b-4591-9f4f-dfc5f4ba9eb0"

# Check if the zip file exists in the same directory as the script
if [ ! -f "$MAC_ZIP" ]; then
    echo "Error: origin_v0.44.4.zip not found in the same directory as install.command"
    exit 1
fi

rm -rf "$PLUGIN_PATH"
mkdir -p "$PLUGIN_PATH"
unzip "$MAC_ZIP" -d "$PLUGIN_PATH"
find "$PLUGIN_PATH" -name "*.so" -exec xattr -c {} \;