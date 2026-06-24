#! /bin/bash
TMP="${TMPDIR:-/tmp}"
for dir in "$TMP"/jsii-kernel-*; do
    [ -d "$dir" ] && rm -rf "$dir"
done

