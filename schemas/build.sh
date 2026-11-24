#!/bin/sh
set -eu
# RX compiles ordinary CDDL groups into a single Definition for each root.
rx=${RX:-rx}
cd "$(dirname "$0")"
for root in log http-log database-log messaging-log; do
  "$rx" write log.cddl "$root.rx" --root "$root"
done
"$rx" write collection.cddl collection.rx --root collection

# Embedded logger artifact is generated from the same authored source.
cp http-log.rx ../go/logger/client/definitions/http-log.rx
