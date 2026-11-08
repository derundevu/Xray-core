#!/usr/bin/env bash
set -euo pipefail
: "${ASSET_NAME:?}" "${GOOS:?}" "${GOARCH:?}"
cp resources/geoip.dat resources/geosite.dat build_assets/
cp README.md LICENSE build_assets/
if [[ "$GOOS" == windows ]]; then
  cp .github/build/windows/* build_assets/
  arch=$GOARCH
  [[ "$arch" != 386 ]] || arch=x86
  cp "resources/wintun/bin/$arch/wintun.dll" build_assets/
  cp resources/wintun/LICENSE.txt build_assets/LICENSE-Wintun
fi
(cd build_assets && zip -9 -r "../Xray-$ASSET_NAME.zip" .)
for method in md5 sha1 sha256 sha512; do
  openssl dgst "-$method" "Xray-$ASSET_NAME.zip" >> "Xray-$ASSET_NAME.zip.dgst"
done
