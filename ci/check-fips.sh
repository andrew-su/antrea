#!/bin/bash

set -x

fips_check() {
    PROGRAM="$1"
    GOVERSION="go1.23."
    declare -A SYMBOLS_MAP=(
        ["Cfunc__goboringcrypto"]=1
        ["goboringcrypto"]=1
        ["crypto/internal/boring/sig.BoringCrypto"]=1
        ["crypto/internal/boring/sig.StandardCrypto"]=0
        ["$GOVERSION"]=1
        ["X:boringcrypto"]=1
    )

    for symbol in "${!SYMBOLS_MAP[@]}"; do
        echo "Grep symbol $symbol in $PROGRAM..."
        count=$(strings "$PROGRAM" | grep -ci "$symbol")

        # Get the expected condition (0 or 1) from the map
        expected="${SYMBOLS_MAP[$symbol]}"
        if { [ "$expected" -eq 1 ] && [ "$count" -gt 0 ]; } || { [ "$expected" -eq 0 ] && [ "$count" -eq 0 ]; }; then
                echo "Count: $count. PASS"
        else
            echo "Count: $count. FAIL"
            echo "Checking Crypto symbols failed"
            exit 1
        fi
    done
}

for file in "$@"; do
  fips_check $file
done
