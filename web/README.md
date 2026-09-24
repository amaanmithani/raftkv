Build locally:

    GOOS=js GOARCH=wasm go build -ldflags="-s -w" -o web/raftkv.wasm ./cmd/wasm
    cp "$(go env GOROOT)/lib/wasm/wasm_exec.js" web/
    python3 -m http.server --directory web 8080
