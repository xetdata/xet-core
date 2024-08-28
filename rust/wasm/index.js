async function run_wasm() {
    await wasm_bindgen();

    console.log("index.js loaded");

    const loaded = w =>
        new Promise(r => w.addEventListener("message", r, {once: true}));

    const channel = new BroadcastChannel("my_bus");
    const worker = new Worker("./worker.js");
    await Promise.all([
        loaded(worker),
    ]);

    const fileData = `hello world`
    worker.postMessage({action: "upload", data: fileData});
}

run_wasm();
