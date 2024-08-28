importScripts("./pkg/wasm.js");

console.log("Initializing worker");

const {entry} = wasm_bindgen;

async function init_wasm_in_worker() {

    await wasm_bindgen("./pkg/wasm_bg.wasm");
    self.postMessage("loaded");

    self.onmessage = async (event) => {
        console.log(event);
        const eventData = event?.data;
        if (!eventData) {
            console.log("missing event data");
        }
        if (eventData?.action === "upload" && eventData?.data) {
            console.log("upload with data; ", eventData?.data)
            const data = eventData?.data;
            const result = await entry(new TextEncoder().encode(data));
            console.log("done calling upload!");
            self.postMessage(result);
        } else {
            console.log(
                `not doing event because upload cond: ${
                    eventData?.action === "upload"
                } and ${eventData?.data}`
            );
            self.postMessage(`can't do event ${event}`);
        }
    };
}

init_wasm_in_worker();
