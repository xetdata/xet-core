async function run_wasm() {
    console.log("index.js loaded");

    const worker = new Worker("./lib/worker.js");

    document.getElementById("upload").addEventListener("change", function (event) {
        if (!event || !event.target || !event.target.files || event.target.files.length === 0) {
            return;
        }
        file = event.target.files[0]
        const reader = new FileReader()
        reader.onload = (e) => {
            worker.postMessage({action: "clean", data: e.target.result});
        }
        reader.readAsArrayBuffer(file)
    })

    worker.onmessage = async (event) => {
        console.log("receive event", event)
        const eventData = event?.data;
        if (!eventData) {
            console.log("missing event data");
        }
        if (eventData?.action === "clean_finish" && eventData?.data) {
            document.getElementById("pointer_file").innerText = eventData?.data
        } else if (eventData?.action === "smudge_finish" && eventData?.data) {
            document.getElementById("content").innerText = eventData?.data
        }
    }
    document.getElementById("smudge").onclick = async (event) => {
        const ptr_file = document.getElementById("pointer_file").innerText
        if (ptr_file) {
            worker.postMessage({action: "smudge", data: ptr_file})
        }
    }
}

window.addEventListener("DOMContentLoaded", run_wasm);
