async function run_wasm() {
  await wasm_bindgen();

  console.log("index.js loaded");

  const worker = new Worker("./worker.js");

  document
    .getElementById("upload")
    .addEventListener("change", function (event) {
      if (
        !event ||
        !event.target ||
        !event.target.files ||
        event.target.files.length === 0
      ) {
        return;
      }
      file = event.target.files[0];
      const reader = new FileReader();
      reader.onload = (e) => {
        worker.postMessage({ action: "clean", data: e.target.result });
      };
      reader.readAsArrayBuffer(file);
    });

  worker.onmessage = async (event) => {
    console.log("receive event", event);
    const eventData = event?.data;
    if (!eventData) {
      console.log("missing event data");
    }
    if (eventData?.action === "clean_finish" && eventData?.data) {
      document.getElementById("pointer_file").innerText = eventData?.data;
    } else if (eventData?.action === "smudge_finish" && eventData?.data) {
      document.getElementById("content").innerText = eventData?.data;
    }
  };
  document.getElementById("smudge").onclick = async (event) => {
    const ptr_file = document.getElementById("pointer_file").innerText;
    if (ptr_file) {
      worker.postMessage({ action: "smudge", data: ptr_file });
    }
  };

  console.log("fetching");
  fetch(
    "http://localhost:4884/default/c08641b531d4e59ca1b9e5b92128581621a8f315f28fbac460a56d54aa630265"
  )
    .then((v) => console.log(`success ${v}`))
    .catch((e) => console.trace(`failed ${e}`));
}

run_wasm();
