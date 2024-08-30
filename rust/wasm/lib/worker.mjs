import init, { clean, smudge } from '/xet-wasm-core/wasm.js';

console.log('Initializing worker');

async function init_wasm_in_worker() {
	await init();
	self.postMessage({ action: 'loaded' });

	self.onmessage = async (event) => {
		console.log({ event });
		const eventData = event?.data;
		if (!eventData) {
			console.log('missing event data');
		}
		if (eventData?.action === 'clean' && eventData?.data) {
			const data = eventData?.data;
			console.log('upload with data; ', data);
			const result = await clean(data);
			console.log('result', result);
			self.postMessage({ action: 'cleaned', data: result });
		} else if (eventData?.action === 'smudge' && eventData?.data) {
			const ptr_file = eventData?.data;
			console.log('smudge with pointer ', ptr_file);
			const content = await smudge(ptr_file);
			console.log('smudge result ', content);
			self.postMessage({ action: 'smudged', data: content });
		} else {
			console.log(`not doing event because upload cond: ${eventData?.action === 'upload'} and ${eventData?.data}`);
			self.postMessage(`can't do event ${event}`);
		}
	};
}

init_wasm_in_worker();
