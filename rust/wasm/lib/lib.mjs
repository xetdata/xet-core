const worker = new Worker('/xet-wasm-core/worker.mjs', { type: 'module' });

const comms = new EventTarget();

worker.addEventListener('message', (event) => {
	const { action, data } = event.data;

	switch (action) {
		case 'loaded': {
			const loadedEvent = new CustomEvent('loaded');
			comms.dispatchEvent(loadedEvent);
			console.log('Worker loaded');
			break;
		}
		case 'cleaned': {
			const cleanedEvent = new CustomEvent('cleaned', { detail: data });
			comms.dispatchEvent(cleanedEvent);
			console.log('Cleaned', data);
			break;
		}
		case 'smudged': {
			const smudgedEvent = new CustomEvent('smudged', { detail: data });
			comms.dispatchEvent(smudgedEvent);
			console.log('Smudged', data);
			break;
		}
		default:
			console.log('Unknown action', action);
			break;
	}
});

export function init() {
	return new Promise((resolve) => {
		comms.addEventListener('loaded', async () => {
			resolve();
		});
	});
}

export function smudge(hash) {
	return new Promise((resolve, reject) => {
		comms.addEventListener('smudged', async (event) => {
			if (!event.data) {
				reject(`Smudge of ${hash} failed.`);
			}
			resolve(event.data);
		});
		worker.postMessage({ action: 'smudge', data: hash });
	});
}

export function clean(data) {
	return new Promise((resolve, reject) => {
		comms.addEventListener('cleaned', async (event) => {
			if (!event.data) {
				reject(`Clean of "${data.slice(0, 30).trimEnd()}${data.length > 30 ? '...' : ''}" failed.`);
			}
			resolve(event.data);
		});
		worker.postMessage({ action: 'clean', data });
	});
}
