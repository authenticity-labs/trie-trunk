import {readFile} from 'node:fs/promises';
const {chromium, webkit} = await import(process.env.PLAYWRIGHT_MODULE ?? 'playwright-core');
const bytes = await readFile(process.argv[2]);
const engine = process.argv[3] ?? 'chromium';
const browser = engine === 'webkit' ? await webkit.launch({headless:true}) :
    await chromium.launch({headless:true, executablePath:process.env.CHROME_PATH ?? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'});
try {
    console.error(engine, browser.version());
    const page = await browser.newPage();
    const rows = await page.evaluate(async ({encoded, coarseClock}) => {
        const module = await WebAssembly.compile(Uint8Array.from(atob(encoded), c => c.charCodeAt(0)));
        function workerMain() {
            self.onmessage = async ({data}) => {
                try {
                    const {exports:{prepare,run}} = await WebAssembly.instantiate(data.module);
                    const rows = [];
                    for (let round=0; round<19; round++) {
                        for (let index=0; index<3; index++) {
                            const c=(round+index)%3;
                            prepare(c,round);
                            for (let j=0; j<10; j++) {
                                const test=(round*3+j)%10;
                                const algorithm=test%5, side=Math.floor(test/5);
                                const batches = data.coarseClock ? (algorithm === 1 || algorithm === 2 ? 2 : 16) : 1;
                                const start=performance.now();
                                for (let batch=0; batch<batches; batch++) run(algorithm,side);
                                const ns=(performance.now()-start)*1e6/(131072*batches);
                                if (round>=2) rows.push([c,round,algorithm,side,ns.toFixed(3)].join(','));
                            }
                        }
                    }
                    self.postMessage({rows});
                } catch(error) { self.postMessage({error:String(error)}); }
            };
        }
        const url=URL.createObjectURL(new Blob(['('+workerMain.toString()+')()'],{type:'text/javascript'}));
        const worker=new Worker(url);
        try {
            return await new Promise((resolve,reject)=>{
                worker.onmessage=({data})=>data.error ? reject(Error(data.error)) : resolve(data.rows);
                worker.onerror=event=>reject(Error(event.message));
                worker.postMessage({module,coarseClock});
            });
        } finally { worker.terminate(); URL.revokeObjectURL(url); }
    }, {encoded:bytes.toString('base64'), coarseClock:engine==='webkit'});
    console.log(['case,round,algorithm,side,ns_per_lookup',...rows].join('\n'));
} finally { await browser.close(); }
