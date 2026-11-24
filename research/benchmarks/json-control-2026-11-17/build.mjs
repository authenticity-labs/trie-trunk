// A separate, unpublished benchmark build. No control branch ships in Snoot.
import {build,mergeConfig} from '../../../client/node_modules/vite/dist/node/index.js';
import base from '../../../client/vite.config.ts';
import {fileURLToPath} from 'node:url';
const helper=fileURLToPath(new URL('./control.ts',import.meta.url));
const plugin=()=>({name:'json-query-control',enforce:'pre',transform(code,id){
 if(!id.endsWith('/services/search/registered.ts'))return;
 if(!code.includes('export async function query('))throw Error('Query entry changed');
 if(process.env.BENCH_EXPORT==='1')code=code.replace('await yieldTask();', 'await Promise.resolve();').replaceAll('await yieldTask();','await Promise.resolve();');
 return code.replace('export async function query(', 'async function original(')+`\nimport {control} from ${JSON.stringify(helper)};\nexport const query=(...args:Parameters<typeof original>)=>control(original,args);\n`;
}});
process.chdir(fileURLToPath(new URL('../../../client',import.meta.url)));
await build(mergeConfig(base({mode:'production',command:'build'}),{
 configFile:false,plugins:[plugin()],worker:{plugins:()=>[plugin()]},
 build:{outDir:'/tmp/snoot-json-web',emptyOutDir:true}
}));
