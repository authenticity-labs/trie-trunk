// Benchmark-only switch: the same native resource operations over ordinary HTTP.
import {build,mergeConfig} from '../../../client/node_modules/vite/dist/node/index.js';
import base from '../../../client/vite.config.ts';
import {fileURLToPath} from 'node:url';
const plugin=()=>({name:'native-http-control',enforce:'pre',transform(code,id){
 if(!id.endsWith('/pil/node-worker.ts'))return;
 if(!code.includes('desktop=new DesktopResources(url.href);'))throw Error('Desktop setup changed');
 return code.replace('desktop=new DesktopResources(url.href);','/* Native HTTP control. */');
}});
process.chdir(fileURLToPath(new URL('../../../client',import.meta.url)));
await build(mergeConfig(base({mode:'production',command:'build'}),{
 configFile:false,plugins:[plugin()],worker:{plugins:()=>[plugin()]},
 build:{outDir:'/tmp/snoot-delivery-http-web',emptyOutDir:true}
}));
