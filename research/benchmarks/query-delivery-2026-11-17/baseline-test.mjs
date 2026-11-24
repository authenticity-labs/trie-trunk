import {registerHooks} from 'node:module';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {transformSync} from '../../../client/node_modules/rolldown/dist/utils-index.mjs';
registerHooks({load(url,context,next){
 if(url.endsWith('/services/pil/worker-runtime.ts'))return {format:'module',shortCircuit:true,
  source:transformSync(fileURLToPath(url),readFileSync(new URL('./before/worker-runtime.ts',import.meta.url),'utf8')).code};
 return next(url,context);
}});
