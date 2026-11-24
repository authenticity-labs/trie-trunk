// Test-only TS loader for Node releases whose strip mode rejects parameter properties.
import {registerHooks} from 'node:module';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {transformSync} from '../../../client/node_modules/rolldown/dist/utils-index.mjs';
registerHooks({load(url,context,next){
 if(url.startsWith('file:')&&url.endsWith('.ts'))return {
  format:'module',shortCircuit:true,
  source:transformSync(fileURLToPath(url),readFileSync(new URL(url),'utf8')).code,
 };
 return next(url,context);
}});
