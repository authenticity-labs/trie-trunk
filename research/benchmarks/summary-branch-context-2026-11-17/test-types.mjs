import {registerHooks} from 'node:module';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {transformSync} from '../../../client/node_modules/rolldown/dist/utils-index.mjs';
registerHooks({
 resolve(specifier,context,next){try{return next(specifier,context)}catch(error){if(specifier.startsWith('.')&&!/\.[a-z]+$/.test(specifier))return next(specifier+'.ts',context);throw error;}},
 load(url,context,next){if(url.endsWith('.ts'))return {format:'module',source:transformSync(fileURLToPath(url),readFileSync(new URL(url),'utf8')).code,shortCircuit:true};return next(url,context);}
});
