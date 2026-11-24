import { spawnSync,execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { copyFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
const root=fileURLToPath(new URL('../wasm/',import.meta.url));
const env={...process.env};
if(!env.AR_wasm32_unknown_unknown) {
  const sysroot=execFileSync('rustc',['--print','sysroot'],{encoding:'utf8'}).trim();
  const host=execFileSync('rustc',['-vV'],{encoding:'utf8'}).match(/^host: (.+)$/m)[1];
  const ar=resolve(sysroot,'lib/rustlib',host,'bin/llvm-ar');if(existsSync(ar))env.AR_wasm32_unknown_unknown=ar;
}
const run=spawnSync('cargo',['build','--release','--target','wasm32-unknown-unknown','--manifest-path',resolve(root,'Cargo.toml')],{stdio:'inherit',env});
if(run.status!==0)process.exit(run.status??1);
const target=process.env.CARGO_TARGET_DIR?resolve(process.env.CARGO_TARGET_DIR):resolve(root,'target');
await copyFile(resolve(target,'wasm32-unknown-unknown/release/data_seeds_wasm.wasm'),new URL('./data-seeds.wasm',import.meta.url));
