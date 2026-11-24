import {build,mergeConfig} from '../../../client/node_modules/vite/dist/node/index.js';
import base from '../../../client/vite.config.ts';
import {fileURLToPath} from 'node:url';
const once=(code,before,after)=>{if(!code.includes(before))throw Error('Instrumentation anchor missing: '+before);return code.replace(before,after)};
const plugin=()=>({name:'scoped-native-activity',enforce:'pre',transform(code,id){
 if(id.endsWith('/pil/client.ts')){
  code=once(code,'function request(','async function request(');
  code=once(code,'signal.throwIfAborted();',`signal.throwIfAborted();
  if(command.kind==='view'&&!command.query.preview)lastView=command;`);
  code=once(code,'if(trace)trace.portReadyAt=',`await activity('begin',id);
  if(signal.aborted){await activity('end',id);signal.throwIfAborted();}
  if(trace)trace.portReadyAt=`);
  code=once(code,"if(event.data.kind==='storage-status')",`if(event.data.kind==='activity-ended'){void activity('end',event.data.id);return;}
      if(event.data.kind==='error')void activity('end',event.data.id);
      if(event.data.kind==='storage-status')`);
  code=once(code,'connection.postMessage(message);',`if(message.kind==='view'&&!message.query.preview)(message as any).diagnosticDelay=(globalThis as any).__testWorkerDelay??0;
    connection.postMessage(message);`);
  code=`
const activity=(action:string,id?:number)=>{
 const bridge=(window as any).webkit?.messageHandlers?.queryActivity;
 if(!bridge)throw Error('Missing native scheduling diagnostic adapter');
 return bridge.postMessage({action,...(id===undefined?{}:{id})});
};
let lastView:any;
(globalThis as any).__activityStatus=()=>activity('status');
(globalThis as any).__activityCancel=async()=>{
 if(!lastView)throw Error('No measured query available for cancellation test');
 const controller=new AbortController();let dispatched!:()=>void;
 const started=new Promise<void>(resolve=>dispatched=resolve);
 const previous=(globalThis as any).__testWorkerDelay;(globalThis as any).__testWorkerDelay=2000;
 const work=request(lastView,controller.signal,()=>{},undefined,dispatched).then(()=>false,()=>true);
 await started;(globalThis as any).__testWorkerDelay=previous;controller.abort();
 if(!await work)throw Error('Cancelled request unexpectedly resolved');
 const status=await activity('status');if(!status.active)throw Error('Activity released before worker finished cancellation');
 return status;
};
`+code;
  return code;
 }
 if(id.endsWith('/pil/worker-runtime.ts')){
  code=once(code,'const token = request.token.trim();',`if((request as any).diagnosticDelay)await new Promise(resolve=>setTimeout(resolve,(request as any).diagnosticDelay));
        const token = request.token.trim();`);
  return once(code,'if (jobs.get(id) === job) jobs.delete(id);',`if (jobs.get(id) === job) jobs.delete(id);
          send({id,kind:'activity-ended'} as any);`);
 }
}});
process.chdir(fileURLToPath(new URL('../../../client',import.meta.url)));
await build(mergeConfig(base({mode:'production',command:'build'}),{
 configFile:false,plugins:[plugin()],worker:{plugins:()=>[plugin()]},
 build:{outDir:'/tmp/snoot-scoped-scheduling-web',emptyOutDir:true}
}));
