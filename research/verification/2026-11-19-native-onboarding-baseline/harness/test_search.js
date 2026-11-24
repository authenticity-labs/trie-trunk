// Test-only measurements live outside the shipped app.
(async()=>{
 const startup={navigationStartAt:performance.timeOrigin,driverMs:performance.now()};
 globalThis.__testProgress?.({stage:"driver"});
 const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
 const wait=async test=>{const start=performance.now();while(!test()){if(performance.now()-start>55000)throw Error('Search timed out');await sleep(20)}};
 try{
  if(CHECK_ACTIVITY){
   const native=globalThis.webkit.messageHandlers.snootActivity??globalThis.webkit.messageHandlers.queryActivity;
   globalThis.__activityStatus=()=>native.postMessage({action:'status'});
   const a=await native.postMessage({action:'begin',id:-1});
   const b=await native.postMessage({action:'begin',id:-2});
   const c=await native.postMessage({action:'end',id:-1});
   const d=await native.postMessage({action:'end',id:-2});
   if(b.active!==a.active+1||c.active!==a.active||d.active!==a.active-1)throw Error('Native activity overlap failed');
   if(!['default','task','native'].includes(a.mode)&&a.policy!==2)throw Error('Native activity did not disable inactive scheduling');
   if(!d.active&&(d.policy!==d.original||d.token||d.starts!==d.ends))throw Error('Native activity did not restore idle policy');
  }
  const searchTab=()=>[...document.querySelectorAll('nav button')].find(b=>b.textContent.trim()==='Search');
  await wait(()=>searchTab());searchTab().click();
  document.querySelector('button[aria-label="Expand Search"],button[aria-label="Edit Search"]')?.click();
  const button=()=>document.querySelector('.query-controls .primary');
  const controls=()=>button()&&!button().disabled;
  const observeControls=new MutationObserver(()=>{if(startup.controlsReadyMs===undefined&&controls())startup.controlsReadyMs=performance.now()});
  observeControls.observe(document.body,{subtree:true,childList:true,attributes:true,attributeFilter:['disabled']});
  if(controls())startup.controlsReadyMs=performance.now();
  try{await wait(controls)}finally{observeControls.disconnect()}
  const editor=document.getElementById('snoot-query');editor.value=SEARCH_QUERY;editor.dispatchEvent(new Event('input',{bubbles:true}));
  const windowSelect=document.querySelector('select[aria-label="Time Window"]');if(windowSelect.value!==WINDOW_PRESET){windowSelect.value=WINDOW_PRESET;windowSelect.dispatchEvent(new Event('change',{bubbles:true}));}
  await sleep(20);
  if(EXPECT_SUMMARY_PRUNED){
   const started=performance.now();
   await wait(()=>button()?.disabled&&button().title==='Summaries rule out matches in this time window');
   if(!document.body.innerText.includes('No Matches In This Time Window'))throw Error('Missing summary-pruning explanation');
   await fetch('/__desktop/report',{method:'POST',body:JSON.stringify({ok:true,summaryPruned:true,previewWaitMs:performance.now()-started,query:editor.value,screen:document.body.innerText.slice(-3000)})});
   return;
  }
  await wait(()=>button()&&!button().disabled);
  const runs=[];
  for(let repeat=0;repeat<REPEAT_COUNT;repeat++){
  if(NATIVE_WAKE)await globalThis.webkit.messageHandlers.queryActivity.postMessage({action:'wake'});
  if(!button()){
   const expand=document.querySelector('button[aria-label="Expand Search"]');
   if(!expand)throw Error('Missing Search controls');
   expand.click();await wait(()=>button()&&!button().disabled);
  }
  // Completion can precede a pending preview/control update. A click on a
  // disabled button is ignored; wait for the same actionable state a user needs.
  await wait(()=>button()&&!button().disabled&&button().getAttribute('aria-busy')!=='true');
  const prior=globalThis.__snootSearchBoundary;
  // A hidden WebKit page can throttle the driver's polling timer. Observe the
  // actual settled DOM separately so delayed test wakeups aren't called render time.
  const ready=()=>globalThis.__snootSearchBoundary!==prior&&document.querySelector('.paged-results[aria-busy="false"]')&&(!button()||button().getAttribute('aria-busy')!=='true');
  let readyAt;
  const observer=new MutationObserver(()=>{if(readyAt===undefined&&ready())readyAt=performance.now()});
  observer.observe(document.body,{subtree:true,childList:true,attributes:true,attributeFilter:['aria-busy']});
  const started=performance.now();if(repeat===0){startup.submitMs=started;startup.replicaReset=globalThis.__testReplicaReset}button().click();
  const submitHandlerMs=performance.now()-started;
  try{await wait(ready)}finally{observer.disconnect()}
  const error=document.querySelector('.error-notice, .insights-column [role="alert"]');if(error)throw Error(error.textContent);
  const loaded=Number(document.querySelector('.results-card .results-toolbar strong span')?.textContent.replaceAll(',','')??0);
  const visible=document.querySelectorAll('.event-summary').length;
  if(loaded<MIN_MATCHES)throw Error(`Expected at least ${MIN_MATCHES} records, received ${loaded}`);
  if(MIN_MATCHES>0&&!visible)throw Error('The result count is populated but no event is rendered');
  if(EXPECTED_TABLE_TOTAL!==null){
   const cells=[...document.querySelectorAll('.fast-table-scroll tbody tr td:last-child')];
   const total=cells.reduce((n,cell)=>n+Number(cell.textContent.replaceAll(',','')),0);
   if(!cells.length||total!==EXPECTED_TABLE_TOTAL)throw Error(`Expected SQL total ${EXPECTED_TABLE_TOTAL}, received ${total}`);
  }
  const wallMs=performance.now()-started;
  globalThis.__testProgress?.({stage:'complete',repeat,loaded,visible,wallMs,domReadyMs:readyAt===undefined?null:readyAt-started,boundary:globalThis.__snootSearchBoundary});
  runs.push({loaded,visible,wallMs,domReadyMs:readyAt===undefined?null:readyAt-started,submitHandlerMs,startedAt:performance.timeOrigin+started,observedAt:performance.timeOrigin+performance.now(),visibility:document.visibilityState,window:document.querySelector('.time-window-label')?.textContent,boundary:globalThis.__snootSearchBoundary});
  if(repeat+1<REPEAT_COUNT)await Promise.resolve();
  }
  let activity,cancellation;
  if(CHECK_ACTIVITY){
   const start=performance.now();
   do{activity=await globalThis.__activityStatus();if(!activity.active&&!activity.pending)break;if(performance.now()-start>15000)throw Error('Native activity remained active after worker completion');await sleep(20)}while(true);
   if(activity.errors)throw Error('Native asynchronous activity failed');
   if(activity.policy!==activity.original||activity.token||activity.starts!==activity.ends)throw Error('Native activity was not released');
   if(ACTIVITY_CANCEL){
    // Cancel through the real view lifecycle: replacing the submitted query
    // disposes its cursor request. Navigation alone keeps Search mounted.
    document.querySelector('button[aria-label="Expand Search"],button[aria-label="Edit Search"]')?.click();
    await wait(()=>button()&&!button().disabled);
    button().click();
    // Submission can await calendar readiness before acquiring activity.
    let beforeAbort;const acquireStart=performance.now();
    do{beforeAbort=await globalThis.__activityStatus();if(beforeAbort.active)break;if(performance.now()-acquireStart>5000)throw Error('Cancellation test did not observe an active query');await sleep(1)}while(true);
    document.querySelector('button[aria-label="Expand Search"],button[aria-label="Edit Search"]')?.click();
    await wait(()=>button()&&!button().disabled);
    button().click();
    cancellation={beforeAbort,afterAbort:await globalThis.__activityStatus()};
    const start=performance.now();
    do{activity=await globalThis.__activityStatus();if(!activity.active&&!activity.pending)break;if(performance.now()-start>15000)throw Error('Cancelled worker did not release native activity');await sleep(20)}while(true);
    if(activity.errors)throw Error('Native asynchronous activity failed after cancellation');
    if(activity.policy!==activity.original||activity.token||activity.starts!==activity.ends)throw Error('Cancelled worker did not restore idle policy');
    cancellation.afterFinish=activity;
   }
  }
  const nativeActivityState=globalThis.webkit?.messageHandlers?.snootActivity?await webkit.messageHandlers.snootActivity.postMessage({action:'status'}):undefined;
  await fetch('/__desktop/report',{method:'POST',body:JSON.stringify({ok:true,...runs[0],startup,runs,nativeActivityState,warnings:globalThis.__testWarnings??[],...(activity?{activity}:{}),...(cancellation?{cancellation}:{})})});
 }catch(error){globalThis.__testProgress?.({stage:'driver-error',error:String(error),boundary:globalThis.__snootSearchBoundary,busy:document.querySelector('.paged-results')?.getAttribute('aria-busy'),button:document.querySelector('.query-controls .primary')?.textContent});await fetch('/__desktop/report',{method:'POST',body:JSON.stringify({ok:false,error:String(error),screen:document.body.innerText.slice(-4000),alerts:[...document.querySelectorAll('[role="alert"]')].map(e=>e.textContent)})})}
})();
