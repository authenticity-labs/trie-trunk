import {quotaRings} from './quota-rings.mjs';
import {stages} from './lifecycle-data.js';

function sectionFromHash() {
 if (!location.hash) return null;
 try {
  return document.getElementById(decodeURIComponent(location.hash.slice(1)));
 } catch {
  return null;
 }
}

function clearSectionHash() {
 if (location.href.includes('#')) {
  history.replaceState(history.state, '', location.pathname + location.search);
 }
}

// Show one brand at a time: the main lockup hands off to the sticky masthead.
const mainBrand = document.querySelector('.main-brand');
if(mainBrand && 'IntersectionObserver' in window) {
 let brandObserver;
 const observeBrand = () => {
  brandObserver?.disconnect();
  const headerHeight = document.querySelector('.site-header').getBoundingClientRect().height;
  brandObserver = new IntersectionObserver(entries=>{
   document.body.classList.toggle('main-brand-out',!entries[0].isIntersecting);
  },{rootMargin:`-${headerHeight}px 0px 0px 0px`});
  brandObserver.observe(mainBrand);
 };
 observeBrand();
 window.addEventListener('resize',observeBrand);
}

const tabs = [...document.querySelectorAll('[data-stage]')];
function select(index, focus = false) {
 const stage = stages[index];
 if (!stage) return;
 tabs.forEach((tab,i) => {
  tab.setAttribute('aria-selected',String(index===i));
  tab.tabIndex=index===i?0:-1;
 });
 document.querySelector('#stage-panel').setAttribute('aria-labelledby',`stage-${index}`);
 document.querySelector('#stage-place').textContent=stage.location;
 document.querySelector('#stage-title').textContent=stage.action;
 const acronym = document.querySelector('#stage-acronym');
 acronym.textContent=stage.acronym;
 acronym.hidden=!stage.acronym;
 const note = document.querySelector('#stage-note');
 // The linked notation is static, authored lifecycle copy.
 note.innerHTML=stage.note??'';
 note.hidden=!stage.note;
 document.querySelector('#stage-copy').textContent=stage.copy;
 document.querySelector('#stage-foot').textContent=stage.foot;
 // Scene markup is static, authored build data; no remote or user input enters it.
 document.querySelector('#stage-scene').innerHTML=stage.scene;
 if (focus) tabs[index].focus();
}
tabs.forEach((tab,i) => {
 tab.addEventListener('click',()=>select(i));
 tab.addEventListener('keydown',event=>{
  let next;
  if(event.key==='ArrowRight')next=(i+1)%tabs.length;
  if(event.key==='ArrowLeft')next=(i+tabs.length-1)%tabs.length;
  if(event.key==='Home')next=0;
  if(event.key==='End')next=tabs.length-1;
  if(next!==undefined){event.preventDefault();select(next,true)}
 });
});

const syntaxTabs = [...document.querySelectorAll('[data-syntax]')];
function selectSyntax(index, focus = false) {
 syntaxTabs.forEach((tab,i)=>{
  const selected=i===index;
  tab.setAttribute('aria-selected',String(selected));
  tab.tabIndex=selected?0:-1;
  document.getElementById(tab.getAttribute('aria-controls')).hidden=!selected;
 });
 if(focus)syntaxTabs[index].focus();
}
syntaxTabs.forEach((tab,i)=>{
 tab.addEventListener('click',()=>selectSyntax(i));
 tab.addEventListener('keydown',event=>{
  let next;
  if(event.key==='ArrowRight')next=(i+1)%syntaxTabs.length;
  if(event.key==='ArrowLeft')next=(i+syntaxTabs.length-1)%syntaxTabs.length;
  if(event.key==='Home')next=0;
  if(event.key==='End')next=syntaxTabs.length-1;
  if(next!==undefined){event.preventDefault();selectSyntax(next,true)}
 });
});

const themeButtons = [...document.querySelectorAll('[data-theme-choice]')];
const systemTheme = matchMedia('(prefers-color-scheme: dark)');
let themePreference = document.documentElement.dataset.themePreference ?? 'auto';
function applyTheme(preference) {
 themePreference = ['light','dark'].includes(preference) ? preference : 'auto';
 const theme = themePreference==='auto' ? (systemTheme.matches?'dark':'light') : themePreference;
 document.documentElement.dataset.theme = theme;
 document.documentElement.dataset.themePreference = themePreference;
 document.querySelector('meta[name="theme-color"]').content=theme==='dark'?'#181818':'#fafafa';
 themeButtons.forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.themeChoice===themePreference)));
}
themeButtons.forEach(button=>button.addEventListener('click',()=>{
 applyTheme(button.dataset.themeChoice);
 try { localStorage.setItem('trietrunk.theme',themePreference); } catch {}
}));
systemTheme.addEventListener('change',()=>{if(themePreference==='auto')applyTheme('auto')});
window.addEventListener('storage',event=>{
 if(event.key==='trietrunk.theme'||event.key===null)applyTheme(event.newValue);
});
applyTheme(themePreference);

// Keep the three trie explanations in one view, including direct section links.
const trieTabs = [...document.querySelectorAll('[data-trie-tab]')];
function selectTrie(index, focus = false, updateHash = false) {
 if (!trieTabs[index]) return;
 trieTabs.forEach((tab,i)=>{
  const selected=i===index;
  tab.setAttribute('aria-selected',String(selected));
  tab.tabIndex=selected?0:-1;
  document.getElementById(tab.dataset.trieTab).hidden=!selected;
 });
 if (focus) trieTabs[index].focus();
 if (updateHash) history.replaceState(null,'',index===0?'#tries':`#${trieTabs[index].dataset.trieTab}`);
}
trieTabs.forEach((tab,index)=>{
 tab.addEventListener('click',()=>selectTrie(index,false,true));
 tab.addEventListener('keydown',event=>{
  let next;
  if(event.key==='ArrowRight')next=(index+1)%trieTabs.length;
  if(event.key==='ArrowLeft')next=(index+trieTabs.length-1)%trieTabs.length;
  if(event.key==='Home')next=0;
  if(event.key==='End')next=trieTabs.length-1;
  if(next!==undefined){event.preventDefault();selectTrie(next,true,true)}
 });
});
function selectTrieFromHash() {
 const id=location.hash.slice(1);
 const index=id==='tries'?0:trieTabs.findIndex(tab=>tab.dataset.trieTab===id);
 if(index<0||!trieTabs.length)return;
 selectTrie(index);
 document.getElementById('tries').scrollIntoView({block:'start',behavior:'instant'});
}
window.addEventListener('hashchange',selectTrieFromHash);
selectTrieFromHash();

// Highlight the same taxonomy tier in the diagram and its explanatory controls.
const taxonomyButtons = [...document.querySelectorAll('[data-taxonomy-tier]')];
taxonomyButtons.forEach(button=>button.addEventListener('click',()=>{
 const tier=button.dataset.taxonomyTier;
 document.querySelector('.taxonomy-mobile-copy').textContent=button.querySelector('span>span').textContent;
 taxonomyButtons.forEach(item=>item.setAttribute('aria-pressed',String(item===button)));
 document.querySelectorAll('[data-tree-tier]').forEach(node=>{
  node.classList.toggle('is-active',node.dataset.treeTier===tier);
 });
}));

const aggregatePeriods = ['Yearly','Monthly','Daily','Hourly','15-Minute','5-Minute'];
const aggregateCounts = [8,12,30,24,96,288];
const aggregateLabels = [['Year 1','Year 8'],['Jan','Dec'],['Day 1','Day 30'],['00:00','23:00'],['00:00','23:45'],['00:00','23:55']];
document.querySelectorAll('[data-aggregate-period]').forEach(button=>button.addEventListener('click',()=>{
 const index=Number(button.dataset.aggregatePeriod), count=aggregateCounts[index];
 const group=document.getElementById('aggregate-bars');
 group.replaceChildren();
 for(let i=0;i<count;i++) {
  const rect=document.createElementNS('http://www.w3.org/2000/svg','rect');
  const height=25+(Math.sin(i/count*12)*.5+.5)*40+((i*31+17)%30);
  const width=420/count;
  Object.entries({x:i*width+width*.16,y:125-height,width:width*.68,height,rx:Math.min(2,width*.1)}).forEach(([key,value])=>rect.setAttribute(key,String(value)));
  group.append(rect);
 }
 document.getElementById('aggregate-title').textContent=`${aggregatePeriods[index]} View`;
 document.getElementById('aggregate-from').textContent=aggregateLabels[index][0];
 document.getElementById('aggregate-to').textContent=aggregateLabels[index][1];
 document.querySelectorAll('[data-aggregate-period]').forEach(item=>item.setAttribute('aria-pressed',String(item===button)));
}));
const retentionBudget=document.getElementById('retention-budget');
// This public illustration uses an explicit example rate, not a live measurement.
const exampleGBPerDay=4;
const updateRetention=(gigabytes)=>{
 const value=Math.min(Number(retentionBudget.max),Math.max(Number(retentionBudget.min),gigabytes));
 const days=value/exampleGBPerDay;
 const format=n=>n.toLocaleString(undefined,{maximumFractionDigits:1});
 const wholeDays=days.toLocaleString(undefined,{maximumFractionDigits:0});
 retentionBudget.value=String(value);
 document.getElementById('retention-value').textContent=`${format(value)} GB`;
 document.getElementById('retention-days').textContent=`${wholeDays} Days`;
 retentionBudget.setAttribute('aria-valuetext',`${format(value)} GB, approximately ${wholeDays} Days`);
 const group=document.getElementById('quota-rings');
 group.replaceChildren(...quotaRings(value,0,256).map(p=>{
  const path=document.createElementNS('http://www.w3.org/2000/svg','path');
  path.setAttribute('class',`quota-${p.role}`);path.setAttribute('d',p.d);return path;
 }));
};
retentionBudget?.addEventListener('input',()=>updateRetention(Number(retentionBudget.value)));

// Clipboard feedback stays with the command and is announced to screen readers.
for (const button of document.querySelectorAll('[data-copy-code]')) {
 let reset;
 button.addEventListener('click', async () => {
  const code = document.getElementById(button.dataset.copyCode);
  if (!code) return;
  const feedback = button.querySelector('[data-copy-feedback]');
  clearTimeout(reset);
  button.disabled = true;
  try {
   await navigator.clipboard.writeText(code.textContent);
   button.dataset.copied = 'true';
   feedback.textContent = 'Copied';
  } catch {
   delete button.dataset.copied;
   for(let parent=code.parentElement;parent;parent=parent.parentElement)if(parent instanceof HTMLDetailsElement)parent.open=true;
   const range = document.createRange();
   range.selectNodeContents(code);
   const selection = window.getSelection();
   selection.removeAllRanges();
   selection.addRange(range);
   feedback.textContent = 'Copy Selected Text';
  } finally {
   button.disabled = false;
   reset = setTimeout(() => {
    delete button.dataset.copied;
    feedback.textContent = '';
   }, 3000);
  }
 });
}

const sectionShares = {
 PIL: [
  ['.pil-hero h1', ''],
  ['#libraries > h2', 'libraries', 'section'],
  ['#definition > h2', 'definition', 'section'],
  ['#bench > h2', 'bench', 'section'],
 ],
 trunk: [
  ['#snoot-title', 'snoot'],
  ['#lifecycle .section-heading > h2', 'seeds'],
  ['#bench-title', 'bench', 'section'],
 ],
};

for (const [selector, sectionId, tabScope] of sectionShares[document.body.dataset.page] ?? []) {
 const heading = document.querySelector(selector);
 if (!heading) continue;
 const name = heading.innerText.replace(/\s+/g, ' ').trim();
 const label = `Share ${name}`;
 const button = document.createElement('button');
 button.type = 'button';
 button.className = 'section-share';
 button.title = label;
 button.setAttribute('aria-label', label);
 heading.setAttribute('aria-label', name);
 button.innerHTML = `<svg class="section-share-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><path d="m8.6 10.5 6.8-4m-6.8 7 6.8 4"/></svg><svg class="section-share-check" viewBox="0 0 24 24" aria-hidden="true"><path d="m5 12 4 4L19 6"/></svg><span class="copy-feedback" role="status"></span>`;
 heading.append(button);
 const status = button.querySelector('[role=status]');
 let reset;
 button.addEventListener('click', async () => {
  const activeTab = tabScope && heading.closest(tabScope)?.querySelector('[role=tab][aria-selected=true]');
  const url = new URL(location.pathname, location.origin);
  url.hash = activeTab?.getAttribute('aria-controls') || sectionId;
  try {
   await navigator.clipboard.writeText(url.href);
   clearTimeout(reset);
   button.dataset.copied = 'true';
   status.textContent = 'Link Copied';
   reset = setTimeout(() => {
    delete button.dataset.copied;
    status.textContent = '';
   }, 2000);
  } catch {
   window.prompt('Copy Section Link', url.href);
  }
 });
}

// Independent notebook and language tabs; deep links reveal their target pane.
for(const group of document.querySelectorAll('[data-pil-tabs]')){
 const list=group.querySelector('[role=tablist]');
 const tabs=[...list.querySelectorAll('[role=tab]')];
 const panels=tabs.map(tab=>document.getElementById(tab.getAttribute('aria-controls')));
 const select=(index,focus=false,align=false)=>{
  tabs.forEach((tab,i)=>{tab.setAttribute('aria-selected',String(i===index));tab.tabIndex=i===index?0:-1;panels[i].hidden=i!==index;});
  if(focus)tabs[index].focus();
  if(align&&group.hasAttribute('data-pil-deep-links')){
   const headerBottom=document.querySelector('.site-header')?.getBoundingClientRect().bottom??0;
   if(panels[index].getBoundingClientRect().top<headerBottom)panels[index].scrollIntoView({block:'start'});
  }
  group.dispatchEvent(new CustomEvent('pil-tab-change',{bubbles:true}));
 };
 tabs.forEach((tab,index)=>{
  tab.addEventListener('click',()=>select(index,false,true));
  tab.addEventListener('keydown',event=>{
   const vertical=list.getAttribute('aria-orientation')==='vertical';
   let next;
   if(event.key===(vertical?'ArrowDown':'ArrowRight'))next=(index+1)%tabs.length;
   if(event.key===(vertical?'ArrowUp':'ArrowLeft'))next=(index+tabs.length-1)%tabs.length;
   if(event.key==='Home')next=0;
   if(event.key==='End')next=tabs.length-1;
   if(next!==undefined){event.preventDefault();select(next,true,true);}
  });
 });
 const reveal=()=>{
  const target=sectionFromHash();
  if(!target)return;
  const index=panels.findIndex(panel=>panel===target||panel.contains(target));
  if(index<0)return;
  select(index);
  for(let ancestor=target;ancestor&&ancestor!==group;ancestor=ancestor.parentElement){if(ancestor.tagName==='DETAILS')ancestor.open=true;}
 };
 reveal();window.addEventListener('hashchange',reveal);
}

// Keep section headings clear of the responsive sticky header.
if(document.body.dataset.page==='PIL'){
 const root=document.documentElement;
 const header=document.querySelector('.site-header');
 const updateClearance=()=>{
  root.style.setProperty('--pil-header-clearance',`${Math.ceil(header?.getBoundingClientRect().height??0)+16}px`);
  for(const section of document.querySelectorAll('#main>section[id]')){
   const heading=section.querySelector('h2');
   if(heading)section.style.setProperty('--pil-heading-inset',`${heading.getBoundingClientRect().top-section.getBoundingClientRect().top}px`);
  }
 };
 updateClearance();
 root.classList.add('pil-section-snap');
 const links=[...header.querySelectorAll('[data-pil-section]')];
 const sections=links.map(link=>document.getElementById(link.dataset.pilSection)?.closest('section'));
 // Direct navigation should reach its target, not stop at an intermediate snap in WebKit.
 links.forEach((link,index)=>link.addEventListener('click',event=>{
  if(event.defaultPrevented||event.button!==0||event.metaKey||event.ctrlKey||event.shiftKey||event.altKey)return;
  event.preventDefault();
  history.pushState(history.state,'',location.pathname+location.search);
  sections[index].scrollIntoView({block:'start',behavior:'instant'});
  updateSection();
 }));
 let pending=false;
 const updateSection=()=>{
  pending=false;
  const threshold=header.getBoundingClientRect().bottom+window.innerHeight*.25;
  let current=0;
  sections.forEach((section,index)=>{if(section.getBoundingClientRect().top<=threshold)current=index;});
  links.forEach((link,index)=>{if(index===current)link.setAttribute('aria-current','location');else link.removeAttribute('aria-current');});
 };
 const scheduleSection=()=>{if(!pending){pending=true;requestAnimationFrame(updateSection);}};
 // Arrow navigation follows the two axes of the guide. Scrolling remains native.
 document.addEventListener('keydown',event=>{
  if(event.defaultPrevented||event.altKey||event.ctrlKey||event.metaKey||event.shiftKey)return;
  if(!['ArrowLeft','ArrowRight','ArrowUp','ArrowDown'].includes(event.key))return;
  const target=event.target;
  if(target.closest('input,textarea,select,[contenteditable="true"],summary'))return;
  if(target.closest('button')&&!target.closest('[role=tab]'))return;
  if(window.getSelection()?.toString())return;
  const focusedLink=links.indexOf(target.closest('[data-pil-section]'));
  const focusedSection=sections.indexOf(target.closest('#main>section[id]'));
  const current=focusedLink>=0?focusedLink:focusedSection>=0?focusedSection:Math.max(0,links.findIndex(link=>link.getAttribute('aria-current')==='location'));
  if(event.key==='ArrowLeft'||event.key==='ArrowRight'){
   event.preventDefault();
   const next=(current+(event.key==='ArrowRight'?1:-1)+links.length)%links.length;
   links[next].click();
   links[next].focus({preventScroll:true});
  }else{
   const tabs=[...sections[current].querySelectorAll('[role=tab]')];
   if(!tabs.length)return;
   event.preventDefault();
   const selected=Math.max(0,tabs.findIndex(tab=>tab.getAttribute('aria-selected')==='true'));
   const next=(selected+(event.key==='ArrowDown'?1:-1)+tabs.length)%tabs.length;
   tabs[next].click();
   tabs[next].focus({preventScroll:true});
  }
 });
 window.addEventListener('scroll',scheduleSection,{passive:true});
 window.addEventListener('resize',scheduleSection);
 document.addEventListener('pil-tab-change',updateSection);
 // Notebook listeners reveal hidden targets before this handler scrolls to them.
 const followFragment=()=>{
  const hash=location.hash;
  if(!location.href.includes('#'))return;
  requestAnimationFrame(()=>{
   if(location.hash!==hash)return;
   sectionFromHash()?.scrollIntoView({block:'start',behavior:'instant'});
   clearSectionHash();
   updateSection();
  });
 };
 window.addEventListener('hashchange',followFragment);
 followFragment();
 updateSection();
 document.fonts.ready.then(updateClearance);
 window.addEventListener('resize',updateClearance);
 if(header)new ResizeObserver(updateClearance).observe(header);
}
