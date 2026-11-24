const escape = value => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
const keywords = new Set('as async await break const defer else enum export false finally fn for from func if import in let match mod mut nil null package pub return struct throw true try type use var while'.split(' '));

// Highlight source at build time; the clipboard still receives plain source text.
export function highlight(code) {
 const tokens = /\/\/[^\n]*|\/\*[\s\S]*?\*\/|"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\\n])*'|`(?:\\.|[^`\\])*`|\b(?:0x[\da-fA-F]+|\d+(?:\.\d+)?)\b|\b[A-Za-z_]\w*\b/g;
 let html = '', end = 0;
 for (const match of code.matchAll(tokens)) {
  const value = match[0];
  const kind = value.startsWith('//') || value.startsWith('/*') ? 'comment'
   : /^["'`]/.test(value) ? 'string'
   : /^\d/.test(value) ? 'number'
   : keywords.has(value) ? 'keyword'
   : /^\s*\(/.test(code.slice(match.index + value.length)) ? 'function'
   : /^[A-Z]/.test(value) ? 'type' : null;
  html += escape(code.slice(end, match.index)) + (kind ? `<span class="code-${kind}">${escape(value)}</span>` : escape(value));
  end = match.index + value.length;
 }
 return html + escape(code.slice(end));
}

export function highlightShell(code) {
 const tokens = /#[^\n]*|"(?:\\.|[^"\\])*"|'[^']*'|&&|\|\||[|;]|[^\s|;&]+/g;
 let html = '', end = 0, command = true, rxAction = false;
 for (const match of code.matchAll(tokens)) {
  const gap = code.slice(end, match.index), value = match[0];
  if (gap.includes('\n')) { command = true; rxAction = false; }
  let kind;
  if (value.startsWith('#')) kind = 'comment';
  else if (/^(?:&&|\|\||[|;])$/.test(value)) {
   kind = 'keyword'; command = true; rxAction = false;
  } else {
   kind = command ? 'function' : rxAction || value.startsWith('-') ? 'keyword'
    : /^["']/.test(value) || /[./]/.test(value) ? 'string' : null;
   rxAction = command && value === 'rx';
   command = false;
  }
  html += escape(gap) + (kind ? `<span class="code-${kind}">${escape(value)}</span>` : escape(value));
  end = match.index + value.length;
 }
 return html + escape(code.slice(end));
}
