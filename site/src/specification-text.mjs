// Plain-text publication of the specification. Source examples remain verbatim.
const WIDTH = 78;

function wrap(text, width) {
  const lines = [];
  let line = '';
  for (const word of text.split(/\s+/).filter(Boolean)) {
    if (line && line.length + word.length + 1 > width) {
      lines.push(line);
      line = '';
    }
    line += (line ? ' ' : '') + word;
  }
  if (line) lines.push(line);
  return lines.length ? lines : [''];
}

function cells(line) {
  const result = [];
  let cell = '', quoted = false;
  for (const char of line.trim().replace(/^\||\|$/g, '')) {
    if (char === '`') quoted = !quoted;
    if (char === '|' && !quoted) { result.push(cell.trim()); cell = ''; }
    else cell += char;
  }
  result.push(cell.trim());
  return result;
}

export function renderSpecificationText(source) {
  const lines = source.trim().split('\n');
  const output = [], headings = [], references = [];
  const inline = text => text
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label, url) => {
      let index = references.indexOf(url);
      if (index < 0) index = references.push(url) - 1;
      return `${label} [${index + 1}]`;
    })
    .replace(/`([^`]+)`/g, '$1')
    .replace(/\*\*([^*]+)\*\*/g, '$1');
  let section = '', subsection = 0;
  const paragraph = (text, prefix = '   ') => {
    const continuation = ' '.repeat(prefix.length);
    output.push(...wrap(inline(text), WIDTH - prefix.length)
      .map((line, index) => (index ? continuation : prefix) + line), '');
  };
  for (let i = 0; i < lines.length;) {
    const line = lines[i];
    if (!line.trim()) { i++; continue; }
    if (line.startsWith('```')) {
      i++;
      while (i < lines.length && !lines[i].startsWith('```')) output.push('   ' + lines[i++]);
      i++;
      output.push('');
      continue;
    }
    const heading = /^(#{1,4})\s+(.+)$/.exec(line);
    if (heading) {
      let title = inline(heading[2]);
      if (/^Appendix A\./.test(title)) { section = 'A'; subsection = 0; }
      else if (/^\d+\./.test(title)) { section = title.match(/^\d+/)[0]; subsection = 0; }
      else if (heading[1].length >= 3) title = `${section}.${++subsection}. ${title}`;
      headings.push(title);
      output.push('', title, '-'.repeat(Math.min(title.length, WIDTH)), '');
      i++;
      continue;
    }
    if (line.trim().startsWith('|')) {
      const rows = [];
      while (i < lines.length && lines[i].trim().startsWith('|')) {
        const row = cells(lines[i++]);
        if (!row.every(cell => /^:?-+:?$/.test(cell))) rows.push(row.map(inline));
      }
      const count = rows[0].length;
      if (rows.some(row => row.length !== count)) throw Error('Uneven specification table');
      const widths = rows[0].map((_, column) => Math.min(36, Math.max(8, ...rows.map(row => row[column].length))));
      while (widths.reduce((a, b) => a + b, 0) + (count - 1) * 3 > WIDTH - 3) {
        const widest = widths.indexOf(Math.max(...widths));
        if (widths[widest] <= 8) throw Error('Specification table exceeds text width');
        widths[widest]--;
      }
      rows.forEach((row, rowIndex) => {
        const wrapped = row.map((cell, column) => wrap(cell, widths[column]));
        for (let n = 0; n < Math.max(...wrapped.map(cell => cell.length)); n++)
          output.push('   ' + wrapped.map((cell, column) => (cell[n] ?? '').padEnd(widths[column])).join('   ').trimEnd());
        if (rowIndex === 0) output.push('   ' + widths.map(width => '-'.repeat(width)).join('   '));
        else if (wrapped.some(cell => cell.length > 1)) output.push('');
      });
      output.push('');
      continue;
    }
    const list = /^(?:- |\d+[.)] )/.exec(line);
    const parts = [list ? line.slice(list[0].length) : line];
    i++;
    while (i < lines.length && lines[i].trim() && !/^(?:#|```|\||- |\d+[.)] )/.test(lines[i])) parts.push(lines[i++].trim());
    paragraph(parts.join(' '), list ? '   ' + list[0] : '   ');
  }
  const contents = headings.flatMap(title => wrap(title, WIDTH - 3).map(line => '   ' + line));
  const refs = references.length ? ['References', '----------', '', ...references.flatMap((url, i) => [`   [${i + 1}] ${url}`, ''])] : [];
  return [
    'PIL: Positional Interchange Layout',
    'Format and Library Specification',
    '',
    'Contents',
    '--------',
    '',
    ...contents, '', ...output, ...refs,
  ].join('\n').replace(/\n{4,}/g, '\n\n\n').trimEnd() + '\n';
}
