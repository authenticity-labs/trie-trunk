//! Screen published row ordinals with the existing hour index. A leaf number
//! belongs to a Definition root; a number alone never identifies the version.
use super::{
    Reader,
    index::{Indexes, WindowReader},
};
use crate::{
    Error, LeafTrie, RowSelection, SeedDefinition, SourceFilter, error,
    query::{Expression, NativeRow},
};
use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// The cursor retains the unread ranges and index page across cooperative polls.
pub(crate) struct IndexWindow {
    reader: WindowReader,
    remaining: std::vec::IntoIter<Range<usize>>,
    roots: Vec<Root>,
    metadata: Option<Arc<super::Metadata>>,
    clean_root: Option<usize>,
}

struct Root {
    definition: Arc<SeedDefinition>,
    trie: Option<Arc<LeafTrie>>,
    expr: Arc<Expression>,
    has_fields: bool,
    cache: [Option<(u16, Option<bool>)>; 64],
}

struct LeafRow<'a> {
    trie: &'a LeafTrie,
    id: u16,
}
impl NativeRow for LeafRow<'_> {
    fn native_value(&self, field: usize) -> Result<pil_codec::api::NativeValue<'_>, Error> {
        self.trie
            .value(self.id, field)?
            .ok_or_else(|| error("field outside leaf"))
    }
    fn is_present(&self, field: usize) -> Result<bool, Error> {
        self.trie.covers(self.id, field)
    }
}

// An uncovered atom requires the PAC. A false conjunct or true disjunct
// still decides its parent. Unknown comparisons stay unresolved under NOT.
fn test(expr: &Expression, row: &LeafRow<'_>) -> Result<Option<bool>, Error> {
    match expr {
        // The full UUID also needs the time for this specific event.
        Expression::EventId(..) => Ok(None),
        Expression::And(parts) | Expression::Or(parts) => {
            let decisive = matches!(expr, Expression::Or(_));
            let mut complete = true;
            for p in parts {
                match test(p, row)? {
                    Some(v) if v == decisive => return Ok(Some(v)),
                    None => complete = false,
                    _ => {}
                }
            }
            Ok(complete.then_some(!decisive))
        }
        Expression::Not(p) => Ok(test(p, row)?.map(|v| !v)),
        _ => {
            if let Some(f) = expr.field() {
                if !row.is_present(f)? {
                    return Ok(None);
                }
            }
            expr.evaluate(row)
        }
    }
}
impl Root {
    fn matches(&mut self, id: u16) -> Result<Option<bool>, Error> {
        let slot = id as usize % self.cache.len();
        if let Some((leaf, result)) = self.cache[slot] {
            if leaf == id {
                return Ok(result);
            }
        }
        let result = self.evaluate(id)?;
        self.cache[slot] = Some((id, result));
        Ok(result)
    }

    fn evaluate(&self, id: u16) -> Result<Option<bool>, Error> {
        let Some(trie) = self.trie.as_deref() else {
            return Ok(None);
        };
        if id == 0 || id as usize > trie.len() || !self.has_fields {
            return Ok(None);
        }
        test(&self.expr, &LeafRow { trie, id })
    }
}

impl Indexes {
    pub(super) fn prune(
        &self,
        reader: &Reader,
        ordinal: usize,
        query: &mut SourceFilter<'_>,
        cancel: &AtomicBool,
        sought: bool,
    ) -> Result<(RowSelection, bool), Error> {
        let mut index = if let Some(index) = query.window.take() {
            index
        } else {
            if self
                .plans
                .values()
                .all(|p| p.leaves.as_ref().is_none_or(|r| r.is_empty()))
            {
                return Ok((reader.selected_rows(ordinal)?, false));
            }
            let mut roots = Vec::new();
            let mut useful = false;
            for plan in self.plans.values() {
                let Some(expr) = query.expression(&plan.definition, sought) else {
                    return Ok((reader.selected_rows(ordinal)?, false));
                };
                let mut fields = Vec::new();
                expr.fields(&mut fields);
                fields.sort_unstable();
                fields.dedup();
                let trie = plan.leaves.as_deref();
                useful |=
                    !fields.is_empty() && trie.is_some_and(|r| fields.iter().any(|&f| r.has_field(f)));
                roots.push(Root {
                    definition: plan.definition.clone(),
                    trie: plan.leaves.clone(),
                    expr,
                    has_fields: !fields.is_empty(),
                    cache: [None; 64],
                });
            }
            if !useful {
                return Ok((reader.selected_rows(ordinal)?, false));
            }
            let time = self.published_hour(ordinal, reader.0.selection.addresses[ordinal])?;
            let count = usize::try_from(time.count).map_err(|_| Error::Overflow)?;
            let mut ranges = match reader.selected_rows(ordinal)? {
                RowSelection::All => vec![0..count],
                RowSelection::Range(r) => vec![r],
                RowSelection::Ranges(v) => v,
            };
            ranges.retain(|r| !r.is_empty());
            if ranges.is_empty() {
                return Ok((RowSelection::Range(0..0), false));
            }
            IndexWindow {
                reader: WindowReader::new(
                    self.rows(reader.0.storage.clone(), reader.0.budget.clone(), &time)?,
                    16384,
                ),
                remaining: ranges.into_iter(),
                roots,
                metadata: None,
                clean_root: None,
            }
        };
        let ranges = query.slice(&mut index.remaining);
        let roots = &mut index.roots;
        let mut selection: Vec<Range<usize>> = Vec::new();
        let mut proven = true;
        let mut candidates = 0;
        let goal = query.scan_goal();
        let mut ranges = ranges.into_iter();
        while let Some(range) = ranges.next() {
            let mut row = range.start;
            while row < range.end {
                if cancel.load(Ordering::Relaxed) {
                    return Err(error("query cancelled"));
                }
                let (_, leaf) = index.reader.row(row)?;
                let mut end = range.end.min(row.saturating_add(256));
                // With no version directory in Time, first try a decision common
                // to every registered root. Only disagreements require PAC metadata.
                let mut result = if let Some(i) = index.clean_root {
                    roots[i].matches(leaf)?
                } else {
                    roots[0].matches(leaf).unwrap_or(None)
                };
                if index.clean_root.is_none()
                    && roots
                        .iter_mut()
                        .skip(1)
                        .any(|r| r.matches(leaf).unwrap_or(None) != result)
                {
                    if index.metadata.is_none() {
                        index.metadata = Some(reader.metadata(ordinal)?);
                    }
                    let m = index.metadata.as_ref().unwrap();
                    let d = if let Some(dir) = m.plan.version_directory() {
                        let entry = dir
                            .range(row as u64)
                            .ok_or_else(|| error("missing row version"))?;
                        end = end.min(usize::try_from(entry.end).map_err(|_| Error::Overflow)?);
                        reader.0.scope.definition(
                            u16::try_from(entry.definition).map_err(|_| Error::Overflow)?,
                        )?
                    } else {
                        &m.definition
                    };
                    let i = roots
                        .iter()
                        .position(|r| r.definition.same_as(d))
                        .ok_or_else(|| error("unknown leaf root"))?;
                    if m.plan.version_directory().is_none() {
                        index.clean_root = Some(i);
                    }
                    result = roots[i].matches(leaf)?;
                }
                if result != Some(false) {
                    end = end.min(row.saturating_add(goal.saturating_sub(candidates).max(1)));
                }
                let start = row;
                row = index.reader.same_leaf_end(start, end, leaf)?;
                if result == Some(false) {
                    continue;
                }
                proven &= result == Some(true);
                if let Some(last) = selection.last_mut().filter(|r| r.end == start) {
                    last.end = row;
                } else {
                    selection.push(start..row);
                }
                candidates += row - start;
                // Bound fragmentation and avoid scanning a large index ahead of LIMIT.
                if candidates >= goal && proven {
                    if query.continues() {
                        *query.window = Some(index);
                    }
                    return Ok((RowSelection::Ranges(selection), true));
                }
                if candidates >= goal || selection.len() >= 512 {
                    if row < range.end {
                        selection.last_mut().unwrap().end = range.end;
                    }
                    selection.extend(ranges);
                    if query.continues() {
                        *query.window = Some(index);
                    }
                    return Ok((RowSelection::Ranges(selection), false));
                }
            }
        }
        if query.continues() {
            *query.window = Some(index);
        }
        Ok((RowSelection::Ranges(selection), proven))
    }
}
