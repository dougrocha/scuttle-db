//! Sorting more records than fit in memory.
//!
//! Records are held until they reach a memory budget, then sorted and written to a
//! temporary file as one sorted "run". At the end the runs are merged: a min-heap holds the
//! next record of each run, and the smallest is popped and replaced by the next record of
//! its run. Only one record per run is in memory while merging.
//!
//! Merging every run at once would hold a file open for each, and with a small budget on a
//! big input that can be more files than the OS allows. So runs are merged at most
//! [`MERGE_FAN_IN`] at a time: while there are more, each group of that many is merged
//! into one longer run, and that is one more pass over the data. With B runs merged at a
//! time and N runs, that takes ⌈log_B(N)⌉ passes.
//!
//! The sort is stable: records that compare equal come out in the order they went in.
//! Each run is sorted with a stable sort, a merge breaks ties by run (earlier runs came
//! earlier in the input), and a merge pass only ever merges runs that sit next to each
//! other, putting the result where they were.

use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    path::{Path, PathBuf},
    rc::Rc,
};

use miette::Result;

use crate::storage::spill::{SpillFile, SpillReader, SpillWriter};

/// Most runs merged at once, so most spill files open at once. Each open run also holds
/// one read buffer (8 KB) outside the memory budget.
pub(crate) const MERGE_FAN_IN: usize = 64;

/// Something an [`ExternalSort`] can write to a run and read back.
pub(crate) trait Record: Sized {
    fn encode(&self, out: &mut Vec<u8>);
    fn decode(record: &[u8]) -> Result<Self>;

    /// About how many bytes the record points to on the heap, for the memory budget.
    fn heap_size(&self) -> usize;
}

/// How two records compare.
type Compare<T> = Rc<dyn Fn(&T, &T) -> Ordering>;

/// Where runs go, and how many bytes of records are held before one is written.
#[derive(Debug, Clone)]
pub(crate) struct Spill {
    pub(crate) dir: PathBuf,
    pub(crate) budget: usize,
}

/// Sorts records pushed into it, writing sorted runs to disk past the memory budget.
pub(crate) struct ExternalSort<T> {
    compare: Compare<T>,

    /// `None` sorts in memory however many records come in.
    spill: Option<Spill>,
    fan_in: usize,

    /// Records not yet written to a run, and what they point to on the heap.
    held: Vec<T>,
    held_bytes: usize,
    runs: Vec<SpillFile>,
}

impl<T: Record + 'static> ExternalSort<T> {
    pub(crate) fn new(
        compare: impl Fn(&T, &T) -> Ordering + 'static,
        spill: Option<Spill>,
    ) -> Self {
        Self {
            compare: Rc::new(compare),
            spill,
            fan_in: MERGE_FAN_IN,
            held: Vec::new(),
            held_bytes: 0,
            runs: Vec::new(),
        }
    }

    /// Merges at most `fan_in` runs at once instead of [`MERGE_FAN_IN`], so tests can make
    /// several passes with few runs.
    #[cfg(test)]
    fn with_fan_in(mut self, fan_in: usize) -> Self {
        self.fan_in = fan_in.max(2);
        self
    }

    pub(crate) fn push(&mut self, record: T) -> Result<()> {
        self.held_bytes += record.heap_size();
        self.held.push(record);

        // The list itself is counted at its capacity, since a Vec that has grown holds up
        // to twice the room its records use
        let list_bytes = self.held.capacity() * size_of::<T>();
        if let Some(spill) = &self.spill
            && self.held_bytes + list_bytes >= spill.budget
        {
            let run = self.write_run(spill.dir.clone())?;
            self.runs.push(run);
        }
        Ok(())
    }

    /// Sorts the held records and writes them out as one run.
    fn write_run(&mut self, dir: PathBuf) -> Result<SpillFile> {
        let mut held = std::mem::take(&mut self.held);
        self.held_bytes = 0;
        held.sort_by(|a, b| (self.compare)(a, b));
        write(&dir, held.into_iter().map(Ok))
    }

    /// Every record pushed, in order.
    pub(crate) fn finish(mut self) -> Result<Sorted<T>> {
        // Nothing was written out, so everything fit in memory
        let Some(spill) = self.spill.clone().filter(|_| !self.runs.is_empty()) else {
            let mut held = self.held;
            held.sort_by(|a, b| (self.compare)(a, b));
            return Ok(Sorted {
                source: Source::Memory(held.into_iter()),
                runs: 0,
                passes: 0,
            });
        };
        if !self.held.is_empty() {
            let run = self.write_run(spill.dir.clone())?;
            self.runs.push(run);
        }

        let runs = self.runs.len();
        let mut passes = 1;
        while self.runs.len() > self.fan_in {
            let mut merged = Vec::new();
            let mut left = std::mem::take(&mut self.runs).into_iter();
            loop {
                let group: Vec<SpillFile> = left.by_ref().take(self.fan_in).collect();
                match group.len() {
                    0 => break,
                    1 => merged.extend(group),
                    _ => {
                        let mut merge = Merge::open(group, Rc::clone(&self.compare))?;
                        merged.push(write(
                            &spill.dir,
                            std::iter::from_fn(|| merge.next().transpose()),
                        )?);
                    }
                }
            }
            self.runs = merged;
            passes += 1;
        }

        Ok(Sorted {
            source: Source::Merge(Merge::open(self.runs, self.compare)?),
            runs,
            passes,
        })
    }
}

/// Writes `records` to a new run in `dir`.
fn write<T: Record>(dir: &Path, records: impl Iterator<Item = Result<T>>) -> Result<SpillFile> {
    let mut writer = SpillWriter::create(dir)?;
    let mut bytes = Vec::new();
    for record in records {
        bytes.clear();
        record?.encode(&mut bytes);
        writer.write(&bytes)?;
    }
    writer.finish()
}

/// The records of an [`ExternalSort`], in order.
pub(crate) struct Sorted<T> {
    source: Source<T>,

    /// Runs written to disk: 0 when everything fit in memory.
    pub(crate) runs: usize,

    /// Passes the merge makes over the data, the last one being this read.
    pub(crate) passes: usize,
}

enum Source<T> {
    Memory(std::vec::IntoIter<T>),
    Merge(Merge<T>),
}

impl<T> std::fmt::Debug for Sorted<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sorted")
            .field("runs", &self.runs)
            .field("passes", &self.passes)
            .finish_non_exhaustive()
    }
}

impl<T: Record> Sorted<T> {
    /// The next record, or `None` after the last.
    pub(crate) fn next(&mut self) -> Result<Option<T>> {
        match &mut self.source {
            Source::Memory(records) => Ok(records.next()),
            Source::Merge(merge) => merge.next(),
        }
    }
}

/// Merges sorted runs into one sorted stream.
struct Merge<T> {
    runs: Vec<SpillReader>,

    /// The next record of each run that has any left. The smallest is on top.
    heap: BinaryHeap<Reverse<RunHead<T>>>,
    compare: Compare<T>,
}

impl<T: Record> Merge<T> {
    fn open(runs: Vec<SpillFile>, compare: Compare<T>) -> Result<Self> {
        let mut merge = Self {
            runs: runs
                .into_iter()
                .map(SpillFile::open)
                .collect::<Result<_>>()?,
            heap: BinaryHeap::new(),
            compare,
        };
        for run in 0..merge.runs.len() {
            merge.refill(run)?;
        }
        Ok(merge)
    }

    fn next(&mut self) -> Result<Option<T>> {
        let Some(Reverse(head)) = self.heap.pop() else {
            return Ok(None);
        };
        self.refill(head.run)?;
        Ok(Some(head.record))
    }

    /// Puts the next record of `run` on the heap, if it has one.
    fn refill(&mut self, run: usize) -> Result<()> {
        if let Some(bytes) = self.runs[run].read()? {
            self.heap.push(Reverse(RunHead {
                record: T::decode(&bytes)?,
                run,
                compare: Rc::clone(&self.compare),
            }));
        }
        Ok(())
    }
}

/// The next record of one run. Ties go to the earlier run, which keeps the sort stable.
struct RunHead<T> {
    record: T,
    run: usize,
    compare: Compare<T>,
}
impl<T> Ord for RunHead<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.compare)(&self.record, &other.record).then(self.run.cmp(&other.run))
    }
}
impl<T> PartialOrd for RunHead<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> PartialEq for RunHead<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl<T> Eq for RunHead<T> {}

/// A byte string, such as an index entry, sorts as itself.
impl Record for Vec<u8> {
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self);
    }

    fn decode(record: &[u8]) -> Result<Self> {
        Ok(record.to_vec())
    }

    fn heap_size(&self) -> usize {
        self.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key to sort by and the position it was pushed at, to check ties keep their order.
    #[derive(Debug, Clone, PartialEq)]
    struct Item {
        key: u8,
        pos: u32,
    }
    impl Record for Item {
        fn encode(&self, out: &mut Vec<u8>) {
            out.push(self.key);
            out.extend_from_slice(&self.pos.to_le_bytes());
        }
        fn decode(record: &[u8]) -> Result<Self> {
            Ok(Item {
                key: record[0],
                pos: u32::from_le_bytes(record[1..5].try_into().unwrap()),
            })
        }
        fn heap_size(&self) -> usize {
            0
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("scuttle-xsort-{name}-{}", std::process::id()))
    }

    fn sort(
        dir: PathBuf,
        items: &[Item],
        budget: Option<usize>,
        fan_in: usize,
    ) -> (Vec<Item>, usize, usize) {
        let spill = budget.map(|budget| Spill { dir, budget });
        let mut sort =
            ExternalSort::new(|a: &Item, b: &Item| a.key.cmp(&b.key), spill).with_fan_in(fan_in);
        for item in items {
            sort.push(item.clone()).unwrap();
        }

        let mut sorted = sort.finish().unwrap();
        let mut out = Vec::new();
        while let Some(item) = sorted.next().unwrap() {
            out.push(item);
        }
        (out, sorted.runs, sorted.passes)
    }

    #[test]
    fn test_every_way_of_sorting_gives_the_same_stable_order() {
        // Few keys, so most items tie and only their positions tell them apart
        let items: Vec<Item> = (0..2000)
            .map(|pos| Item {
                key: (pos * 7 % 5) as u8,
                pos,
            })
            .collect();
        let mut expected = items.clone();
        expected.sort_by_key(|item| item.key);

        assert_eq!(
            sort(temp_dir("order"), &items, None, 64),
            (expected.clone(), 0, 0)
        );
        // About 64 items per run: 31 runs, merged at once
        let (out, runs, passes) = sort(temp_dir("order"), &items, Some(1024), 64);
        assert_eq!((out, passes), (expected.clone(), 1));
        assert!(runs > 20, "{runs} runs");
        // The same runs, merged 3 at a time
        let (out, _, passes) = sort(temp_dir("order"), &items, Some(1024), 3);
        assert_eq!(out, expected);
        assert!(passes >= 4, "{passes} passes");
    }

    #[test]
    fn test_the_runs_are_deleted_once_merged() {
        let dir = temp_dir("cleanup");
        let items: Vec<Item> = (0..500).map(|pos| Item { key: 0, pos }).collect();
        let (out, ..) = sort(dir.clone(), &items, Some(256), 2);
        assert_eq!(out.len(), 500);
        assert!(std::fs::read_dir(&dir).is_ok_and(|mut files| files.next().is_none()));
    }
}
