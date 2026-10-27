//! Lays out query results as a text table, the way psql does.

use crate::{Row, Value};

/// Writes a result table a batch of rows at a time, so rows can be printed as a query
/// produces them.
///
/// Each column is as wide as its widest value among the header and the first batch. A
/// wider value in a later batch runs past its column, as in psql with `FETCH_COUNT` set:
/// the header is printed before the later rows exist.
pub(crate) struct ResultTable {
    widths: Vec<usize>,
    rows: usize,
}

impl ResultTable {
    /// Writes the header to `out`, with columns sized to fit `columns` and `first_rows`.
    pub(crate) fn new(columns: &[String], first_rows: &[Row], out: &mut String) -> Self {
        let cells = cells(first_rows);
        let widths = columns
            .iter()
            .enumerate()
            .map(|(i, name)| {
                cells
                    .iter()
                    .filter_map(|row| row.get(i))
                    .chain([name])
                    .map(|cell| cell.chars().count())
                    .max()
                    .unwrap_or(0)
            })
            .collect();

        let table = Self { widths, rows: 0 };
        table.write_line(columns, out);
        let rule: Vec<String> = table.widths.iter().map(|width| "-".repeat(width + 2)).collect();
        out.push_str(&rule.join("+"));
        out.push('\n');
        table
    }

    /// Writes `rows` to `out`, one line each.
    pub(crate) fn push(&mut self, rows: &[Row], out: &mut String) {
        for row in cells(rows) {
            self.write_line(&row, out);
        }
        self.rows += rows.len();
    }

    /// Writes the row count that ends the table, without a newline after it.
    pub(crate) fn finish(&self, out: &mut String) {
        let noun = if self.rows == 1 { "row" } else { "rows" };
        out.push_str(&format!("({} {noun})", self.rows));
    }

    fn write_line(&self, cells: &[String], out: &mut String) {
        let padded: Vec<String> = cells
            .iter()
            .zip(&self.widths)
            .map(|(cell, &width)| format!(" {cell:<width$} "))
            .collect();
        out.push_str(padded.join("|").trim_end());
        out.push('\n');
    }
}

fn cells(rows: &[Row]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.values.iter().map(Value::to_string).collect())
        .collect()
}
