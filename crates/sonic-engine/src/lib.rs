//! Pure download logic: planning, scheduling, stream counts, speeds and limits. No I/O.
//!
//! Nothing here reads a clock of its own. Every function that depends on time takes it as an
//! argument — [`Millis`](sonic_core::Millis) for the download's tick clock, or a
//! [`chrono::DateTime`] for anything that follows the calendar (data periods, schedule windows) —
//! so each rule can be checked against an exact situation, DST changes included.
//!
//! - [`plan`]: block size and how many streams a joining network starts with.
//! - [`progress`]: a block's frontier and who delivered it.
//! - [`scheduler`]: which block a free stream fetches next, hedges included.
//! - [`concurrency`]: how many streams each network runs, and whether the disk keeps up.
//! - [`meter`]: speeds, time left, peak speed and the speed history.
//! - [`limits`]: speed limits and data usage per calendar period.
//! - [`schedule`]: when the download schedule lets downloads run.

pub mod concurrency;
pub mod limits;
pub mod meter;
pub mod plan;
pub mod progress;
pub mod schedule;
pub mod scheduler;

#[cfg(test)]
mod test_tz;
