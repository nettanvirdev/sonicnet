//! The queue's rules, apart from whoever keeps it: where a download goes in line, and which
//! queued ones start when there is room.

use sonic_core::Millis;

/// A queue place for a download joining at `now` among downloads queued at `others`: behind all
/// of them, or — asked for by name (Resume) — ahead of all of them. Places only order; they are
/// timestamps so a queue saved before a restart sorts the same afterwards.
pub(crate) fn place(front: bool, now: Millis, others: impl IntoIterator<Item = Millis>) -> Millis {
    let others: Vec<Millis> = others.into_iter().collect();
    if front {
        others.iter().copied().fold(now, Millis::min) - 1
    } else {
        others.iter().map(|place| place + 1).fold(now, Millis::max)
    }
}

/// Of `queued` (id, place), the ones to start, first queued first, so that no more than
/// `at_once` run. Lowering `at_once` below `running` starts nothing and stops nothing: the ones
/// running finish.
pub(crate) fn to_start<Id: Copy>(
    mut queued: Vec<(Id, Millis)>,
    running: usize,
    at_once: usize,
) -> Vec<Id> {
    queued.sort_by_key(|(_, place)| *place);
    let room = at_once.saturating_sub(running);
    queued.into_iter().take(room).map(|(id, _)| id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_at_the_back_or_by_name_at_the_front() {
        assert_eq!(place(false, 100, []), 100);
        assert_eq!(place(false, 100, [50, 200]), 201);
        assert_eq!(place(true, 100, [50, 200]), 49);
        assert_eq!(place(true, 100, []), 99);
        // The clock behind the queue (a restored one) still goes behind it.
        assert_eq!(place(false, 10, [500]), 501);
    }

    #[test]
    fn starts_first_queued_first_up_to_the_room_left() {
        let queued = vec![("c", 30), ("a", 10), ("b", 20)];
        assert_eq!(to_start(queued.clone(), 0, 2), ["a", "b"]);
        assert_eq!(to_start(queued.clone(), 1, 2), ["a"]);
        // Lowered below what runs: nothing starts, nothing is stopped.
        assert!(to_start(queued, 3, 2).is_empty());
    }
}
