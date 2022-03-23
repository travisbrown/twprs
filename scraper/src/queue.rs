use chrono::{DateTime, Duration, Utc};
use futures::{join, FutureExt};
use futures_locks::RwLock;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

const DEFAULT_SKIP_DEACTIVATION_RECHECK_SECONDS: i64 = 2 * 60 * 60;
const MAX_TARGET_DAYS: f32 = 10.0;

pub struct UserQueue {
    underlying: RwLock<PriorityQueue<u64, Reverse<u32>>>,
    scores: RwLock<HashMap<u64, u32>>,
    recently_deactivated: RwLock<HashMap<u64, DateTime<Utc>>>,
    skip_deactivation_recheck: Duration,
}

impl UserQueue {
    pub fn new<E, I: Iterator<Item = Result<(u64, u32, DateTime<Utc>), E>>>(
        values: I,
    ) -> Result<Self, E> {
        let mut underlying = PriorityQueue::new();
        let mut scores = HashMap::new();

        for value in values {
            let (id, score, last_snapshot) = value?;

            underlying.push(id, Reverse(Self::compute_target(last_snapshot, score)));
            scores.insert(id, score);
        }

        Ok(Self {
            underlying: RwLock::new(underlying),
            scores: RwLock::new(scores),
            recently_deactivated: RwLock::new(HashMap::new()),
            skip_deactivation_recheck: Duration::seconds(DEFAULT_SKIP_DEACTIVATION_RECHECK_SECONDS),
        })
    }

    pub async fn next_batch(&self, count: usize) -> Vec<u64> {
        self.underlying
            .with_write(move |mut queue| {
                let mut batch = Vec::with_capacity(count);

                for _ in 0..count {
                    match queue.pop() {
                        Some((value, _)) => {
                            batch.push(value);
                        }
                        None => {
                            break;
                        }
                    }
                }

                futures::future::ready(batch)
            })
            .await
    }

    /// Process a batch of IDs that were seen unfollowing or being unfollowed.
    pub async fn process_removals<I: IntoIterator<Item = u64> + Send + 'static>(
        &self,
        ids: I,
    ) -> () {
        let ids = ids.into_iter().collect::<HashSet<_>>();

        join!(
            // These are given the highest priority, since they represent deactivation candidates.
            self.remove_recently_deactivated(ids.clone())
                .then(|ids| self.prioritize(ids, 0)),
            // Their scores are decremented, since they have one fewer connection.
            self.decrement_scores(ids)
        );
    }

    /// Process a batch of IDs that were seen following or being followed.
    pub async fn process_additions<I: IntoIterator<Item = u64> + Send + 'static>(
        &self,
        ids: I,
    ) -> () {
        let ids = ids.into_iter().collect::<HashSet<_>>();

        join!(
            // We add these to the priority queue with the highest priority (if they aren't already there).
            self.prioritize_new(ids.clone()),
            // Their scores are incremented, since they have one new connection.
            self.increment_scores(ids)
        );
    }

    pub async fn process_updates<I: IntoIterator<Item = u64> + Send + 'static>(
        &self,
        ids: I,
    ) -> () {
        let now = Utc::now();
        let scores = self.scores.read().await;

        self.underlying
            .with_write(move |mut queue| {
                for id in ids {
                    let score = scores.get(&id).unwrap_or(&1);
                    queue.push(id, Reverse(Self::compute_target(now, *score)));
                }
                futures::future::ready(())
            })
            .await
    }

    pub async fn process_deactivations<I: IntoIterator<Item = u64> + Send + 'static>(
        &self,
        ids: I,
    ) -> () {
        let now = Utc::now();

        self.recently_deactivated
            .with_write(move |mut recently_deactivated| {
                for id in ids {
                    recently_deactivated.insert(id, now);
                }
                futures::future::ready(())
            })
            .await
    }

    fn compute_target(now: DateTime<Utc>, score: u32) -> u32 {
        let base = now.timestamp() as f32;
        let days = MAX_TARGET_DAYS / (score as f32);

        (base + (days * 24.0 * 60.0 * 60.0)) as u32
    }

    async fn decrement_scores(&self, ids: HashSet<u64>) -> () {
        self.scores
            .with_write(move |mut scores| {
                for id in ids {
                    let score = scores.entry(id).or_default();
                    if *score > 0 {
                        *score -= 1;
                    }
                }
                futures::future::ready(())
            })
            .await
    }

    async fn increment_scores(&self, ids: HashSet<u64>) -> () {
        self.scores
            .with_write(move |mut scores| {
                for id in ids {
                    let score = scores.entry(id).or_default();
                    *score += 1;
                }
                futures::future::ready(())
            })
            .await
    }

    async fn remove_recently_deactivated(&self, mut ids: HashSet<u64>) -> HashSet<u64> {
        let now = Utc::now();
        let skip_deactivation_recheck = self.skip_deactivation_recheck;

        self.recently_deactivated
            .with_read(move |recently_deactivated| {
                ids.retain(|id| {
                    recently_deactivated.get(id).map_or(true, |deactivation| {
                        now - *deactivation <= skip_deactivation_recheck
                    })
                });
                futures::future::ready(ids)
            })
            .await
    }

    async fn prioritize(&self, ids: HashSet<u64>, priority: u32) -> () {
        self.underlying
            .with_write(move |mut queue| {
                for id in ids {
                    queue.push_increase(id, Reverse(priority));
                }
                futures::future::ready(())
            })
            .await
    }

    async fn prioritize_new(&self, ids: HashSet<u64>) -> () {
        self.underlying
            .with_write(move |mut queue| {
                for id in ids {
                    queue.push_decrease(id, Reverse(0));
                }
                futures::future::ready(())
            })
            .await
    }
}
