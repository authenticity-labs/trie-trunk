package cache

import (
	"sync"
	"time"
)

// CommitScheduler bounds hour packing and canonical file writes across stores.
// Acquire only after releasing the writer lock. Release the permit before
// acquiring that lock again to publish the completed hour.
type CommitScheduler struct {
	permits chan struct{}
	mu      sync.Mutex
	stats   CommitStats
}
type CommitStats struct {
	Workers, Active, Waiting                 int
	Attempts                                 uint64
	QueueTime, WorkTime, MaxWorkTime         time.Duration
	PublishWait, PublishTime, MaxPublishTime time.Duration
	OverWindow                               uint64
	MaxOverrun                               time.Duration
}

func NewCommitScheduler(workers int) *CommitScheduler {
	if workers < 1 {
		workers = 1
	}
	return &CommitScheduler{permits: make(chan struct{}, workers), stats: CommitStats{Workers: workers}}
}

var defaultCommitScheduler = NewCommitScheduler(1)

func (s *CommitScheduler) Stats() CommitStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
func (s *CommitScheduler) acquire() func() {
	queued := time.Now()
	s.mu.Lock()
	s.stats.Waiting++
	s.mu.Unlock()
	s.permits <- struct{}{}
	started := time.Now()
	s.mu.Lock()
	s.stats.Waiting--
	s.stats.Active++
	s.stats.QueueTime += started.Sub(queued)
	s.mu.Unlock()
	return func() {
		elapsed := time.Since(started)
		s.mu.Lock()
		s.stats.Active--
		s.stats.Attempts++
		s.stats.WorkTime += elapsed
		s.stats.MaxWorkTime = max(s.stats.MaxWorkTime, elapsed)
		s.mu.Unlock()
		<-s.permits
	}
}
func (s *Store) SealingStats() CommitStats { return s.config.CommitScheduler.Stats() }

// Hour H closes at H+1h and becomes sealable after the 15-minute tail.
// Its ordinary target is H+2h, leaving 45 minutes for packing and publication.
func (s *CommitScheduler) observeWindow(hourEnd, finished time.Time) {
	over := finished.Sub(hourEnd.Add(time.Hour))
	if over <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.OverWindow++
	s.stats.MaxOverrun = max(s.stats.MaxOverrun, over)
}

func (s *CommitScheduler) observePublication(wait, work time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.PublishWait += wait
	s.stats.PublishTime += work
	s.stats.MaxPublishTime = max(s.stats.MaxPublishTime, work)
}
