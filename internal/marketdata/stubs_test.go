package marketdata

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/marketdata/repository"
)

// stubRepo is the in-memory Repository used by service tests. It mirrors
// the production semantics for the fields the gRPC handlers actually
// touch: stream upsert by key, request lifecycle, lease lifecycle, and
// history-request lifecycle. Defaults to behaving like a clean DB; tests
// pre-populate as needed.
type stubRepo struct {
	mu sync.Mutex

	streams     map[int64]domain.MarketDataStream  // stream_id → stream
	streamByKey map[domain.StreamKey]int64         // key → stream_id
	requests    map[int64]domain.MarketDataRequest // request_id → request
	leases      map[string]domain.MarketDataLease  // session+stream → lease (key: fmt session_id|stream_id)
	historyByID map[int64]domain.MarketDataHistoryRequest
	subs        map[int64]domain.SessionMarketDataSubscription
	subByKey    map[string]int64
	delivery    map[string]domain.StreamDeliveryLease
	writer      map[string]domain.MarketDataWriterLease
	coverage    map[int64]domain.MarketDataCoverageSegment

	nextStreamID   int64
	nextRequestID  int64
	nextLeaseID    int64
	nextHistoryID  int64
	nextSubID      int64
	nextCoverageID int64
}

func newStubRepo() *stubRepo {
	return &stubRepo{
		streams:        map[int64]domain.MarketDataStream{},
		streamByKey:    map[domain.StreamKey]int64{},
		requests:       map[int64]domain.MarketDataRequest{},
		leases:         map[string]domain.MarketDataLease{},
		historyByID:    map[int64]domain.MarketDataHistoryRequest{},
		subs:           map[int64]domain.SessionMarketDataSubscription{},
		subByKey:       map[string]int64{},
		delivery:       map[string]domain.StreamDeliveryLease{},
		writer:         map[string]domain.MarketDataWriterLease{},
		coverage:       map[int64]domain.MarketDataCoverageSegment{},
		nextStreamID:   1,
		nextRequestID:  1,
		nextLeaseID:    1,
		nextHistoryID:  1,
		nextSubID:      1,
		nextCoverageID: 1,
	}
}

// ── streams ─────────────────────────────────────────────────────────────

func (s *stubRepo) UpsertMarketDataStream(_ context.Context, key domain.StreamKey) (domain.MarketDataStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.streamByKey[key]; ok {
		st := s.streams[id]
		st.UpdatedAt = time.Now()
		s.streams[id] = st
		return st, nil
	}
	id := s.nextStreamID
	s.nextStreamID++
	now := time.Now()
	st := domain.MarketDataStream{
		StreamID:     id,
		Key:          key,
		DesiredState: domain.StreamDesiredRunning,
		ActualState:  domain.StreamActualPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	s.streams[id] = st
	s.streamByKey[key] = id
	return st, nil
}

func (s *stubRepo) GetMarketDataStream(_ context.Context, streamID int64) (domain.MarketDataStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[streamID]
	if !ok {
		return domain.MarketDataStream{}, repository.ErrNotFound
	}
	return st, nil
}

func (s *stubRepo) GetMarketDataStreamByKey(_ context.Context, key domain.StreamKey) (domain.MarketDataStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.streamByKey[key]
	if !ok {
		return domain.MarketDataStream{}, repository.ErrNotFound
	}
	return s.streams[id], nil
}

func (s *stubRepo) ListMarketDataStreams(_ context.Context) ([]domain.MarketDataStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.MarketDataStream, 0, len(s.streams))
	for _, st := range s.streams {
		out = append(out, st)
	}
	return out, nil
}

func (s *stubRepo) UpdateMarketDataStreamActualState(
	_ context.Context,
	streamID int64,
	state domain.StreamActualState,
	lastData *time.Time,
	lastErr string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.streams[streamID]
	if !ok {
		return repository.ErrNotFound
	}
	st.ActualState = state
	if lastData != nil {
		t := *lastData
		st.LastDataAt = &t
	}
	if lastErr != "" {
		st.LastError = lastErr
	}
	now := time.Now()
	st.LastReconciledAt = &now
	st.UpdatedAt = now
	s.streams[streamID] = st
	return nil
}

// ── requests ────────────────────────────────────────────────────────────

func (s *stubRepo) UpsertMarketDataRequest(
	ctx context.Context,
	userID int64,
	accountID *int64,
	key domain.StreamKey,
	needsLive bool,
) (domain.MarketDataRequest, error) {
	st, _ := s.UpsertMarketDataStream(ctx, key)
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, r := range s.requests {
		if r.UserID == userID && r.Key == key && r.Status != domain.RequestStatusCancelled {
			r.NeedsLiveDelivery = needsLive
			r.Status = domain.RequestStatusActive
			if accountID != nil {
				r.AccountID = accountID
			}
			r.UpdatedAt = time.Now()
			s.requests[id] = r
			return r, nil
		}
	}
	id := s.nextRequestID
	s.nextRequestID++
	now := time.Now()
	r := domain.MarketDataRequest{
		RequestID:         id,
		UserID:            userID,
		AccountID:         accountID,
		StreamID:          st.StreamID,
		Key:               key,
		NeedsLiveDelivery: needsLive,
		Status:            domain.RequestStatusActive,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	s.requests[id] = r
	return r, nil
}

func (s *stubRepo) CancelMarketDataRequest(_ context.Context, requestID, userID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.requests[requestID]
	if !ok {
		return repository.ErrNotFound
	}
	if r.UserID != userID {
		return repository.ErrPermissionDenied
	}
	if r.Status == domain.RequestStatusCancelled {
		return nil
	}
	r.Status = domain.RequestStatusCancelled
	now := time.Now()
	r.CancelledAt = &now
	r.UpdatedAt = now
	s.requests[requestID] = r
	return nil
}

func (s *stubRepo) GetMarketDataRequest(_ context.Context, requestID, userID int64) (domain.MarketDataRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.requests[requestID]
	if !ok {
		return domain.MarketDataRequest{}, repository.ErrNotFound
	}
	if r.UserID != userID {
		return domain.MarketDataRequest{}, repository.ErrPermissionDenied
	}
	return r, nil
}

func (s *stubRepo) ListMarketDataRequestsByUser(_ context.Context, userID int64) ([]domain.MarketDataRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.MarketDataRequest
	for _, r := range s.requests {
		if r.UserID == userID && r.Status != domain.RequestStatusCancelled {
			out = append(out, r)
		}
	}
	return out, nil
}

// ── leases ──────────────────────────────────────────────────────────────

func (s *stubRepo) leaseKey(sessionID string, streamID int64) string {
	return sessionID + "|" + time.Time{}.Format(time.RFC3339Nano) + "|" + itoa64(streamID)
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func (s *stubRepo) CreateOrRenewLease(
	_ context.Context,
	sessionID string,
	strategyID, accountID *int64,
	streamID int64,
	ttl time.Duration,
) (domain.MarketDataLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.leaseKey(sessionID, streamID)
	now := time.Now()
	expires := now.Add(ttl).UTC()
	if existing, ok := s.leases[k]; ok {
		existing.ExpiresAt = expires
		existing.LastHeartbeatAt = now
		existing.ReleasedAt = nil
		if strategyID != nil {
			existing.StrategyID = strategyID
		}
		if accountID != nil {
			existing.AccountID = accountID
		}
		s.leases[k] = existing
		return existing, nil
	}
	id := s.nextLeaseID
	s.nextLeaseID++
	l := domain.MarketDataLease{
		LeaseID:         id,
		SessionID:       sessionID,
		StrategyID:      strategyID,
		AccountID:       accountID,
		StreamID:        streamID,
		ExpiresAt:       expires,
		LastHeartbeatAt: now,
		CreatedAt:       now,
	}
	s.leases[k] = l
	return l, nil
}

func (s *stubRepo) ReleaseLease(_ context.Context, sessionID string, streamID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.leaseKey(sessionID, streamID)
	l, ok := s.leases[k]
	if !ok {
		return nil
	}
	now := time.Now()
	l.ReleasedAt = &now
	s.leases[k] = l
	return nil
}

func (s *stubRepo) CountActiveLeasesForStream(_ context.Context, streamID int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	now := time.Now()
	for _, l := range s.leases {
		if l.StreamID == streamID && l.ReleasedAt == nil && l.ExpiresAt.After(now) {
			n++
		}
	}
	return n, nil
}

func (s *stubRepo) CountActiveLeasesByStream(_ context.Context) (map[int64]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[int64]int{}
	now := time.Now()
	for _, l := range s.leases {
		if l.ReleasedAt == nil && l.ExpiresAt.After(now) {
			out[l.StreamID]++
		}
	}
	return out, nil
}

func (s *stubRepo) ExpireStaleLeases(_ context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, l := range s.leases {
		if l.ReleasedAt == nil && !l.ExpiresAt.After(now) {
			t := now
			l.ReleasedAt = &t
			s.leases[k] = l
			n++
		}
	}
	return n, nil
}

func (s *stubRepo) ListActiveLeasesForStream(_ context.Context, streamID int64) ([]domain.MarketDataLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []domain.MarketDataLease
	for _, l := range s.leases {
		if l.StreamID == streamID && l.ReleasedAt == nil && l.ExpiresAt.After(now) {
			out = append(out, l)
		}
	}
	return out, nil
}

// ── RuntimeChannel delivery subscriptions ──────────────────────────────

func subscriptionKey(sessionID string, key domain.StreamKey, mode int32) string {
	return sessionID + "|" + key.Exchange + "|" + key.Market + "|" + key.Kind + "|" + key.Symbol + "|" + key.Interval + "|" + itoa64(int64(mode))
}

func (s *stubRepo) UpsertSessionMarketDataSubscriptions(
	_ context.Context,
	userID int64,
	sessionID string,
	runtimeID string,
	mode int32,
	keys []domain.StreamKey,
) ([]domain.SessionMarketDataSubscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	out := make([]domain.SessionMarketDataSubscription, 0, len(keys))
	for _, key := range keys {
		k := subscriptionKey(sessionID, key, mode)
		if id, ok := s.subByKey[k]; ok {
			sub := s.subs[id]
			sub.UserID = userID
			sub.RuntimeID = runtimeID
			sub.UpdatedAt = now
			s.subs[id] = sub
			out = append(out, sub)
			continue
		}
		id := s.nextSubID
		s.nextSubID++
		sub := domain.SessionMarketDataSubscription{
			SubscriptionID: id,
			UserID:         userID,
			SessionID:      sessionID,
			RuntimeID:      runtimeID,
			Key:            key,
			Mode:           mode,
			Status:         "active",
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		s.subs[id] = sub
		s.subByKey[k] = id
		out = append(out, sub)
	}
	return out, nil
}

func (s *stubRepo) ReleaseSessionMarketDataSubscriptions(_ context.Context, sessionID, runtimeID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var n int64
	for id, sub := range s.subs {
		if sub.SessionID != sessionID || sub.Status != "active" {
			continue
		}
		if runtimeID != "" && sub.RuntimeID != runtimeID {
			continue
		}
		sub.Status = "released"
		sub.ReleasedAt = &now
		sub.UpdatedAt = now
		s.subs[id] = sub
		n++
	}
	return n, nil
}

func (s *stubRepo) ListActiveSessionMarketDataSubscriptions(_ context.Context) ([]domain.SessionMarketDataSubscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.SessionMarketDataSubscription, 0, len(s.subs))
	for _, sub := range s.subs {
		if sub.Status == "active" {
			out = append(out, sub)
		}
	}
	return out, nil
}

func (s *stubRepo) CreateOrRenewStreamDeliveryLease(
	_ context.Context,
	subscriptionID int64,
	ownerInstanceID string,
	ttl time.Duration,
) (domain.StreamDeliveryLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if existing, ok := s.delivery[itoa64(subscriptionID)]; ok {
		if existing.OwnerInstanceID != ownerInstanceID && existing.ExpiresAt.After(now) {
			return domain.StreamDeliveryLease{}, repository.ErrPermissionDenied
		}
		existing.OwnerInstanceID = ownerInstanceID
		existing.LastHeartbeatAt = now
		existing.ExpiresAt = now.Add(ttl)
		existing.UpdatedAt = now
		s.delivery[itoa64(subscriptionID)] = existing
		return existing, nil
	}
	lease := domain.StreamDeliveryLease{
		LeaseID:         "sdl-" + itoa64(subscriptionID),
		SubscriptionID:  subscriptionID,
		OwnerInstanceID: ownerInstanceID,
		Status:          "active",
		AcquiredAt:      now,
		LastHeartbeatAt: now,
		ExpiresAt:       now.Add(ttl),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	s.delivery[itoa64(subscriptionID)] = lease
	return lease, nil
}

func (s *stubRepo) RecordStreamDeliveryFailure(context.Context, domain.StreamDeliveryFailure) error {
	return nil
}

func (s *stubRepo) RecordStreamDeliveryProgress(_ context.Context, subscriptionID int64, ownerInstanceID string, topic string, partition int32, offset int64, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	lease, ok := s.delivery[itoa64(subscriptionID)]
	if !ok || lease.OwnerInstanceID != ownerInstanceID || lease.Status != "active" {
		return repository.ErrNotFound
	}
	lease.LastDeliveryAt = &at
	lease.LastTopic = topic
	lease.LastPartition = partition
	lease.LastOffset = offset
	lease.LastHeartbeatAt = at
	lease.UpdatedAt = at
	s.delivery[itoa64(subscriptionID)] = lease
	return nil
}

func (s *stubRepo) ListSessionDeliveryHealth(_ context.Context, userID int64, sessionID, runtimeID string) ([]domain.SessionDeliveryHealth, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.SessionDeliveryHealth, 0, len(s.subs))
	for _, sub := range s.subs {
		if sub.UserID != userID || sub.Status != "active" {
			continue
		}
		if sessionID != "" && sub.SessionID != sessionID {
			continue
		}
		if runtimeID != "" && sub.RuntimeID != runtimeID {
			continue
		}
		item := domain.SessionDeliveryHealth{Subscription: sub}
		if lease, ok := s.delivery[itoa64(sub.SubscriptionID)]; ok {
			copyLease := lease
			item.Lease = &copyLease
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *stubRepo) ReleaseStreamDeliveryLease(_ context.Context, leaseID, ownerInstanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, lease := range s.delivery {
		if lease.LeaseID == leaseID && lease.OwnerInstanceID == ownerInstanceID && lease.Status == "active" {
			lease.Status = "released"
			lease.ReleasedAt = &now
			lease.UpdatedAt = now
			s.delivery[k] = lease
			return nil
		}
	}
	return repository.ErrNotFound
}

func (s *stubRepo) writerKey(key domain.StreamKey, year int32) string {
	return key.Exchange + "|" + key.Market + "|" + key.Kind + "|" + key.Symbol + "|" + key.Interval + "|" + itoa64(int64(year))
}

func (s *stubRepo) CreateOrRenewMarketDataWriterLease(
	_ context.Context,
	key domain.StreamKey,
	year int32,
	ownerInstanceID string,
	scraperInstanceID string,
	collectorID string,
	ttl time.Duration,
) (domain.MarketDataWriterLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	storeKey := s.writerKey(key, year)
	if existing, ok := s.writer[storeKey]; ok {
		if existing.OwnerInstanceID != ownerInstanceID && existing.ExpiresAt.After(now) {
			return domain.MarketDataWriterLease{}, repository.ErrPermissionDenied
		}
		if existing.OwnerInstanceID != ownerInstanceID {
			existing.LeaseID = "mdwl-" + collectorID
			existing.AcquiredAt = now
		}
		existing.OwnerInstanceID = ownerInstanceID
		existing.ScraperInstanceID = scraperInstanceID
		existing.CollectorID = collectorID
		existing.Status = "active"
		existing.LastHeartbeatAt = now
		existing.ExpiresAt = now.Add(ttl)
		existing.ReleasedAt = nil
		existing.UpdatedAt = now
		s.writer[storeKey] = existing
		return existing, nil
	}
	lease := domain.MarketDataWriterLease{
		LeaseID:           "mdwl-" + collectorID,
		Key:               key,
		Year:              year,
		OwnerInstanceID:   ownerInstanceID,
		ScraperInstanceID: scraperInstanceID,
		CollectorID:       collectorID,
		Status:            "active",
		AcquiredAt:        now,
		LastHeartbeatAt:   now,
		ExpiresAt:         now.Add(ttl),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	s.writer[storeKey] = lease
	return lease, nil
}

func (s *stubRepo) ReleaseMarketDataWriterLease(_ context.Context, leaseID, ownerInstanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, lease := range s.writer {
		if lease.LeaseID == leaseID && lease.OwnerInstanceID == ownerInstanceID && lease.Status == "active" {
			lease.Status = "released"
			lease.ReleasedAt = &now
			lease.UpdatedAt = now
			s.writer[k] = lease
			return nil
		}
	}
	return repository.ErrNotFound
}

// ── history requests ────────────────────────────────────────────────────

func (s *stubRepo) UpsertMarketDataHistoryRequest(
	_ context.Context,
	userID int64,
	accountID *int64,
	key domain.StreamKey,
	startAt, endAt time.Time,
) (domain.MarketDataHistoryRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, h := range s.historyByID {
		if h.UserID == userID && h.Key == key &&
			h.RequestedStartAt.Equal(startAt) && h.RequestedEndAt.Equal(endAt) &&
			h.Status != domain.HistoryRequestCancelled {
			if accountID != nil {
				h.AccountID = accountID
			}
			h.UpdatedAt = time.Now()
			s.historyByID[id] = h
			return h, nil
		}
	}
	id := s.nextHistoryID
	s.nextHistoryID++
	now := time.Now()
	h := domain.MarketDataHistoryRequest{
		RequestID:        id,
		UserID:           userID,
		AccountID:        accountID,
		Key:              key,
		Status:           domain.HistoryRequestPending,
		RequestedStartAt: startAt,
		RequestedEndAt:   endAt,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	s.historyByID[id] = h
	return h, nil
}

func (s *stubRepo) CancelMarketDataHistoryRequest(_ context.Context, requestID, userID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.historyByID[requestID]
	if !ok {
		return repository.ErrNotFound
	}
	if h.UserID != userID {
		return repository.ErrPermissionDenied
	}
	if h.Status == domain.HistoryRequestCancelled {
		return nil
	}
	h.Status = domain.HistoryRequestCancelled
	now := time.Now()
	h.CancelledAt = &now
	h.UpdatedAt = now
	s.historyByID[requestID] = h
	return nil
}

func (s *stubRepo) ListMarketDataHistoryRequests(_ context.Context, includeTerminal bool) ([]domain.MarketDataHistoryRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.MarketDataHistoryRequest
	for _, h := range s.historyByID {
		if !includeTerminal {
			if h.Status == domain.HistoryRequestReady || h.Status == domain.HistoryRequestCancelled {
				continue
			}
		}
		out = append(out, h)
	}
	return out, nil
}

func (s *stubRepo) ListMarketDataHistoryRequestsByUser(_ context.Context, userID int64) ([]domain.MarketDataHistoryRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.MarketDataHistoryRequest
	for _, h := range s.historyByID {
		if h.UserID == userID &&
			h.Status != domain.HistoryRequestReady &&
			h.Status != domain.HistoryRequestCancelled {
			out = append(out, h)
		}
	}
	return out, nil
}

func (s *stubRepo) UpdateMarketDataHistoryRequestState(
	_ context.Context,
	requestID int64,
	statusValue domain.MarketDataHistoryRequestStatus,
	coveredStart, coveredEnd *time.Time,
	lastErr string,
) (domain.MarketDataHistoryRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.historyByID[requestID]
	if !ok {
		return domain.MarketDataHistoryRequest{}, repository.ErrNotFound
	}
	h.Status = statusValue
	if coveredStart != nil {
		t := *coveredStart
		h.CoveredStartAt = &t
	}
	if coveredEnd != nil {
		t := *coveredEnd
		h.CoveredEndAt = &t
	}
	h.LastError = lastErr
	h.UpdatedAt = time.Now()
	s.historyByID[requestID] = h
	return h, nil
}

// ── historical coverage ────────────────────────────────────────────────

func (s *stubRepo) MergeMarketDataCoverageSegments(_ context.Context, segments []domain.MarketDataCoverageSegment) ([]domain.MarketDataCoverageSegment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	out := make([]domain.MarketDataCoverageSegment, 0, len(segments))
	for _, incoming := range segments {
		incoming.StartAt = incoming.StartAt.UTC()
		incoming.EndAt = incoming.EndAt.UTC()
		mergedStart := incoming.StartAt
		mergedEnd := incoming.EndAt
		candidates := map[int64]struct{}{}
		for {
			foundNew := false
			for id, existing := range s.coverage {
				if existing.Key != incoming.Key || existing.Year != incoming.Year {
					continue
				}
				if _, ok := candidates[id]; ok {
					continue
				}
				if existing.EndAt.Before(mergedStart) || existing.StartAt.After(mergedEnd) {
					continue
				}
				mergedStart = minTime(mergedStart, existing.StartAt)
				mergedEnd = maxTime(mergedEnd, existing.EndAt)
				candidates[id] = struct{}{}
				foundNew = true
			}
			if !foundNew {
				break
			}
		}
		for id := range candidates {
			delete(s.coverage, id)
		}
		rowCount, err := expectedCount(mergedStart, mergedEnd, incoming.Key.Interval)
		if err != nil {
			return nil, err
		}
		id := s.nextCoverageID
		s.nextCoverageID++
		merged := domain.MarketDataCoverageSegment{
			SegmentID: id,
			Key:       incoming.Key,
			Year:      incoming.Year,
			StartAt:   mergedStart,
			EndAt:     mergedEnd,
			RowCount:  rowCount,
			Source:    incoming.Source,
			UpdatedAt: now,
		}
		s.coverage[id] = merged
		out = append(out, merged)
	}
	return out, nil
}

func (s *stubRepo) QueryMarketDataCoverage(_ context.Context, key domain.StreamKey, startAt, endAt time.Time) ([]domain.MarketDataCoverageSegment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	startAt = startAt.UTC()
	endAt = endAt.UTC()
	out := make([]domain.MarketDataCoverageSegment, 0)
	for _, seg := range s.coverage {
		if seg.Key != key {
			continue
		}
		if seg.EndAt.After(startAt) && seg.StartAt.Before(endAt) {
			out = append(out, seg)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].StartAt.Equal(out[j].StartAt) {
			return out[i].EndAt.Before(out[j].EndAt)
		}
		return out[i].StartAt.Before(out[j].StartAt)
	})
	return out, nil
}
