package cache

// Two hour slots cover normal ingress and the grace-period tail. A slot is
// released only after sealing commits. Backlogged or out-of-order hours stay in
// overflow storage rather than overwriting an unsealed hour. Promotion into a
// freed slot is part of sealing, never a wall-clock timer or summary request.
type summarySlot struct {
	hour  int64
	value *summaryAccumulator
}
type summaryRing struct {
	slots    [2]summarySlot
	overflow map[int64]*summaryAccumulator
}

func (r *summaryRing) get(hour int64) *summaryAccumulator {
	slot := &r.slots[(hour/TimeHourMS)%2]
	if slot.value != nil && slot.hour == hour {
		return slot.value
	}
	return r.overflow[hour]
}
func (r *summaryRing) acquire(hour int64) *summaryAccumulator {
	if a := r.get(hour); a != nil {
		return a
	}
	a := &summaryAccumulator{index: map[accumulatorKey]int{}}
	slot := &r.slots[(hour/TimeHourMS)%2]
	if slot.value == nil {
		*slot = summarySlot{hour, a}
	} else {
		if r.overflow == nil {
			r.overflow = map[int64]*summaryAccumulator{}
		}
		r.overflow[hour] = a
	}
	return a
}
func (r *summaryRing) release(hour int64) {
	slot := &r.slots[(hour/TimeHourMS)%2]
	if slot.value == nil || slot.hour != hour {
		delete(r.overflow, hour)
		return
	}
	*slot = summarySlot{}
	var oldest int64
	var next *summaryAccumulator
	for h, a := range r.overflow {
		if (h/TimeHourMS)%2 == (hour/TimeHourMS)%2 && (next == nil || h < oldest) {
			oldest, next = h, a
		}
	}
	if next != nil {
		*slot = summarySlot{oldest, next}
		delete(r.overflow, oldest)
	}
}
func (r *summaryRing) each(visit func(int64, *summaryAccumulator)) {
	for _, s := range r.slots {
		if s.value != nil {
			visit(s.hour, s.value)
		}
	}
	for h, a := range r.overflow {
		visit(h, a)
	}
}

func (r *summaryRing) frozen(hour int64) summaryRing {
	var out summaryRing
	out.slots[(hour/TimeHourMS)%2] = summarySlot{hour, r.get(hour)}
	return out
}
func (r *summaryRing) all(yield func(int64, *summaryAccumulator) bool) {
	for _, s := range r.slots {
		if s.value != nil && !yield(s.hour, s.value) {
			return
		}
	}
	for h, a := range r.overflow {
		if !yield(h, a) {
			return
		}
	}
}
func (r *summaryRing) len() int {
	n := len(r.overflow)
	for _, s := range r.slots {
		if s.value != nil {
			n++
		}
	}
	return n
}
