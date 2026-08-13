package scheduler

// hostSet is an unordered set of host IDs with O(1) add, remove and pick.
type hostSet struct {
	ids   []HostID
	index map[HostID]int
}

func newHostSet() hostSet {
	return hostSet{index: make(map[HostID]int)}
}

func (h *hostSet) add(id HostID) {
	if h.index == nil {
		h.index = make(map[HostID]int)
	}
	if _, exists := h.index[id]; exists {
		return
	}
	h.index[id] = len(h.ids)
	h.ids = append(h.ids, id)
}

// remove deletes id by swapping the last element into its place.
func (h *hostSet) remove(id HostID) {
	i, exists := h.index[id]
	if !exists {
		return
	}
	last := len(h.ids) - 1
	if i != last {
		moved := h.ids[last]
		h.ids[i] = moved
		h.index[moved] = i
	}
	h.ids = h.ids[:last]
	delete(h.index, id)
}

// any returns some member of the set, or false when empty.
func (h *hostSet) any() (HostID, bool) {
	if len(h.ids) == 0 {
		return "", false
	}
	return h.ids[len(h.ids)-1], true
}

func (h *hostSet) len() int { return len(h.ids) }

func (h *hostSet) contains(id HostID) bool {
	_, ok := h.index[id]
	return ok
}
