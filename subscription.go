package notifier

type Subscription struct {
	Topic
	ch       chan string
	id       int
	unsub    func() error
	isClosed bool
}

func (s *Subscription) Close() error {
	if s.isClosed {
		return nil
	}
	close(s.ch)
	return s.unsub()
}

// returns the channel where messages are sent
func (s *Subscription) Channel() <-chan string {
	return s.ch
}
