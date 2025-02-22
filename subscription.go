package notifier

type Subscription struct {
	ch     chan string
	id     int
	unsub  func() error
	closed bool
}

// A Callback when receiving a message on a subscribed topic
type Callback func(msg string)

func (s *Subscription) Close() error {
	if s.closed {
		return nil
	}
	close(s.ch)
	return s.unsub()
}

// returns the channel where messages are sent
func (s *Subscription) Channel() <-chan string {
	return s.ch
}
