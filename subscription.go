package notifier

type Subscription struct {
	cb Callback
}

// A Callback when receiving a message on a subscribed topic
type Callback func(msg string)

func (s *Subscription) Close() error {
	return nil
}
