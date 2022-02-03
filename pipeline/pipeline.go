package pipeline

func maybeEmitError(err error, errCh chan<- error) {
	select {
	case errCh <- err:
	default:
	}
}
