package loadtester

type taskPhase uint8

const (
	taskPhaseInvalid taskPhase = iota
	taskPhaseDo
	taskPhaseRetry
	taskPhaseCanRetry
)

func (p taskPhase) String() string {
	return []string{
		"",
		"do",
		"retry",
		"can-retry",
	}[p]
}
