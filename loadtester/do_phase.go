package loadtester

type doPhase uint8

const (
	doPhaseInvalid doPhase = iota
	doPhaseDo
	doPhaseRetry
	doPhaseCanRetry
)

func (p doPhase) String() string {
	return []string{
		"",
		"do",
		"retry",
		"can-retry",
	}[p]
}
