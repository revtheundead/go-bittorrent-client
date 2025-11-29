package downloader

import "math/rand"

const (
	StrategySequential PiecePickerStrategy = iota
	StrategyRandom
	StrategyRarest
)

type PiecePickerStrategy int

func buildPieceOrder(numPieces int, strategy PiecePickerStrategy) []int {
	order := make([]int, numPieces)
	for i := 0; i < numPieces; i++ {
		order[i] = i
	}

	switch strategy {
	case StrategySequential:
		// Already in order
	case StrategyRandom:
		rand.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	case StrategyRarest:
		// Same for sequential for now, implement later
	}

	return order
}
