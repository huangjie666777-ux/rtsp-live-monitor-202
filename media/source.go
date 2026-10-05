package media

import (
	"fmt"
	"os"
)

const (
	SampleRate     = 8000
	SamplesPerTick = 160 // 20ms of 8kHz mono PCMU
)

// Source is a fixed 8kHz mono PCMU (u-law, payload type 0) audio clip.
type Source struct {
	data []byte // 1 byte per sample
}

func LoadSource(path string) (*Source, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty media file")
	}
	return &Source{data: data}, nil
}

func (s *Source) Samples() int { return len(s.data) }

// Duration in seconds.
func (s *Source) Duration() float64 {
	return float64(len(s.data)) / SampleRate
}

// Chunk returns up to SamplesPerTick samples starting at sample offset off.
func (s *Source) Chunk(off int) []byte {
	if off >= len(s.data) {
		return nil
	}
	end := off + SamplesPerTick
	if end > len(s.data) {
		end = len(s.data)
	}
	return s.data[off:end]
}
