package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type Manifest struct {
	fileName string  `json:"-"`
	Levels   []Level `json:"levels"`
}

type Level struct {
	Segments []string `json:"segments"`
}

// Read a manifest file, or create it if it does not exist.
func LoadManifest(filename string) (*Manifest, error) {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		// TODO: Check for backup if not found.
		m := &Manifest{fileName: filename}
		writeManifest(m)
		return m, nil
	} else if err != nil {
		return nil, fmt.Errorf("Failed to stat manifest file: %v", err)
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to open manifest file: %v", err)
	}
	defer file.Close()

	var manifest Manifest
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("Failed to decode manifest: %v", err)
	}
	// TODO: Check for backup if not found.

	manifest.fileName = filename
	return &manifest, nil
}

func writeManifest(m *Manifest) error {
	file, err := os.OpenFile(m.fileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(m)
	if err != nil {
		return err
	}

	return nil
}

func (m *Manifest) AddSegment(segmentName string, level int) error {
	if level == len(m.Levels) {
		// Create a new level.
		m.Levels = append(m.Levels, Level{})
	} else if level > len(m.Levels) {
		return fmt.Errorf("Invalid level (got %d, len is %d)", level, len(m.Levels))
	}
	m.Levels[level].Segments = append(m.Levels[level].Segments, segmentName)
	return writeManifest(m)
}
