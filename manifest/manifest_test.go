package manifest

import (
	"path/filepath"
	"testing"
)

// Test creating a manifest, adding a segment, and reading the manifest back afterwards.
func TestAddSegment(t *testing.T) {
	dataDirectory := t.TempDir()
	filePath := filepath.Join(dataDirectory, "manifest.json")

	// Load empty manifest.
	manifest, err := LoadManifest(filePath)
	if err != nil {
		t.Error(err)
	}

	expectedLevels := []Level{
		{Segments: []string{"foo"}},
		{Segments: []string{"bar", "baz"}},
	}

	for i, level := range expectedLevels {
		for _, segment := range level.Segments {
			manifest.AddSegment(segment, i)
		}
	}

	// Read updated manifest.
	manifest, err = LoadManifest(filePath)
	if err != nil {
		t.Error(err)
	}

	for i, level := range expectedLevels {
		for j, segment := range level.Segments {
			if manifest.Levels[i].Segments[j] != segment {
				t.Errorf("Expected segment name %s, got %s", manifest.Levels[i].Segments[j], segment)
			}
		}
	}
}
