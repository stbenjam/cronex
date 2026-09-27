package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/pelletier/go-toml/v2"
)

func readKeepAlive(path string) (time.Duration, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var settings map[string]string
	if err := toml.Unmarshal(data, &settings); err != nil {
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	for key := range settings {
		if key != "keepAlive" {
			return 0, fmt.Errorf("%s: unknown setting %q", path, key)
		}
	}
	if settings["keepAlive"] == "" {
		return 0, nil
	}
	interval, err := time.ParseDuration(settings["keepAlive"])
	if err != nil || interval < 0 || (interval > 0 && interval < time.Second) {
		return 0, fmt.Errorf("%s: keepAlive must be a duration of at least 1s, or 0 to disable it", path)
	}
	return interval, nil
}
