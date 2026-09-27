package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestKeepAliveSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if interval, err := readKeepAlive(path); err != nil || interval != 0 {
		t.Fatalf("missing settings must default off: %v %v", interval, err)
	}
	for _, tc := range []struct {
		text string
		want time.Duration
		bad  bool
	}{
		{"", 0, false}, {`keepAlive = ""`, 0, false}, {`keepAlive = "0"`, 0, false},
		{`keepAlive = "0s"`, 0, false}, {`keepAlive = "27m"`, 27 * time.Minute, false},
		{`keepAlive = "1s"`, time.Second, false}, {`keepAlive = "2h30m"`, 150 * time.Minute, false},
		{`keepAlive = "-1m"`, 0, true}, {`keepAlive = "1ms"`, 0, true},
		{`keepAlive = 27`, 0, true}, {`keepAlive = true`, 0, true}, {`keepAlive = "27"`, 0, true},
		{`keepAlive = "99999999999999999999h"`, 0, true}, {`keepalive = "27m"`, 0, true},
	} {
		t.Run(tc.text, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			got, err := readKeepAlive(path)
			if (err != nil) != tc.bad || got != tc.want {
				t.Fatalf("settings: %v %v", got, err)
			}
		})
	}
}
