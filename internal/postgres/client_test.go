package postgres

import (
	"strings"
	"testing"
)

func TestSanitizeLastError(t *testing.T) {
	got := SanitizeLastError("  database write failed  ")
	if got != "database write failed" {
		t.Fatalf("expected trimmed error, got %q", got)
	}
}

func TestSanitizeLastErrorUsesFallback(t *testing.T) {
	got := SanitizeLastError("   ")
	if got != "event processing failed" {
		t.Fatalf("expected fallback error, got %q", got)
	}
}

func TestSanitizeLastErrorTruncatesLongMessages(t *testing.T) {
	got := SanitizeLastError(strings.Repeat("x", maxLastErrorRunes+1))
	if len([]rune(got)) != maxLastErrorRunes {
		t.Fatalf("expected %d runes, got %d", maxLastErrorRunes, len([]rune(got)))
	}
}
