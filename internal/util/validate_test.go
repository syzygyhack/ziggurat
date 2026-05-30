package util

import "testing"

func TestValidateNoOCIImage(t *testing.T) {
	if err := ValidateNoOCIImage(""); err != nil {
		t.Errorf("empty image should not error: %v", err)
	}
	if err := ValidateNoOCIImage("ubuntu:22.04"); err == nil {
		t.Error("non-empty image should error")
	}
}
