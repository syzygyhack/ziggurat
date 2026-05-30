package util

import "fmt"

// ValidateNoOCIImage returns an error if image is non-empty, indicating that
// OCI container execution is requested but not yet supported by Ziggurat.
func ValidateNoOCIImage(image string) error {
	if image != "" {
		return fmt.Errorf("OCI image execution is not yet supported; omit the image field to run on the host OS")
	}
	return nil
}
