package util

import "testing"

func TestParseCUDAVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{
			"Cuda compilation tools, release 12.4, V12.4.131\nBuild cuda_12.4.r12.4/compiler.33450725_0",
			"12.4",
		},
		{
			"nvcc: NVIDIA (R) Cuda compiler driver\nCopyright (c) 2005-2023 NVIDIA Corporation\nBuilt on Tue_Jul_11_02:20:34_PDT_2023\nCuda compilation tools, release 12.2, V12.2.140\nBuild cuda_12.2.r12.2/compiler.33191640_0",
			"12.2",
		},
		{
			"some random output\nwithout any version",
			"",
		},
		{
			"release 11.8\nother stuff",
			"11.8",
		},
	}
	for _, tt := range tests {
		got := ParseCUDAVersion(tt.input)
		if got != tt.want {
			t.Errorf("ParseCUDAVersion(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
