// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package registration

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncryptedString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		en   EncryptedString
		args args
	}{
		{
			name: "Success",
			args: args{
				s: "hello",
			},
		},
		{
			name: "Empty",
			args: args{
				s: "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encryptedStr, err := NewEncryptString(tt.args.s)
			if err != nil {
				t.Fatal(err)
			}

			got, err := encryptedStr.Decrypt()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.args.s {
				t.Errorf("Decrypt() = %v, want %v", got, tt.args.s)
			}
		})
	}
}

func TestEncryptedString_Decrypt(t *testing.T) {
	tests := []struct {
		name             string
		es               EncryptedString
		want             string
		expectedErrorStr string
	}{
		{
			name:             "Wrong base64",
			es:               "123456",
			want:             "",
			expectedErrorStr: "invalid input string",
		},
		{
			name:             "Too short cipherText",
			es:               EncryptedString(base64.StdEncoding.EncodeToString([]byte("123456"))),
			want:             "",
			expectedErrorStr: "too short for input string",
		},
		{
			name:             "Wrong cipherText",
			es:               EncryptedString(base64.StdEncoding.EncodeToString([]byte("123456789012345678901234567890"))),
			want:             "",
			expectedErrorStr: "invalid input string",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.es.Decrypt()
			if err != nil {
				assert.Contains(t, err.Error(), tt.expectedErrorStr)
			}
			if got != tt.want {
				t.Errorf("Decrypt() = %v, want %v", got, tt.want)
			}
		})
	}
}
