// ******************************************************************************
// Copyright (c) 2020-2022 VMware, Inc. All rights reserved. VMware Confidential.
// ******************************************************************************

package registration

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

type EncryptedString string

var (
	plaintextKey = []byte{186, 8, 215, 220, 171, 49, 29, 7, 12, 231, 206, 89, 51, 151, 249, 138, 142, 187, 251, 65, 50, 129, 179, 170, 203, 54, 129, 209, 64, 1, 86, 232}
)

func (es EncryptedString) Decrypt() (string, error) {
	cipher, _ := chacha20poly1305.NewX(plaintextKey)
	cipherText, err := base64.StdEncoding.DecodeString(string(es))
	if err != nil {
		return "", fmt.Errorf("invalid input string: %w", err)
	}
	if len(cipherText) < cipher.NonceSize() {
		return "", fmt.Errorf("too short for input string %s", cipherText)
	}
	nonce, cipherText := cipherText[:cipher.NonceSize()], cipherText[cipher.NonceSize():]
	plainText, err := cipher.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return "", fmt.Errorf("invalid input string: %w", err)
	}
	return string(plainText), nil
}

func NewEncryptString(s string) (EncryptedString, error) {
	cipher, _ := chacha20poly1305.NewX(plaintextKey)
	nonce := make([]byte, cipher.NonceSize(), cipher.NonceSize()+len(s)+cipher.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}
	plainText := []byte(s)
	cipherText := cipher.Seal(nonce, nonce, plainText, nil)
	return EncryptedString(base64.StdEncoding.EncodeToString(cipherText)), nil
}
