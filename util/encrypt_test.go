package util

import "testing"

func TestEncrypt(t *testing.T) {
	s, err := Encrypt("weareatest")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(s)
	s, ok, err := Decrypt(s)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Log(s)
	}
}
