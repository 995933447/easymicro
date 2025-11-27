package util

import (
	"os"

	"github.com/995933447/goencrypt"
)

const (
	AesKey = "3AiOOIBSWsRR50fVYEsOBU3leghtyhM="
	AesIv  = "V42hceaV/+6K9ih5"
)

func GetEncryptAesKeyAndAesIv() (string, string) {
	aesKey := os.Getenv("EASYMICRO_AES_KEY")
	if aesKey == "" {
		aesKey = AesKey
	}
	aesIv := os.Getenv("EASYMICRO_AES_IV")
	if aesIv == "" {
		aesIv = AesIv
	}
	return aesKey, aesIv
}

func Encrypt(s string) (string, error) {
	aesKey, aesIv := GetEncryptAesKeyAndAesIv()
	buf, err := goencrypt.EncryptAESCBCBase64([]byte(s), aesKey, aesIv, true)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func Decrypt(s string) (string, bool, error) {
	aesKey, aesIv := GetEncryptAesKeyAndAesIv()
	buf, ok, err := goencrypt.DecryptAESCBCBase64([]byte(s), aesKey, aesIv, true)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	return string(buf), true, nil
}
