package util

import (
	"os"

	"github.com/deatil/go-cryptobin/cryptobin/crypto"
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

	c := crypto.FromString(s).SetKey(aesKey).SetIv(aesIv).Aes().CBC().PKCS7Padding().Encrypt()

	if err := c.Error(); err != nil {
		return "", err
	}

	return c.ToBase64String(), nil
}

func Decrypt(s string) (string, error) {
	aesKey, aesIv := GetEncryptAesKeyAndAesIv()

	d := crypto.FromBase64String(s).SetKey(aesKey).SetIv(aesIv).Aes().CBC().PKCS7Padding().Decrypt()

	if err := d.Error(); err != nil {
		return "", err
	}

	return d.ToString(), nil
}
