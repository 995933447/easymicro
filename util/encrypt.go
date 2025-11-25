package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"os"
	"strings"
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
	buf, err := EncryptBytesByKey([]byte(s), aesKey, aesIv, false)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

func EncryptBytesByKey(s []byte, aesKey, aesIv string, isPadding bool) ([]byte, error) {
	block, err := aes.NewCipher([]byte(aesKey))
	if err != nil {
		return nil, err
	}

	cfb := cipher.NewCBCEncrypter(block, []byte(aesIv))

	var plainBytes []byte
	if !isPadding {
		padding := cfb.BlockSize() - len(s)%cfb.BlockSize()
		padding = padding % cfb.BlockSize()
		padText := bytes.Repeat([]byte{' '}, padding)
		plainBytes = append(s, padText...)
	} else {
		plainBytes = pKCS7Padding(s, cfb.BlockSize())
	}

	enBytes := make([]byte, len(plainBytes))
	cfb.CryptBlocks(enBytes, plainBytes)
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(enBytes)))
	base64.StdEncoding.Encode(buf, enBytes)

	return buf, nil
}

func pKCS7Padding(cipherText []byte, blockSize int) []byte {
	padding := blockSize - len(cipherText)%blockSize
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(cipherText, padText...)
}

func Decrypt(s string) (string, bool, error) {
	aesKey, aesIv := GetEncryptAesKeyAndAesIv()
	buf, ok, err := DecryptBytesByKey([]byte(s), aesKey, aesIv, false)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}
	return string(buf), true, nil
}

func DecryptBytesByKey(s []byte, aesKey, aesIV string, isPadding bool) ([]byte, bool, error) {
	buf := make([]byte, base64.StdEncoding.DecodedLen(len(s)))
	n, err := base64.StdEncoding.Decode(buf, s)
	if n < aes.BlockSize || n%aes.BlockSize != 0 {
		return nil, false, nil
	}

	block, err := aes.NewCipher([]byte(aesKey))
	if err != nil {
		return nil, false, err
	}

	cfb := cipher.NewCBCDecrypter(block, []byte(aesIV))

	plainTxt := make([]byte, n)
	cfb.CryptBlocks(plainTxt, buf[:n])

	var plainBytes []byte
	if !isPadding {
		plainBytes = []byte(strings.TrimRight(string(plainTxt), " "))
	} else {
		plainBytes = pKCS7UnPadding(plainTxt)
	}

	return plainBytes, true, nil
}

func pKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	if length <= 0 {
		return origData
	}
	unPadding := int(origData[length-1])
	if unPadding <= 0 {
		unPadding = 1
	}
	if length-unPadding <= 0 {
		return origData
	}
	return origData[:(length - unPadding)]
}
