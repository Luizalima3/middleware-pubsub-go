//go:build !windows && !linux

package metricas

import "errors"

func lerProcesso() (Processo, error) {
	return Processo{}, errors.New("coleta de CPU/RAM disponível apenas em Windows e Linux")
}
