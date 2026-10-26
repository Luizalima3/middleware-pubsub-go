package protocol

import (
	"encoding/binary"
	"errors"
)

// Tipo identifica a operação que a mensagem representa.
type Tipo uint8

const (
	TipoPublicar     Tipo = 1
	TipoAssinar      Tipo = 2
	TipoCancelar     Tipo = 3
	TipoCriarTopico  Tipo = 4
	TipoApagarTopico Tipo = 5

	TipoMensagem     Tipo = 16
	TipoOK           Tipo = 17
	TipoErro         Tipo = 18
	TipoSemInscritos Tipo = 19
	TipoDescartadas  Tipo = 20
)

const (
	Versao           = 1
	TamanhoCabecalho = 7 // versão + tipo + ID + tamanho do tópico
	MaxTopico        = 255
	MaxMensagem      = 64 * 1024
)

var (
	ErrMensagemCurta  = errors.New("mensagem menor que o cabeçalho")
	ErrMensagemGrande = errors.New("mensagem maior que o limite")
	ErrTopicoGrande   = errors.New("nome do tópico maior que o limite")
	ErrVersao         = errors.New("versão do protocolo não suportada")
)

// Mensagem é a forma decodificada de um frame do protocolo.
type Mensagem struct {
	Tipo     Tipo
	ID       uint32
	Topico   string
	Conteudo []byte
}

// Codificar transforma uma Mensagem em bytes prontos para enviar.
func Codificar(m Mensagem) ([]byte, error) {
	if len(m.Topico) > MaxTopico {
		return nil, ErrTopicoGrande
	}
	total := TamanhoCabecalho + len(m.Topico) + len(m.Conteudo)
	if total > MaxMensagem {
		return nil, ErrMensagemGrande
	}

	buf := make([]byte, total)
	buf[0] = Versao
	buf[1] = byte(m.Tipo)
	binary.BigEndian.PutUint32(buf[2:6], m.ID)
	buf[6] = byte(len(m.Topico))
	copy(buf[TamanhoCabecalho:], m.Topico)
	copy(buf[TamanhoCabecalho+len(m.Topico):], m.Conteudo)
	return buf, nil
}

// Decodificar transforma bytes recebidos em uma Mensagem.
func Decodificar(dados []byte) (Mensagem, error) {
	if len(dados) > MaxMensagem {
		return Mensagem{}, ErrMensagemGrande
	}
	if len(dados) < TamanhoCabecalho {
		return Mensagem{}, ErrMensagemCurta
	}
	if dados[0] != Versao {
		return Mensagem{}, ErrVersao
	}

	tamTopico := int(dados[6])
	if len(dados) < TamanhoCabecalho+tamTopico {
		return Mensagem{}, ErrMensagemCurta
	}

	inicioConteudo := TamanhoCabecalho + tamTopico
	return Mensagem{
		Tipo:     Tipo(dados[1]),
		ID:       binary.BigEndian.Uint32(dados[2:6]),
		Topico:   string(dados[TamanhoCabecalho:inicioConteudo]),
		Conteudo: append([]byte(nil), dados[inicioConteudo:]...),
	}, nil
}
