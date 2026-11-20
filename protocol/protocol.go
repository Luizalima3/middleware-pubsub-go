// Package protocol define uma mensagem JSON por datagrama UDP.
package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

type Tipo string

const (
	TipoPublicar     Tipo = "PUBLICAR"
	TipoAssinar      Tipo = "ASSINAR"
	TipoCancelar     Tipo = "CANCELAR"
	TipoCriarTopico  Tipo = "CRIAR_TOPICO"
	TipoApagarTopico Tipo = "APAGAR_TOPICO"
	TipoMensagem     Tipo = "MENSAGEM"
	TipoOK           Tipo = "OK"
	TipoErro         Tipo = "ERRO"
	TipoSemInscritos Tipo = "SEM_INSCRITOS"
	TipoDescartadas  Tipo = "DESCARTADAS"
	TipoAtividade    Tipo = "ATIVIDADE"
	TipoEstado       Tipo = "ESTADO"
	TipoRecebido     Tipo = "RECEBIDO"
	TipoInteresse    Tipo = "INTERESSE"
	TipoRepasse      Tipo = "REPASSE"
	TipoCriarRemoto  Tipo = "CRIAR_REMOTO"
	TipoApagarRemoto Tipo = "APAGAR_REMOTO"
	TipoEstatisticas Tipo = "ESTATISTICAS"
)
const (
	MaxTopico   = 80
	MaxCliente  = 64
	MaxMensagem = 1000
)

var (
	ErrMensagemCurta  = errors.New("mensagem vazia ou inválida")
	ErrMensagemGrande = errors.New("datagrama excede 1000 bytes")
	ErrTopicoGrande   = errors.New("tópico excede 80 bytes")
)

// Conteudo é []byte na API Go e uma string UTF-8 legível no JSON.
type Mensagem struct {
	Enviadas  uint64  `json:"enviadas,omitempty"`
	Tipo      Tipo    `json:"tipo"`
	ID        uint64  `json:"id,omitempty"`
	Cliente   string  `json:"cliente,omitempty"`
	Topico    string  `json:"topico,omitempty"`
	Conteudo  []byte  `json:"-"`
	Origem    string  `json:"origem,omitempty"`
	Aviso     uint64  `json:"aviso,omitempty"`
	Taxa      float64 `json:"taxa,omitempty"`
	Fila      int     `json:"fila,omitempty"`
	Recebidas uint64  `json:"recebidas,omitempty"`
	Perdidas  uint64  `json:"perdidas,omitempty"`
}
type pacote struct {
	Enviadas  uint64  `json:"enviadas,omitempty"`
	Tipo      Tipo    `json:"tipo"`
	ID        uint64  `json:"id,omitempty"`
	Cliente   string  `json:"cliente,omitempty"`
	Topico    string  `json:"topico,omitempty"`
	Conteudo  string  `json:"conteudo,omitempty"`
	Origem    string  `json:"origem,omitempty"`
	Aviso     uint64  `json:"aviso,omitempty"`
	Taxa      float64 `json:"taxa,omitempty"`
	Fila      int     `json:"fila,omitempty"`
	Recebidas uint64  `json:"recebidas,omitempty"`
	Perdidas  uint64  `json:"perdidas,omitempty"`
}

func Codificar(m Mensagem) ([]byte, error) {
	if !utf8.Valid(m.Conteudo) {
		return nil, errors.New("conteúdo deve ser texto UTF-8")
	}
	if err := validar(m); err != nil {
		return nil, err
	}
	b, err := json.Marshal(pacote{m.Enviadas, m.Tipo, m.ID, m.Cliente, m.Topico, string(m.Conteudo), m.Origem, m.Aviso, m.Taxa, m.Fila, m.Recebidas, m.Perdidas})
	if err != nil {
		return nil, err
	}
	if len(b) > MaxMensagem {
		return nil, ErrMensagemGrande
	}
	return b, nil
}
func Decodificar(b []byte) (Mensagem, error) {
	if len(b) > MaxMensagem {
		return Mensagem{}, ErrMensagemGrande
	}
	if len(b) == 0 || !utf8.Valid(b) {
		return Mensagem{}, ErrMensagemCurta
	}
	var p pacote
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return Mensagem{}, fmt.Errorf("JSON inválido: %w", err)
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return Mensagem{}, errors.New("somente um objeto por datagrama")
	}
	m := Mensagem{p.Enviadas, p.Tipo, p.ID, p.Cliente, p.Topico, []byte(p.Conteudo), p.Origem, p.Aviso, p.Taxa, p.Fila, p.Recebidas, p.Perdidas}
	return m, validar(m)
}
func validar(m Mensagem) error {
	if len(m.Topico) > MaxTopico {
		return ErrTopicoGrande
	}
	if len(m.Cliente) > MaxCliente || len(m.Origem) > MaxCliente {
		return errors.New("identificador excede 64 bytes")
	}
	if strings.TrimSpace(string(m.Tipo)) == "" {
		return ErrMensagemCurta
	}
	if !utf8.ValidString(m.Topico) || !utf8.ValidString(m.Cliente) {
		return errors.New("texto UTF-8 inválido")
	}
	return nil
}
func TopicoValido(t string) bool {
	return t != "" && len(t) <= MaxTopico && utf8.ValidString(t) && !strings.ContainsAny(t, "\x00\r\n\t ")
}
func Administrativo(t Tipo) bool {
	return t == TipoAssinar || t == TipoCancelar || t == TipoCriarTopico || t == TipoApagarTopico || t == TipoEstatisticas
}
