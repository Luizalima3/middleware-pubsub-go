package protocol

import (
	"bytes"
	"testing"
)

func TestIdaEVolta(t *testing.T) {
	original := Mensagem{
		Tipo:     TipoPublicar,
		ID:       42,
		Topico:   "leilao/lance",
		Conteudo: []byte("100"),
	}

	dados, err := Codificar(original)
	if err != nil {
		t.Fatal(err)
	}
	volta, err := Decodificar(dados)
	if err != nil {
		t.Fatal(err)
	}

	if volta.Tipo != original.Tipo || volta.ID != original.ID ||
		volta.Topico != original.Topico || !bytes.Equal(volta.Conteudo, original.Conteudo) {
		t.Fatalf("esperava %+v, veio %+v", original, volta)
	}
}

func TestMensagemCurta(t *testing.T) {
	_, err := Decodificar([]byte{1, 2})
	if err != ErrMensagemCurta {
		t.Fatalf("esperava ErrMensagemCurta, veio %v", err)
	}
}

func TestTopicoGrande(t *testing.T) {
	m := Mensagem{Tipo: TipoAssinar, Topico: string(make([]byte, 300))}
	_, err := Codificar(m)
	if err != ErrTopicoGrande {
		t.Fatalf("esperava ErrTopicoGrande, veio %v", err)
	}
}
