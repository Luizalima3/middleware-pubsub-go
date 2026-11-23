package protocol

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestJSONLegivelELimite(t *testing.T) {
	m := Mensagem{Tipo: TipoPublicar, ID: 42, Cliente: "ana", Topico: "jogo/reacoes", Conteudo: []byte("Gol! ação")}
	b, e := Codificar(m)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(b, []byte("Gol! ação")) {
		t.Fatalf("conteúdo não legível: %s", b)
	}
	r, e := Decodificar(b)
	if e != nil || r.ID != 42 || !bytes.Equal(r.Conteudo, m.Conteudo) {
		t.Fatalf("%+v %v", r, e)
	}
	m.Conteudo = []byte(strings.Repeat("a", 1000))
	if _, e = Codificar(m); !errors.Is(e, ErrMensagemGrande) {
		t.Fatal(e)
	}
	for _, b := range [][]byte{nil, []byte("x"), []byte("{} {}"), []byte("{\"tipo\":\"OK\",\"segredo\":1}"), bytes.Repeat([]byte("x"), 1001)} {
		if _, e = Decodificar(b); e == nil {
			t.Fatalf("aceitou %q", b)
		}
	}
	m.Conteudo = []byte{255}
	if _, e = Codificar(m); e == nil {
		t.Fatal("aceitou UTF-8 inválido")
	}
}
func TestTopicoValido(t *testing.T) {
	for _, s := range []string{"", "a b", "a\n", strings.Repeat("x", 81)} {
		if TopicoValido(s) {
			t.Fatal(s)
		}
	}
	if !TopicoValido("evento/reacoes") {
		t.Fatal("tópico válido recusado")
	}
}
