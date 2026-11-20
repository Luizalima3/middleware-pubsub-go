package fila

import "testing"

func TestLimitesBytesDescartaMaisAntigoELimpeza(t *testing.T) {
	f := Nova[string](3, 6)
	f.Adicionar("a", 3, false)
	f.Adicionar("b", 3, false)
	if _, ok := f.Adicionar("c", 1, false); ok {
		t.Fatal("ignorou limite em bytes")
	}
	r, ok := f.Adicionar("c", 2, true)
	if !ok || len(r) != 1 || r[0] != "a" {
		t.Fatal(r, ok)
	}
	if n, b := f.Tamanho(); n != 2 || b != 5 {
		t.Fatal(n, b)
	}
	f.Remover(func(s string) bool { return s == "b" })
	v, ok := f.Retirar()
	if !ok || v != "c" {
		t.Fatal(v, ok)
	}
	if n, b := f.Tamanho(); n != 0 || b != 0 {
		t.Fatal(n, b)
	}
	if _, ok = f.Adicionar("muito grande", 7, true); ok {
		t.Fatal("aceitou maior que capacidade")
	}
}
