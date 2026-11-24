package fila

import "testing"

func TestPicosRegistramRajadaEntreAmostras(t *testing.T) {
	f := Nova[string](3, 10)
	f.Adicionar("a", 4, false)
	f.Adicionar("b", 5, false)
	f.Retirar()
	f.Retirar()
	s := f.Estado()
	if s.Itens != 0 || s.Bytes != 0 || s.PicoItens != 2 || s.PicoBytes != 9 {
		t.Fatal(s)
	}
	f.Limpar()
	if f.Estado().PicoBytes != 9 {
		t.Fatal("limpeza apagou pico histórico")
	}
}
