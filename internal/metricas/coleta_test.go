package metricas

import (
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Luizalima3/middleware-pubsub-go/internal/fila"
)

func TestCPUUsaTempoDoProcessoESemLimitarAUmNucleo(t *testing.T) {
	if v := percentualCPU(Processo{CPUSegundos: 4}, Processo{CPUSegundos: 5}, 2); v != 50 {
		t.Fatal(v)
	}
	if v := percentualCPU(Processo{}, Processo{CPUSegundos: 3}, 2); v != 150 {
		t.Fatal(v)
	}
	if v := percentualCPU(Processo{}, Processo{CPUSegundos: 3}, 0); v != 0 {
		t.Fatal(v)
	}
}

func TestColetaRecursosReaisECSVSemSobrescrever(t *testing.T) {
	p, err := LerProcesso()
	if err != nil {
		t.Fatal(err)
	}
	if p.RAMBytes == 0 {
		t.Fatal("RAM não foi coletada")
	}
	path := filepath.Join(t.TempDir(), "metricas.csv")
	c, err := Iniciar(context.Background(), path, 10*time.Millisecond, func() []Fila {
		return []Fila{{Tipo: "topico", Nome: "a,b", Estado: fila.Estado{Itens: 2, Bytes: 10, LimiteItens: 4, LimiteBytes: 20, PicoItens: 3, PicoBytes: 15}}}
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(35 * time.Millisecond)
	if err = c.Fechar(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) < 3 || rows[1][5] != "" || rows[2][5] == "" {
		t.Fatal(rows)
	}
	for _, row := range rows[1:] {
		if row[11] != "a,b" || row[12] != "2" || row[14] != "4" || row[16] != "3" {
			t.Fatal(row)
		}
		ram, e := strconv.ParseUint(row[8], 10, 64)
		if e != nil || ram == 0 {
			t.Fatal(row)
		}
	}
	if _, err = Iniciar(context.Background(), path, time.Second, func() []Fila { return nil }); err == nil {
		t.Fatal("sobrescreveu dados existentes")
	}
}
