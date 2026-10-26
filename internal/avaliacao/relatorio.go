package avaliacao

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Salvar gera artefatos independentes, sem bibliotecas nem serviços externos.
func Salvar(dir string, r Resultado) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(r.Resumo, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dir, "resumo.json"), append(data, '\n'), 0644); err != nil {
		return err
	}
	var buf strings.Builder
	w := csv.NewWriter(&buf)
	w.Write([]string{"segundo", "oferecidas", "enviadas_socket", "puladas_gerador", "erros_envio", "entregas_unicas", "avisos_rate_limit", "avisos_sem_inscritos", "p99_ms"})
	for _, l := range r.Linhas {
		w.Write([]string{strconv.Itoa(l.Segundo), u(l.Oferecidas), u(l.Enviadas), u(l.Puladas), u(l.ErrosEnvio), u(l.Recebidas), u(l.Rejeitadas), u(l.SemInscritos), fmt.Sprintf("%.6f", l.P99MS)})
	}
	w.Flush()
	if err = w.Error(); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dir, "intervalos.csv"), []byte(buf.String()), 0644); err != nil {
		return err
	}
	vazao := grafico("Publicações e entregas por intervalo", "contagem / intervalo de 1 s", r.Linhas, false)
	lat := grafico("Latência p99 das entregas recebidas", "milissegundos", r.Linhas, true)
	for name, svg := range map[string]string{"vazao.svg": vazao, "latencia.svg": lat} {
		if err = os.WriteFile(filepath.Join(dir, name), []byte(svg), 0644); err != nil {
			return err
		}
	}
	s := r.Resumo
	page := fmt.Sprintf(`<!doctype html><html lang="pt-BR"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Avaliação Pub/Sub</title>
<style>body{font:16px/1.6 system-ui,sans-serif;max-width:1050px;margin:40px auto;padding:0 24px;color:#213448;background:#f5f8fc}h1,h2{line-height:1.2}section{background:white;padding:24px;border:1px solid #dce4ee;border-radius:12px;margin:20px 0}.cards{display:flex;gap:24px;flex-wrap:wrap}.card strong{display:block;font-size:25px;color:#175e79}svg{max-width:100%%;height:auto}table{border-collapse:collapse;width:100%%}td,th{text-align:left;padding:8px;border-bottom:1px solid #ddd}code{overflow-wrap:anywhere}</style>
<h1>Avaliação do middleware Pub/Sub</h1><p>Tópico <code>%s</code> · início %s</p>
<section class="cards"><div class="card"><strong>%.2f ms</strong>p99 observado</div><div class="card"><strong>%d</strong>entregas únicas</div><div class="card"><strong>%d</strong>entregas ausentes</div><div class="card"><strong>%d</strong>publicações puladas</div></section>
<section><h2>Configuração e contadores</h2><table><tr><th>Medida</th><th>Valor</th></tr>
<tr><td>Publicadores / assinantes / assinantes lentos</td><td>%d / %d / %d</td></tr><tr><td>Taxa por publicador</td><td>%.2f msg/s</td></tr><tr><td>Duração / drenagem</td><td>%s / %s</td></tr><tr><td>Escritas sem erro / erros de envio</td><td>%d / %d</td></tr><tr><td>Avisos de limite / sem inscritos</td><td>%d / %d</td></tr><tr><td>Duplicadas / outros erros do broker</td><td>%d / %d</td></tr><tr><td>p50 / p95</td><td>%.3f / %.3f ms</td></tr><tr><td>p99 normais / lentos</td><td>%.3f / %.3f ms</td></tr><tr><td>p99 atraso do gerador</td><td>%.3f ms</td></tr><tr><td>Execução interrompida</td><td>%t</td></tr></table></section>
<section>%s</section><section>%s</section>
<section><h2>Como interpretar</h2><p>Latência desde o instante agendado até o consumo pela aplicação. Inclui atraso do gerador, filas e eventual atraso artificial do assinante lento. Os relógios usados pertencem ao mesmo processo; percentis aproximados pelo limite superior de buckets de 2%%. Não há sincronização de relógios entre máquinas envolvida nesta medida.</p><p>Uma escrita sem erro no socket não confirma aceitação pelo broker. Entregas ausentes são pares publicação/assinante de escritas sem erro que não chegaram antes do fim da drenagem. Podem incluir rejeição por limite, descarte, falha ou atraso: não representam, isoladamente, perda de rede. Envios com erro podem ter sido parcialmente processados e não entram nesse denominador.</p><p>O cenário lento atrasa o consumo do canal da biblioteca, que também tem buffer de 256 eventos. Testes curtos podem não exercer pressão sobre o socket do broker. Estas métricas não medem CPU, memória ou motivo interno de descarte do servidor.</p><p>Execuções contra o broker de teste validam o gerador; não comprovam desempenho do middleware final. Compare percentis junto com contadores, taxa oferecida e carga pulada. Guarde resumo.json e intervalos.csv com cada experimento.</p></section></html>`, html.EscapeString(s.Config.Topico), s.Inicio.Format("2006-01-02 15:04:05Z07:00"), s.P99MS, s.Recebidas, s.Ausentes, s.Puladas, s.Config.Publicadores, s.Config.Assinantes, s.Config.Lentos, s.Config.Taxa, s.Config.Duracao, s.Config.Drenagem, s.Enviadas, s.ErrosEnvio, s.Rejeitadas, s.SemInscritos, s.Duplicadas, s.OutrosErros, s.P50MS, s.P95MS, s.P99NormaisMS, s.P99LentosMS, s.P99AtrasoGeradorMS, s.Interrompido, vazao, lat)
	return os.WriteFile(filepath.Join(dir, "relatorio.html"), []byte(page), 0644)
}

func u(n uint64) string { return strconv.FormatUint(n, 10) }
func grafico(titulo, unidade string, linhas []Linha, lat bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 940 330" role="img" aria-label="%s"><rect width="940" height="330" fill="white"/><text x="65" y="25" font-family="sans-serif" font-size="18">%s</text>`, html.EscapeString(titulo), html.EscapeString(titulo))
	max := 1.0
	for _, l := range linhas {
		for _, v := range []float64{float64(l.Oferecidas), float64(l.Enviadas), float64(l.Recebidas)} {
			if !lat && v > max {
				max = v
			}
		}
		if lat && l.P99MS > max {
			max = l.P99MS
		}
	}
	for i := 0; i <= 4; i++ {
		y := 270 - float64(i)*50
		fmt.Fprintf(&b, `<path d="M65 %.1fH910" stroke="#e3e8ef"/><text x="58" y="%.1f" text-anchor="end" font-family="sans-serif" font-size="11">%.1f</text>`, y, y+4, max*float64(i)/4)
	}
	n := len(linhas) - 1
	if n < 1 {
		n = 1
	}
	series := []struct {
		nome, cor string
		valor     func(Linha) float64
	}{{"oferecidas", "#8c5ace", func(l Linha) float64 { return float64(l.Oferecidas) }}, {"escritas no socket", "#137c8b", func(l Linha) float64 { return float64(l.Enviadas) }}, {"entregas únicas", "#d27a18", func(l Linha) float64 { return float64(l.Recebidas) }}}
	if lat {
		series = series[:1]
		series[0].nome = "p99"
		series[0].valor = func(l Linha) float64 { return l.P99MS }
	}
	for j, s := range series {
		fmt.Fprintf(&b, `<polyline fill="none" stroke="%s" stroke-width="2" points="`, s.cor)
		for i, l := range linhas {
			fmt.Fprintf(&b, "%.1f,%.1f ", 65+float64(i)*845/float64(n), 270-s.valor(l)*200/max)
		}
		fmt.Fprintf(&b, `"/><text x="%d" y="318" font-family="sans-serif" font-size="12" fill="%s">%s</text>`, 65+j*240, s.cor, s.nome)
	}
	fmt.Fprintf(&b, `<text x="65" y="53" font-family="sans-serif" font-size="11">%s</text><text x="65" y="290" font-family="sans-serif" font-size="11">0 s</text><text x="910" y="290" text-anchor="end" font-family="sans-serif" font-size="11">%d s</text></svg>`, unidade, n)
	return b.String()
}
