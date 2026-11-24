"""Analisa CSVs e gera comparação das repetições, sem substituir dados brutos."""
import argparse
import base64
import csv
import html
import hashlib
import json
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def instante(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def ler_csv(path):
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    valid = [r for r in rows if None not in r and all(v is not None for v in r.values())]
    return valid, len(rows)-len(valid)


def texto_html(text):
    parts = []
    table = False
    def inline(value):
        return re.sub(r"\*\*(.*?)\*\*", r"<strong>\1</strong>", html.escape(value))
    for line in text.splitlines():
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if all(c and set(c) <= {"-"} for c in cells):
                continue
            tag = "td" if table else "th"
            if not table:
                parts.append('<div class="tabela"><table>'); table = True
            parts.append("<tr>"+"".join(f"<{tag}>"+inline(c)+f"</{tag}>" for c in cells)+"</tr>")
            continue
        if table:
            parts.append("</table></div>"); table = False
        if line.startswith("#"):
            level = min(6, len(line)-len(line.lstrip("#"))+1)
            parts.append(f"<h{level}>"+inline(line.lstrip("# "))+f"</h{level}>")
        elif line:
            parts.append("<p>"+inline(line.removeprefix("- "))+"</p>")
    if table:
        parts.append("</table></div>")
    return "\n".join(parts)


def analisar(root, run):
    path = root/Path(run["pasta"].replace("\\", "/"))
    s = json.loads((path/"resumo.json").read_text(encoding="utf-8"))
    start = instante(s["inicio"])
    duration = s["janela_observada_segundos"]
    end = start+duration
    cpu_s, ram_peaks, filas_peak, locais_peak = 0., [], defaultdict(float), 0.
    detalhes, series = [], []
    incompletas = 0
    for file in sorted(path.glob("broker-*.csv")):
        rows, bad = ler_csv(file)
        incompletas += bad
        # Recursos aparecem repetidos em cada fila; cada amostra só conta uma vez.
        samples = {r["amostra"]: r for r in rows}
        inside = [r for r in samples.values() if start <= instante(r["instante"]) <= end]
        covered = [r for r in inside if r["cpu_pct_1_nucleo"] and
                   instante(r["instante"])-float(r["intervalo_s"]) >= start]
        if len(inside) < 2 or not covered:
            raise ValueError("Coleta de recursos insuficiente: "+str(file))
        seconds = sum(float(r["intervalo_s"]) for r in covered)
        cpu = sum(float(r["cpu_pct_1_nucleo"])*float(r["intervalo_s"])/100 for r in covered)
        memory = max(int(r["ram_residente_bytes"]) for r in inside)
        if memory <= 0:
            raise ValueError("RAM residente não coletada")
        cpu_s += cpu
        ram_peaks.append(memory/1024**2)
        detalhes.append({"broker": file.stem, "cpu_segundos_cobertos": cpu,
                         "segundos_cobertos": seconds, "cobertura_janela_pct": seconds/duration*100,
                         "cpu_media_periodo_ativo_pct_1_nucleo": cpu/seconds*100,
                         "ram_residente_pico_mib": memory/1024**2})
        for r in rows:
            if not start <= instante(r["instante"]) <= end:
                continue
            cap, count = int(r["limite_itens"]), int(r["itens"])
            if count < 0 or count > cap:
                raise ValueError("Fila excedeu o limite: "+str(file))
            if r["limite_bytes"] and int(r["bytes"]) > int(r["limite_bytes"]):
                raise ValueError("Fila excedeu o limite de bytes: "+str(file))
            if cap:
                # Picos são registrados em cada inserção, mesmo entre amostras.
                value = int(r["pico_itens"]) if r["pico_itens"] else count
                filas_peak[r["fila_tipo"]] = max(filas_peak[r["fila_tipo"]], value/cap*100)
        series.append((file.stem, inside, rows))
    local, bad = ler_csv(path/"filas-clientes.csv")
    incompletas += bad
    for r in local:
        cap, count = int(r["limite_itens"]), int(r["itens"])
        if count < 0 or count > cap:
            raise ValueError("Fila local excedeu a capacidade")
        if r["papel"] == "assinante" and r["fila"] == "dados" and cap:
            locais_peak = max(locais_peak, count/cap*100)
    expected = s["publicacoes_escritas_no_socket"]*s["configuracao"]["assinantes"]
    result = {"cenario": run["cenario"], "repeticao": run["repeticao"],
              "configuracao_sha256": run["configuracao_sha256"],
              "publicacoes": s["publicacoes_escritas_no_socket"], "entregas": s["entregas_unicas"],
              "ausentes": s["entregas_ausentes_entre_envios_sem_erro"],
              "ausentes_pct": 100*s["entregas_ausentes_entre_envios_sem_erro"]/expected if expected else 0.,
              "entregas_s": s["entregas_por_segundo_observado"],
              "p99_ms": s["p99_ms"], "p99_normais_ms": s["p99_assinantes_normais_ms"],
              "p99_lentos_ms": s["p99_assinantes_lentos_ms"],
              "cpu_pct_1_nucleo": cpu_s/duration*100,
              "cpu_segundos_cobertos": cpu_s, "ram_picos_soma_mib": sum(ram_peaks),
              "fila_consumidor_pico_pct": filas_peak["consumidor"],
              "fila_topico_pico_pct": filas_peak["topico"],
              "fila_cliente_max_amostrado_pct": locais_peak,
              "descartes_cliente": s["descartes_fila_cliente"],
              "descartes_broker_avisados": s["avisos_descartes_broker"],
              "duplicatas_cliente": s["duplicatas_descartadas_pelo_cliente"],
              "duplicatas_gerador": s["duplicatas_detectadas_pelo_gerador"],
              "duplicatas_total": s["entregas_duplicadas"],
              "avisos_rate_limit": s["avisos_rate_limit"],
              "puladas": s["publicacoes_puladas_pelo_gerador"],
              "linhas_csv_incompletas": incompletas, "brokers": detalhes}
    if result["duplicatas_total"] != result["duplicatas_cliente"]+result["duplicatas_gerador"]:
        raise ValueError("Contagem de duplicatas inconsistente")
    return result, (s, start, end, series, local)


def grafico_series(root, run, data):
    s, start, end, series, local = data
    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True, constrained_layout=True)
    for name, samples, rows in series:
        cpu = [r for r in samples if r["cpu_pct_1_nucleo"]]
        axes[0].plot([instante(r["instante"])-start for r in cpu],
                     [float(r["cpu_pct_1_nucleo"]) for r in cpu], label=name)
        axes[1].plot([instante(r["instante"])-start for r in samples],
                     [int(r["ram_residente_bytes"])/1024**2 for r in samples], label=name)
        queues = defaultdict(lambda: defaultdict(float))
        for r in rows:
            t = instante(r["instante"])
            cap = int(r["limite_itens"])
            if start <= t <= end and cap and r["fila_tipo"] in ("topico", "consumidor"):
                queues[r["fila_tipo"]][t] = max(queues[r["fila_tipo"]][t], int(r["itens"])/cap*100)
        for kind, values in queues.items():
            axes[2].plot([t-start for t in values], list(values.values()), label=name+" "+kind)
    dados = defaultdict(float)
    for r in local:
        if r["fila"] == "dados" and r["papel"] == "assinante" and int(r["limite_itens"]):
            t = float(r["segundos"])
            dados[t] = max(dados[t], int(r["itens"])/int(r["limite_itens"])*100)
    axes[2].plot(list(dados), list(dados.values()), "--", label="máximo nas filas locais de dados")
    labels = ["CPU (% de um núcleo)", "RAM residente (MiB)", "Ocupação amostrada (%)"]
    for ax, label in zip(axes, labels):
        ax.set_ylabel(label); ax.grid(alpha=.25); ax.legend(fontsize=8, loc="upper right")
        ax.set_ylim(bottom=0)
    axes[2].set_ylim(0, 105)
    axes[2].set_xlabel("Segundos desde o início da carga (inclui drenagem)")
    axes[2].set_xlim(0, end-start)
    publication_end = s["configuracao"]["duracao_ns"]/1e9
    for ax in axes:
        ax.axvline(publication_end, color="grey", linestyle=":", alpha=.6)
        if run.get("evento_falha"):
            ax.axvline(instante(run["evento_falha"]["instante"])-start, color="#b91c1c", linestyle="--", alpha=.7)
    fig.suptitle(f"{run['cenario']} — repetição {run['repeticao']}\nPontilhado: fim dos envios; vermelho: término do broker, quando aplicável")
    name = f"series-{run['cenario']}-{run['repeticao']:02}.png"
    fig.savefig(root/name, dpi=150); plt.close(fig)
    return name


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--entrada", type=Path, required=True)
    args = ap.parse_args()
    root = args.entrada.resolve()
    manifest = json.loads((root/"manifesto.json").read_text(encoding="utf-8"))
    cfg = json.loads((root/"configuracao.json").read_text(encoding="utf-8"))
    grouped = defaultdict(list)
    records, charts = [], {}
    if len(manifest["execucoes"]) != len(manifest["plano"]):
        raise ValueError("Experimento incompleto; não gerar comparação como se estivesse concluído")
    for run in manifest["execucoes"]:
        if not run["sucesso"]:
            raise ValueError("Há execução sem sucesso")
        r, data = analisar(root, run)
        records.append(r); grouped[r["cenario"]].append(r)
        charts[(run["cenario"], run["repeticao"])] = grafico_series(root, run, data)
    names = [c["nome"] for c in cfg["cenarios"]]
    for name in names:
        rows = grouped[name]
        if len(rows) != cfg["repeticoes"] or len({r["configuracao_sha256"] for r in rows}) != 1:
            raise ValueError("Repetições com configuração diferente ou faltando: "+name)
    flat = [{k: v for k, v in r.items() if k != "brokers"} for r in records]
    with (root/"comparativo.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(flat[0])); w.writeheader(); w.writerows(flat)
    (root/"comparativo.json").write_text(json.dumps(records, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    metrics = [("cpu_pct_1_nucleo", "CPU total registrada / janela", "% de um núcleo; cobertura parcial"),
               ("ram_picos_soma_mib", "Soma dos picos residentes dos brokers", "MiB; picos não necessariamente simultâneos"),
               ("fila_consumidor_pico_pct", "Maior pico de uma fila de consumidor", "% da capacidade em itens"),
               ("p99_normais_ms", "Latência p99 dos assinantes normais", "ms; mediana dos p99 por execução"),
               ("entregas_s", "Vazão de entregas únicas", "entregas/s; inclui drenagem"),
               ("ausentes_pct", "Entregas ausentes", "% dos pares publicação/assinante")]
    fig, axes = plt.subplots(2, 3, figsize=(15, 9), constrained_layout=True)
    for ax, (key, title, unit) in zip(axes.flat, metrics):
        values = [[r[key] for r in grouped[name]] for name in names]
        medians = [statistics.median(v) for v in values]
        ax.bar(range(len(names)), medians, color=["#147d92", "#d7882b", "#b95757", "#7763a3", "#526881"])
        ax.errorbar(range(len(names)), medians,
                    yerr=[[m-min(v) for m, v in zip(medians, values)],
                          [max(v)-m for m, v in zip(medians, values)]], fmt="none", ecolor="#263238", capsize=4)
        ax.set_xticks(range(len(names)), names, rotation=20)
        ax.set_title(title, fontsize=11); ax.set_ylabel(unit, fontsize=9)
        ax.set_ylim(bottom=0); ax.grid(axis="y", alpha=.2)
    fig.suptitle(f"Comparação descritiva — {cfg['repeticoes']} repetições por cenário\nBarras: mediana; hastes: mínimo–máximo (não são intervalos de confiança). Cargas diferentes entre cenários.")
    fig.savefig(root/"comparativo.png", dpi=160)
    fig.savefig(root/"comparativo.svg"); plt.close(fig)
    def faixa(name, key):
        v = [r[key] for r in grouped[name]]
        return f"{statistics.median(v):.2f} [{min(v):.2f}–{max(v):.2f}]"
    text = ["# Resultados dos experimentos", "",
            f"Foram concluídas {len(records)} execuções: {cfg['repeticoes']} por cenário. Mesma configuração dentro de cada cenário; processos novos em cada repetição. Ordem alternada com semente registrada.", "",
            "## Como interpretar", "",
            "CPU usa o tempo de usuário + kernel do próprio broker. 100% corresponde a um núcleo lógico; pode ultrapassar 100%. A soma do tempo de CPU dos intervalos inteiros cobertos é dividida pela janela do experimento. Os intervalos de borda não são extrapolados: a cobertura exata de cada broker está em comparativo.json. Na falha, a coleta do broker terminado acaba antes da janela.", "",
            "RAM é o conjunto residente (working set no Windows), não o heap Go. A comparação soma o maior valor amostrado de cada broker; esses picos podem ocorrer em instantes diferentes e páginas compartilhadas podem ser contadas em ambos. Não é a RAM física exclusiva da máquina.", "",
            "As filas do broker registram ocupação atual e picos desde sua criação, por quantidade e bytes. Os picos das filas de dados são atualizados a cada inserção, inclusive entre amostras; controle tem apenas ocupação amostrada em itens. Filas locais e do gerador são amostradas, podendo perder picos rápidos. Um item já retirado para envio não está na ocupação; em_envio indica a espera no limitador local, sem medir todos os itens em trânsito.", "",
            "As cargas e durações diferem entre cenários. Esta é uma comparação descritiva, não um teste que isola o efeito do algoritmo. Os p99 são calculados por execução: a mediana desses percentis não é o p99 de todas as mensagens reunidas. As hastes mostram variação entre repetições, não significância estatística.", "",
            "## Comparação (mediana [mínimo–máximo])", "",
            "| Cenário | CPU registrada/janela (%) | RAM, soma picos (MiB) | Pico fila consumidor (%) | p99 normais (ms) | Ausentes (%) |",
            "| --- | --- | --- | --- | --- | --- |"]
    for name in names:
        text.append("| "+name+" | "+" | ".join(faixa(name, k) for k in ["cpu_pct_1_nucleo", "ram_picos_soma_mib", "fila_consumidor_pico_pct", "p99_normais_ms", "ausentes_pct"])+" |")
    text += ["", "## O que foi observado", ""]
    for name in names:
        rows = grouped[name]
        text.append(f"- **{name}:** entregas únicas {faixa(name, 'entregas')}; ausentes {faixa(name, 'ausentes_pct')}%; p99 dos consumidores normais {faixa(name, 'p99_normais_ms')} ms. Maior pico de fila de consumidor: {max(r['fila_consumidor_pico_pct'] for r in rows):.1f}%.")
    if "normal" in grouped and "lento" in grouped:
        base = statistics.median(r["p99_normais_ms"] for r in grouped["normal"])
        slow = statistics.median(r["p99_normais_ms"] for r in grouped["lento"])
        text += ["", f"Os consumidores normais tiveram p99 mediano de {base:.2f} ms no cenário normal e {slow:.2f} ms no cenário lento. Assim, não se deve afirmar que o atraso dos normais foi eliminado. A taxa oferecida e o buffer local também diferem entre esses casos: os dados não isolam o consumidor lento como única causa. A avaliação mostra o comportamento observado; calibrar os limites e realizar um teste com apenas um fator alterado é o próximo passo se a equipe quiser demonstrar isolamento de latência."]
    text += ["", "O cenário lento inclui atraso artificial e filas menores no cliente. Seu p99 geral mistura consumidores lentos e normais; por isso a comparação usa também o p99 separado dos normais. Ausências, avisos de rejeição e descartes podem se sobrepor e não devem ser somados como causas independentes.", "",
             "O cenário de excesso ignora deliberadamente o controle local para verificar a proteção do broker. A redução da taxa informada nesse caso não significa que o cliente agressivo a respeitou. Os dois brokers geram custo agregado de dois processos; não compare essa soma como se fosse o consumo de apenas um processo.", "",
             "Na falha, o primeiro broker é terminado dois segundos após lançar o processo do gerador, não após o início exato das publicações. O instante real do evento está em execucao.json e nos gráficos. O teste verifica retomada no sobrevivente; não mede automaticamente o tempo exato de indisponibilidade.", "",
             "## Duplicatas", "",
             f"As {len(records)} execuções registraram {sum(r['duplicatas_cliente'] for r in records)} duplicatas filtradas pela biblioteca e {sum(r['duplicatas_gerador'] for r in records)} detectadas pelo gerador. O total é a soma das etapas: uma mensagem removida antes de Eventos() não é contada novamente pelo gerador.", "",
             "Há um teste de injeção controlada que envia três cópias de cada entrega e verifica três entregas únicas e seis duplicatas filtradas para três publicações. Esse teste demonstra a contabilização; não é evidência de duplicação espontânea na rede. Zero duplicatas observadas não prova ausência fora da janela/cache nem mede as eliminadas pelo broker.", "",
             "## Limites e reprodução", "",
             "Resultados locais curtos, com instrumentação habilitada em todas as execuções. Não foi quantificado o custo adicional da coleta. Não há promessa de capacidade de produção nem conclusão sobre outras redes ou equipamentos. Aumente duração e repetições para a apresentação, usando uma nova pasta para cada lote.", "",
             "configuracao.json guarda parâmetros; manifesto.json registra ambiente, ordem, versão Go, revisão base, patch e hashes dos binários. Cada repetição contém CSVs, logs, resumo, contadores finais e comandos completos. comparativo.csv tem uma linha por execução; comparativo.json inclui a cobertura de CPU por broker. As séries usam tempo relativo ao início da carga e incluem a drenagem.", ""]
    analysis = "\n".join(text)
    (root/"ANALISE.md").write_text(analysis, encoding="utf-8")
    encoded = base64.b64encode((root/"comparativo.png").read_bytes()).decode()
    # Texto e tabela também disponíveis sem depender de um renderizador Markdown.
    page = '<!doctype html><html lang="pt-BR"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Resultados Pub/Sub</title><style>body{font:16px/1.65 system-ui;max-width:1150px;margin:32px auto;padding:0 20px;color:#213448}img{max-width:100%}h1,h2,h3{line-height:1.3}.tabela{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:14px}td,th{text-align:left;padding:10px;border-bottom:1px solid #ddd}th{background:#f4f7fb}a{color:#126779}</style><h1>CPU, memória, filas e desempenho</h1>'
    page += '<img alt="Comparação das repetições" src="data:image/png;base64,'+encoded+'">'
    page += '<p><a href="comparativo.csv">CSV comparativo</a> · <a href="comparativo.json">Detalhes por broker</a> · <a href="configuracao.json">Configuração</a></p>'+texto_html(analysis)+'<h2>Séries da primeira repetição</h2>'
    for name in names:
        image = charts[(name, 1)]
        page += '<h3>'+html.escape(name)+'</h3><img alt="Série '+html.escape(name)+'" src="'+image+'">'
    page += '</html>'
    (root/"relatorio-comparativo.html").write_text(page, encoding="utf-8")
    metadata = {"gerado_em": datetime.now(timezone.utc).isoformat(), "python": sys.version,
                "matplotlib": matplotlib.__version__,
                "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (root/"metadados-analise.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2)+"\n",encoding="utf-8")
    print("Comparação concluída:", root/"relatorio-comparativo.html", flush=True)


if __name__ == "__main__":
    main()
