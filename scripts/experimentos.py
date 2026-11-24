"""Repete configurações registradas, com processos novos e coleta no broker.

Usa apenas a biblioteca padrão. O relatório opcional usa matplotlib.
"""
import argparse
import hashlib
import json
import os
import platform
import random
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HIDDEN = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def agora():
    return datetime.now(timezone.utc).isoformat()


def gravar(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def porta():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def flags(values):
    args = []
    for key, value in values.items():
        if isinstance(value, bool):
            args.append(f"-{key}={'true' if value else 'false'}")
        else:
            args += ["-"+key, str(value)]
    return args


def estatisticas(endereco):
    host, port = endereco.rsplit(":", 1)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(.5)
        # Consulta ao fim da observação, sem poluir a coleta com polling UDP.
        msg = json.dumps({"tipo": "ESTATISTICAS", "id": 7, "cliente": "avaliacao-final"}).encode()
        for _ in range(3):
            s.sendto(msg, (host, int(port)))
            try:
                while True:
                    data, source = s.recvfrom(1001)
                    if source != (host, int(port)):
                        continue
                    m = json.loads(data)
                    if m.get("tipo") == "OK" and m.get("id") == 7:
                        return json.loads(m["conteudo"])
            except socket.timeout:
                pass
    raise RuntimeError("Sem estatísticas de "+endereco)


def executar(case, rep, cfg, out, bin_dir):
    nome = case["nome"]
    dest = out / nome / f"repeticao-{rep:02}"
    dest.mkdir(parents=True, exist_ok=False)
    servidores, handles, carga = [], [], None
    enderecos = [f"127.0.0.1:{porta()}" for _ in range(case["brokers"])]
    if len(set(enderecos)) != len(enderecos):
        raise RuntimeError("Portas repetidas; execute novamente em outra pasta")
    broker_cfg = cfg["broker_comum"] | case.get("broker", {})
    carga_cfg = cfg["carga_comum"] | case["carga"]
    registro = {"cenario": nome, "repeticao": rep, "pasta": str(dest.relative_to(out)),
                "inicio_processo_carga": None, "evento_falha": None,
                "configuracao_broker": broker_cfg, "configuracao_carga": carga_cfg,
                "comandos_brokers": [], "comando_carga": [], "sucesso": False}
    # Este fingerprint ignora portas/IDs efêmeros e identifica a carga repetida.
    fixed = {"broker": broker_cfg, "carga": carga_cfg, "brokers": case["brokers"],
             "intervalo_metricas": cfg["intervalo_metricas"],
             "aquecimento": cfg["aquecimento_segundos"],
             "falha_apos": case.get("encerrar_primeiro_apos_segundos")}
    registro["configuracao_sha256"] = hashlib.sha256(json.dumps(fixed, sort_keys=True).encode()).hexdigest()
    try:
        for i, endereco in enumerate(enderecos):
            args = [str(bin_dir / executable("broker")), "-endereco", endereco,
                    "-id", f"b{i+1}", "-metricas", str(dest/f"broker-{i+1}.csv"),
                    "-intervalo-metricas", cfg["intervalo_metricas"]] + flags(broker_cfg)
            if len(enderecos) == 2:
                args += ["-par", enderecos[1-i]]
            log = (dest/f"broker-{i+1}.log").open("w", encoding="utf-8")
            handles.append(log)
            servidores.append(subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT,
                                               creationflags=HIDDEN))
            registro["comandos_brokers"].append(args)
        time.sleep(cfg["aquecimento_segundos"])
        if any(p.poll() is not None for p in servidores):
            raise RuntimeError("Broker encerrou na inicialização; veja os logs")
        args = [str(bin_dir / executable("carga")), "-brokers", ",".join(enderecos),
                "-topico", f"avaliacao/{nome}/{rep}", "-saida", str(dest),
                "-intervalo-metricas", cfg["intervalo_metricas"]] + flags(carga_cfg)
        registro["comando_carga"] = args
        registro["inicio_processo_carga"] = agora()
        with (dest/"carga.log").open("w", encoding="utf-8") as log:
            carga = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, creationflags=HIDDEN)
            queda = case.get("encerrar_primeiro_apos_segundos")
            if queda is not None:
                time.sleep(queda)
                registro["evento_falha"] = {"instante": agora(), "broker": 1,
                                            "referencia": "após iniciar o processo do gerador"}
                servidores[0].terminate()
                servidores[0].wait(timeout=5)
            code = carga.wait(timeout=45)
        if code:
            raise RuntimeError("Gerador falhou: "+(dest/"carga.log").read_text(encoding="utf-8"))
        resumo = json.loads((dest/"resumo.json").read_text(encoding="utf-8"))
        counters = [{"broker": i+1, "estatisticas": estatisticas(a)}
                    for i, (a, p) in enumerate(zip(enderecos, servidores)) if p.poll() is None]
        gravar(dest/"brokers-final.json", counters)
        if resumo["entregas_unicas"] <= 0 or resumo["publicacoes_escritas_no_socket"] <= 0:
            raise RuntimeError("Não houve comunicação útil")
        if nome == "excesso" and not any(c["estatisticas"]["rejeitadas"] for c in counters):
            raise RuntimeError("Cenário não exercitou o teto do broker")
        if nome == "lento" and resumo["descartes_fila_cliente"] + sum(c["estatisticas"]["descartadas"] for c in counters) == 0:
            raise RuntimeError("Cenário não exercitou descarte")
        if nome == "dois-brokers" and not any(c["estatisticas"]["repasses"] for c in counters):
            raise RuntimeError("Cenário não exercitou repasse")
        if queda is not None and (len(counters) != 1 or counters[0]["estatisticas"]["aceitas"] == 0):
            raise RuntimeError("Não retomou publicações no sobrevivente")
        registro["sucesso"] = True
        registro["fim"] = agora()
        print(f"OK {nome} #{rep}: {resumo['entregas_unicas']} entregas; "
              f"{resumo['entregas_ausentes_entre_envios_sem_erro']} ausentes", flush=True)
        return registro
    finally:
        if carga is not None and carga.poll() is None:
            carga.terminate(); carga.wait(timeout=5)
        for p in servidores:
            if p.poll() is None:
                p.terminate(); p.wait(timeout=5)
        for log in handles:
            log.close()
        gravar(dest/"execucao.json", registro)


def executable(name):
    return name+(".exe" if os.name == "nt" else "")


def fonte_git(root):
    # O código também pode vir de um ZIP, sem Git instalado ou sem .git.
    try:
        top = subprocess.check_output(["git", "rev-parse", "--show-toplevel"], cwd=root,
                                      stderr=subprocess.DEVNULL, text=True).strip()
        if Path(top).resolve() != root.resolve():
            return None, b""
        base = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root,
                                       stderr=subprocess.DEVNULL, text=True).strip()
        patch = subprocess.check_output(["git", "diff", "HEAD", "--binary"], cwd=root,
                                        stderr=subprocess.DEVNULL)
        return base, patch
    except (OSError, subprocess.CalledProcessError):
        return None, b""


def hashes_fonte(root):
    paths = [root/"go.mod"]
    for dirname in ("client", "cmd", "internal", "protocol", "scripts", "experimentos"):
        paths.extend(p for p in (root/dirname).rglob("*")
                     if p.is_file() and p.suffix in (".go", ".py", ".json", ".txt"))
    return {str(p.relative_to(root)).replace("\\", "/"): sha(p) for p in sorted(paths)}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", type=Path, default=ROOT/"experimentos/configuracao.json")
    ap.add_argument("--saida", type=Path, required=True)
    ap.add_argument("--go", default="go")
    ap.add_argument("--bin-dir", type=Path, default=ROOT/"bin")
    ap.add_argument("--sem-testes", action="store_true", help="usar apenas após verificar a mesma revisão")
    ap.add_argument("--sem-graficos", action="store_true")
    args = ap.parse_args()
    cfg = json.loads(args.config.read_text(encoding="utf-8"))
    if not 1 <= cfg["repeticoes"] <= 20:
        raise ValueError("repeticoes deve estar entre 1 e 20")
    out, bin_dir = args.saida.resolve(), args.bin_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    bin_dir.mkdir(parents=True, exist_ok=True)
    gravar(out/"configuracao.json", cfg)
    versao = subprocess.check_output([args.go, "version"], cwd=ROOT, text=True).strip()
    base, patch = fonte_git(ROOT)
    (out/"fonte.patch").write_bytes(patch)
    with (out/"verificacao.log").open("w", encoding="utf-8") as log:
        if not args.sem_testes:
            subprocess.run([args.go, "test", "./...", "-count=1"], cwd=ROOT, stdout=log,
                           stderr=subprocess.STDOUT, check=True)
        for cmd in ("broker", "carga"):
            subprocess.run([args.go, "build", "-o", str(bin_dir/executable(cmd)), "./cmd/"+cmd],
                           cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    plano = []
    rng = random.Random(cfg["semente_ordem"])
    for rep in range(1, cfg["repeticoes"]+1):
        rodada = list(cfg["cenarios"])
        rng.shuffle(rodada)
        plano.extend((c, rep) for c in rodada)
    manifesto = {"inicio": agora(), "sistema": platform.platform(), "arquitetura": platform.machine(),
                 "cpu": platform.processor(), "cpus_logicas": os.cpu_count(), "go": versao,
                 "base_commit": base, "fonte_patch_sha256": sha(out/"fonte.patch"),
                 "sha256_arquivos_fonte": hashes_fonte(ROOT),
                 "configuracao_sha256": sha(out/"configuracao.json"), "testes_executados": not args.sem_testes,
                 "sha256_scripts": {name: sha(ROOT/"scripts"/name) for name in ("experimentos.py", "comparar.py")},
                 "sha256_binarios": {cmd: sha(bin_dir/executable(cmd)) for cmd in ("broker", "carga")},
                 "plano": [{"cenario": c["nome"], "repeticao": r} for c, r in plano], "execucoes": []}
    gravar(out/"manifesto.json", manifesto)
    for i, (case, rep) in enumerate(plano):
        print(f"[{i+1}/{len(plano)}] {case['nome']} repetição {rep}", flush=True)
        manifesto["execucoes"].append(executar(case, rep, cfg, out, bin_dir))
        gravar(out/"manifesto.json", manifesto)
    manifesto["fim"] = agora()
    gravar(out/"manifesto.json", manifesto)
    if not args.sem_graficos:
        subprocess.run([sys.executable, str(ROOT/"scripts/comparar.py"), "--entrada", str(out)],
                       cwd=ROOT, check=True)
    print("Resultados:", out, flush=True)


if __name__ == "__main__":
    main()
