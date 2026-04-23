import os
import json
import subprocess
import requests
import tempfile
from datetime import datetime
from obspy import read_inventory, read

# === CONFIGURAÇÕES GERAIS ===
ATLAS_URL = "http://localhost:21000/api/atlas/v2"
AUTH = ("admin", "admin")
HEADERS = {"Content-Type": "application/json"}

# Caminho base no HDFS
HDFS_BASE = "/sismologia/bruta/2025"

# Endereço do NameNode para gerar links clicáveis
NAMENODE_URL = "http://localhost:9870/explorer.html#"

# Sensores a serem processados
SENSORES_VALIDOS = ["HHE", "HHN", "HHZ"]


# ====================== FUNÇÕES BÁSICAS ======================

def testar_conexao_atlas():
    """Verifica se o Atlas está online e autenticado"""
    try:
        resp = requests.get("http://localhost:21000/api/atlas/admin/version", auth=AUTH, timeout=10)
        if resp.status_code == 200:
            dados = resp.json()
            print(f"✅ Atlas está online e autenticado!")
            print(f"📦 Versão: {dados.get('Version', 'desconhecida')}")
            return True
        else:
            print(f"❌ Atlas retornou código HTTP {resp.status_code}")
            return False
    except Exception as e:
        print(f"❌ Falha ao conectar ao Atlas: {e}")
        return False


def executar_hdfs(cmd):
    """Executa comandos HDFS"""
    try:
        resultado = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return resultado.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro no HDFS ({cmd}): {e.stderr.strip()}")
        return None


def copiar_do_hdfs(origem, destino):
    """Copia um arquivo do HDFS para local"""
    try:
        if os.path.exists(destino):
            os.remove(destino)
        subprocess.run(["hadoop", "fs", "-copyToLocal", origem, destino], check=True)
        return os.path.exists(destino)
    except Exception as e:
        print(f"❌ Falha ao copiar {origem}: {e}")
        return False


# ====================== COLETA DE METADADOS ======================

def listar_estacoes_hdfs():
    print("🔍 Listando estações no HDFS...")
    saida = executar_hdfs(["hadoop", "fs", "-ls", HDFS_BASE])
    if not saida:
        return []
    estacoes = [linha.split()[-1].split("/")[-1] for linha in saida.split("\n") if HDFS_BASE in linha]
    print(f"🏢 Encontradas {len(estacoes)} estações: {estacoes}")
    return estacoes


def buscar_dataless(estacao):
    saida = executar_hdfs(["hadoop", "fs", "-find", f"{HDFS_BASE}/{estacao}", "-name", "*.ds"])
    if not saida:
        return []
    arquivos = [a for a in saida.split("\n") if a.strip()]
    print(f"   📄 {len(arquivos)} dataless encontrados em {estacao}")
    return arquivos


def buscar_miniseed(estacao):
    saida = executar_hdfs(["hadoop", "fs", "-find", f"{HDFS_BASE}/{estacao}", "-name", "*D.*"])
    if not saida:
        return []
    arquivos = [a for a in saida.split("\n") if any(sensor in a for sensor in SENSORES_VALIDOS)]
    print(f"   📊 {len(arquivos)} arquivos MiniSEED válidos em {estacao}")
    return arquivos


# ====================== FUNÇÕES DO ATLAS ======================

def entidade_existe(tipo, qualified_name):
    """Verifica se a entidade já existe no Atlas"""
    url = f"{ATLAS_URL}/entity/uniqueAttribute/type/{tipo}"
    params = {"attr:qualifiedName": qualified_name}
    r = requests.get(url, auth=AUTH, params=params)
    return r.status_code == 200


def criar_entidade(tipo, atributos):
    """Cria uma entidade no Atlas"""
    if "qualifiedName" not in atributos:
        print(f"⚠️ {tipo} sem qualifiedName — ignorado.")
        return False
    if "name" not in atributos or not atributos["name"]:
        atributos["name"] = atributos["qualifiedName"].split(".")[-1]

    payload = {"entities": [{"typeName": tipo, "attributes": atributos}]}

    try:
        r = requests.post(f"{ATLAS_URL}/entity/bulk", json=payload, auth=AUTH, headers=HEADERS)
        if r.status_code == 200:
            print(f"✅ {tipo} criado: {atributos['qualifiedName']}")
            return True
        else:
            print(f"❌ Falha ao criar {tipo} ({r.status_code}): {r.text[:200]}")
            return False
    except Exception as e:
        print(f"❌ Erro ao criar {tipo}: {e}")
        return False


# ====================== EXTRAÇÃO DE METADADOS ======================

def extrair_dataless(caminho_hdfs):
    """Extrai metadados do arquivo dataless"""
    with tempfile.NamedTemporaryFile(suffix=".ds", delete=False) as tmp:
        local_path = tmp.name
    if not copiar_do_hdfs(caminho_hdfs, local_path):
        return None

    try:
        inv = read_inventory(local_path)
        rede = inv.networks[0]
        estacao = rede.stations[0]

        def get_valor(attr):
            if attr is None:
                return None
            if hasattr(attr, "value"):
                return float(attr.value)
            return float(attr)

        return {
            "qualifiedName": f"sismologia.estacao.{estacao.code}",
            "name": estacao.code,
            "sigla": estacao.code,
            "rede": rede.code,
            "local": estacao.site.name if estacao.site else "Desconhecido",
            "latitude": get_valor(estacao.latitude),
            "longitude": get_valor(estacao.longitude),
            "elevacao": get_valor(estacao.elevation),
            "descricao": f"Estação {estacao.code} - Rede {rede.code}",
            "data_inicio": int(estacao.start_date.timestamp() * 1000) if estacao.start_date else None,
            "data_fim": int(estacao.end_date.timestamp() * 1000) if estacao.end_date else None,
            "caminho_dataless": caminho_hdfs
        }

    except Exception as e:
        print(f"❌ Erro ao ler dataless: {e}")
        return None
    finally:
        os.remove(local_path)


def extrair_miniseed(caminho_hdfs, estacao_nome):
    """Extrai metadados de arquivos MiniSEED"""
    with tempfile.NamedTemporaryFile(suffix=".mseed", delete=False) as tmp:
        local_path = tmp.name
    if not copiar_do_hdfs(caminho_hdfs, local_path):
        return None

    try:
        st = read(local_path)
        tr = st[0]
        stats = tr.stats
        sensor = stats.channel
        data_dir = caminho_hdfs.split("/")[-3] if "-" in caminho_hdfs.split("/")[-3] else "desconhecido"
        hdfs_link = f"{NAMENODE_URL}{caminho_hdfs}"

        return {
            "qualifiedName": f"sismologia.dados.{estacao_nome}.{sensor}.{data_dir}",
            "name": os.path.basename(caminho_hdfs),
            "formato": "MiniSEED",
            "sensor": sensor,
            "caminho_hdfs": caminho_hdfs,
            "download_url": hdfs_link,
            "data_inicio": str(stats.starttime),
            "data_fim": str(stats.endtime),
            "estacao_relacionada": {
                "typeName": "EstacaoSismologica",
                "uniqueAttributes": {"qualifiedName": f"sismologia.estacao.{estacao_nome}"}
            }
        }
    except Exception as e:
        print(f"❌ Erro ao ler MiniSEED: {e}")
        return None
    finally:
        os.remove(local_path)


# ====================== PROCESSAMENTO PRINCIPAL ======================

def processar_estacao(estacao):
    print(f"\n🎯 Processando estação: {estacao}")
    dataless = buscar_dataless(estacao)
    miniseeds = buscar_miniseed(estacao)

    # Criar a estação
    if dataless:
        meta = extrair_dataless(dataless[0])
        if meta and not entidade_existe("EstacaoSismologica", meta["qualifiedName"]):
            criar_entidade("EstacaoSismologica", meta)
    else:
        print(f"⚠️ Nenhum dataless encontrado para {estacao}")

    # Criar os dados sísmicos
    for arq in miniseeds:
        meta = extrair_miniseed(arq, estacao)
        if not meta:
            continue
        if meta["sensor"].upper() not in SENSORES_VALIDOS:
            continue
        if entidade_existe("DadosDeIngestaoV2", meta["qualifiedName"]):
            print(f"♻️ Dados já existentes: {meta['qualifiedName']}")
            continue
        criar_entidade("DadosDeIngestaoV2", meta)


def main():
    print("🚀 REGISTRADOR DE ESTAÇÕES E SENSORES SISMOLÓGICOS")
    print("=" * 60)

    if not testar_conexao_atlas():
        print("❌ Atlas indisponível. Abortando.")
        return

    estacoes = listar_estacoes_hdfs()
    for est in estacoes:
        processar_estacao(est)

    print("\n✅ Execução concluída com sucesso!")


if __name__ == "__main__":
    main()
