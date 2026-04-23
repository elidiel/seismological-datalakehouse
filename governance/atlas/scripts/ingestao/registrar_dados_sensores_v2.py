import os
import json
import subprocess
import requests
from obspy import read

# === CONFIGURAÇÕES ===
ATLAS_URL = "http://localhost:21000/api/atlas/v2/entity/bulk"
HEADERS = {"Content-Type": "application/json"}
AUTH = ("admin", "admin")

# Sensores desejados
SENSORES_VALIDOS = ["HHE", "HHN", "HHZ"]

# Caminho base no HDFS
CAMINHO_BASE_HDFS = "/sismologia/bruta/2020/NB"

# Caminho temporário local para copiar o arquivo MiniSEED
CAMINHO_TMP = "/tmp/sensor.mseed"


def listar_arquivos_hdfs():
    resultado = subprocess.run(["hdfs", "dfs", "-ls", "-R", CAMINHO_BASE_HDFS], capture_output=True, text=True)
    if resultado.returncode != 0:
        print("❌ Erro ao listar arquivos no HDFS")
        return []
    linhas = resultado.stdout.strip().split("\n")
    caminhos = [linha.split()[-1] for linha in linhas if ".D" in linha]
    return caminhos


def copiar_hdfs_para_tmp(caminho_hdfs):
    if os.path.exists(CAMINHO_TMP):
        os.remove(CAMINHO_TMP)
    try:
        subprocess.run(["hdfs", "dfs", "-get", caminho_hdfs, CAMINHO_TMP], check=True)
        return True
    except subprocess.CalledProcessError:
        print(f"⚠️ Falha ao copiar {caminho_hdfs}")
        return False


def buscar_estacao(qualified_name):
    url = f"http://localhost:21000/api/atlas/v2/entity/uniqueAttribute/type/EstacaoSismologica?attr:qualifiedName={qualified_name}"
    resp = requests.get(url, auth=AUTH, headers=HEADERS)
    if resp.status_code == 200 and "entity" in resp.json():
        return resp.json()["entity"]
    return None


def registrar_dados_ingestao_v2(atributos):
    payload = {
        "entities": [
            {
                "typeName": "DadosDeIngestaoV2",
                "attributes": atributos
            }
        ]
    }
    resp = requests.post(ATLAS_URL, json=payload, auth=AUTH, headers=HEADERS)
    if resp.status_code == 200:
        print(f"✅ Registrado: {atributos['qualifiedName']}")
    else:
        print(f"❌ Erro ao registrar: {atributos['qualifiedName']} -> {resp.text}")


def processar_arquivo(caminho_hdfs):
    nome_arquivo = os.path.basename(caminho_hdfs)
    partes = nome_arquivo.split(".")
    if len(partes) < 5:
        return
    _, estacao, _, sensor, data = partes[:5]
    if sensor not in SENSORES_VALIDOS:
        return

    if not copiar_hdfs_para_tmp(caminho_hdfs):
        return

    try:
        st = read(CAMINHO_TMP)
        tr = st[0]
        inicio = int(tr.stats.starttime.timestamp * 1000)
        fim = int(tr.stats.endtime.timestamp * 1000)
    except Exception as e:
        print(f"❌ Falha ao ler {caminho_hdfs}: {e}")
        return
    finally:
        if os.path.exists(CAMINHO_TMP):
            os.remove(CAMINHO_TMP)

    qualified_name_estacao = f"{estacao}@NB"
    estacao_ref = buscar_estacao(qualified_name_estacao)
    if not estacao_ref:
        print(f"⚠️ Estacao não encontrada: {qualified_name_estacao}")
        return

    qualified_name = f"{nome_arquivo}@sismologia"
    atributos = {
        "qualifiedName": qualified_name,
        "name": nome_arquivo,
        "formato": "MiniSEED",
        "data_inicio": inicio,
        "data_fim": fim,
        "sensor": sensor,
        "caminho_hdfs": caminho_hdfs,
        "estacao_relacionada": {
            "typeName": "EstacaoSismologica",
            "uniqueAttributes": {
                "qualifiedName": qualified_name_estacao
            }
        }
    }
    registrar_dados_ingestao_v2(atributos)


def main():
    arquivos = listar_arquivos_hdfs()
    for caminho in arquivos:
        processar_arquivo(caminho)


if __name__ == "__main__":
    main()
