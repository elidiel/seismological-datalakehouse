import os
import subprocess
from datetime import datetime, timedelta

# === CONFIGURAÇÕES ===
CAMINHO_LOCAL_BASE = "/home/elidiel/Documentos/RSISNE_2025_Out_V1/SDS/2025/NB"
CAMINHO_HDFS_BASE = "/sismologia/bruta/2025"

# Estações e sensores disponíveis
ESTACOES = ["NBCB", "NBCL", "NBCM", "NBES", "NBFL", "NBIT", "NBLP", "NBLV",
            "NBMO", "NBPE", "NBPN", "NBPS", "NBSB", "TORO"]
SENSORES = ["HHE", "HHN", "HHZ"]

# Mapeamento de dias julianos 274–295 → 2025-10-01 a 2025-10-22
data_inicial = datetime(2025, 10, 1)
dias = {274 + i: (data_inicial + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(22)}

def rodar(cmd):
    """Executa comandos do sistema e mostra mensagens amigáveis"""
    print(f"🟢 Executando: {cmd}")
    resultado = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if resultado.returncode != 0:
        print(f"❌ Erro: {resultado.stderr.strip()}")
    else:
        print(f"✅ Sucesso")
    return resultado.returncode == 0

def ingestao_dados():
    for estacao in ESTACOES:
        print(f"\n📡 Iniciando ingestão da estação: {estacao}")

        for sensor in SENSORES:
            caminho_local_sensor = os.path.join(CAMINHO_LOCAL_BASE, estacao, f"{sensor}.D")

            if not os.path.exists(caminho_local_sensor):
                print(f"⚠️  Caminho não encontrado: {caminho_local_sensor}")
                continue

            for arquivo in sorted(os.listdir(caminho_local_sensor)):
                # Exemplo: NB.NBIT..HHE.D.2025.274
                if not arquivo.endswith(".D") and ".D." not in arquivo:
                    continue

                partes = arquivo.split(".")
                try:
                    dia_juliano = int(partes[-1])
                except (IndexError, ValueError):
                    print(f"⚠️ Nome inesperado: {arquivo}")
                    continue

                data_real = dias.get(dia_juliano)
                if not data_real:
                    print(f"⚠️ Dia juliano {dia_juliano} fora do intervalo (274–295)")
                    continue

                # Criar diretório no HDFS
                destino_hdfs = f"{CAMINHO_HDFS_BASE}/{estacao}/{data_real}/{sensor}"
                rodar(f"hadoop fs -mkdir -p {destino_hdfs}")

                # Copiar arquivo
                caminho_arquivo_local = os.path.join(caminho_local_sensor, arquivo)
                rodar(f"hadoop fs -put -f {caminho_arquivo_local} {destino_hdfs}/")

    print("\n🚀 Ingestão completa de todas as estações e sensores concluída com sucesso!")

if __name__ == "__main__":
    ingestao_dados()
