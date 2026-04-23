# registrar_eventos_sismicos.py - VERSÃO COM DATAS CORRIGIDAS
import pandas as pd
import requests
import json
import subprocess
from datetime import datetime
import os
import time

ATLAS_URL = "http://localhost:21000/api/atlas/v2"
ATLAS_AUTH = ("admin", "admin")

# CAMINHO CORRIGIDO - usando /opt/airflow/
CSV_PATH = "/opt/airflow/dados/eventos/Consulta_banco_Siescomp3_01-10-2025_a_22-10-2025.csv"

HDFS_BASE = "/sismologia/bruta/2025"

# -------------------------- ATLAS UTILS --------------------------

def formatar_data_atlas(data_br, hora_utc):
    """Converte data brasileira + hora UTC para formato Atlas (timestamp ms)"""
    try:
        # Data no formato DD/MM/YYYY + Hora UTC
        data_str = f"{data_br} {hora_utc}"
        data_obj = datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S")
        
        # Converte para timestamp em milissegundos (formato que Atlas espera)
        timestamp_ms = int(data_obj.timestamp() * 1000)
        return timestamp_ms
    except Exception as e:
        print(f"❌ Erro ao formatar data {data_br} {hora_utc}: {e}")
        return None

def atlas_get(guid):
    """Busca entidade no Atlas pelo GUID"""
    try:
        r = requests.get(f"{ATLAS_URL}/entity/guid/{guid}", auth=ATLAS_AUTH, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar GUID {guid}: {e}")
        return None

def atlas_create(entity):
    """Cria entidade no Atlas - CORREÇÃO DA API"""
    try:
        # CORREÇÃO: A API espera {"entity": entity} e retorna de forma diferente
        payload = {"entity": entity}
        r = requests.post(f"{ATLAS_URL}/entity", json=payload, auth=ATLAS_AUTH, timeout=30)
        r.raise_for_status()
        response = r.json()
        
        # A API retorna de formas diferentes dependendo da versão
        # Vamos tentar diferentes formatos de resposta
        if "mutatedEntities" in response:
            # Formato mais comum
            created = response.get("mutatedEntities", {}).get("CREATE", [])
            if created:
                return created[0]  # Retorna a primeira entidade criada
        elif "guidAssignments" in response:
            # Outro formato possível
            assignments = response["guidAssignments"]
            if assignments:
                first_key = list(assignments.keys())[0]
                return {"guid": assignments[first_key], "qualifiedName": first_key}
        elif "guid" in response:
            # Às vezes retorna diretamente
            return response
        
        print(f"⚠️  Formato de resposta não reconhecido: {response}")
        return None
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao criar entidade: {e}")
        if hasattr(e, 'response') and e.response is not None:
            error_text = e.response.text[:500]  # Mostra mais detalhes do erro
            print(f"   Resposta: {error_text}")
        return None

def atlas_search_by_qn(typeName, qualifiedName):
    """Busca entidade por qualifiedName - CORREÇÃO"""
    try:
        # CORREÇÃO: Usando a API de busca direta por qualifiedName
        url = f"{ATLAS_URL}/entity/uniqueAttribute/type/{typeName}?attr:qualifiedName={qualifiedName}"
        r = requests.get(url, auth=ATLAS_AUTH, timeout=30)
        
        if r.status_code == 200:
            entity = r.json().get("entity")
            if entity:
                return entity["guid"]
        elif r.status_code == 404:
            return None  # Entidade não encontrada
        else:
            r.raise_for_status()
            
        return None
    except requests.exceptions.RequestException as e:
        if e.response is not None and e.response.status_code == 404:
            return None  # Entidade não existe
        print(f"❌ Erro na busca por {qualifiedName}: {e}")
        return None

def atlas_update(guid, attributes):
    """Atualiza atributos de uma entidade existente"""
    try:
        # Primeiro busca a entidade para obter o typeName
        entity_data = atlas_get(guid)
        if not entity_data:
            print(f"❌ Entidade {guid} não encontrada para atualização")
            return None
            
        type_name = entity_data.get("definition", {}).get("typeName")
        if not type_name:
            type_name = "EventoSismico"  # Fallback
            
        payload = {
            "entity": {
                "guid": guid,
                "typeName": type_name,
                "attributes": attributes
            }
        }
        r = requests.put(f"{ATLAS_URL}/entity", json=payload, auth=ATLAS_AUTH, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao atualizar {guid}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Resposta: {e.response.text[:200]}")
        return None

# -------------------------- HDFS SEARCH --------------------------

def listar_miniseed_data(estacao, data_evento):
    """
    Procura arquivos miniseed dessa estação nesse dia.
    data_evento formato: DD/MM/YYYY (como no CSV)
    """
    try:
        # Converte data do formato brasileiro para o usado no HDFS
        data_obj = datetime.strptime(data_evento, "%d/%m/%Y")
        data_hdfs = data_obj.strftime("%Y-%m-%d")
        
        hdfs_path = f"{HDFS_BASE}/{estacao}/{data_hdfs}"
        print(f"   🔍 Procurando em HDFS: {hdfs_path}")

        # Tenta usar comando hadoop
        try:
            output = subprocess.check_output(
                ["hadoop", "fs", "-ls", "-R", hdfs_path], 
                stderr=subprocess.STDOUT,
                timeout=30
            ).decode()
            
            arquivos = []
            for linha in output.split("\n"):
                if ".D" in linha and "drwx" not in linha:  # Ignora diretórios
                    partes = linha.split()
                    if len(partes) > 7:
                        caminho_completo = partes[-1]
                        arquivos.append(caminho_completo)
            
            return arquivos
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            # Se hadoop não funcionar, tenta via WebHDFS
            print(f"   ⚠️  Hadoop não disponível, tentando WebHDFS...")
            webhdfs_url = f"http://localhost:9870/webhdfs/v1{hdfs_path}?op=LISTSTATUS"
            try:
                r = requests.get(webhdfs_url, timeout=30)
                if r.status_code == 200:
                    data = r.json()
                    arquivos = []
                    for item in data.get("FileStatuses", {}).get("FileStatus", []):
                        if ".D" in item["pathSuffix"]:
                            arquivos.append(f"{hdfs_path}/{item['pathSuffix']}")
                    return arquivos
            except:
                pass
        
        return []
        
    except Exception as e:
        print(f"   ⚠️  Erro ao buscar arquivos HDFS: {e}")
        return []

# -------------------------- CSV PROCESSING --------------------------

def processar_linha_csv(row):
    """Processa uma linha do CSV e extrai os dados"""
    try:
        data_evento = row["Data"]  # formato: DD/MM/YYYY
        hora_utc = row["Hora_UTC"]
        hora_br = row["Hora_Brasilia"]
        lat = float(row["Latitude_Original"])
        lon = float(row["Longitude_Original"])
        mag = float(row["Magnitude"])
        tipo_mag = str(row["Tipo_Magnitude"])
        prof = float(row["Profundidade_km"])
        estacoes_detectaram = int(row["Estacoes_Detectaram"])
        codigo_estacao = row["Codigo_Estacao"]
        local_estacao = row.get("Localidade_Estacao", "")
        autor_analise = row.get("Autor_Analise", "")
        event_id = row.get("eventID", "")
        
        # Formata data para o Atlas
        data_atlas = formatar_data_atlas(data_evento, hora_utc)
        
        return {
            "data_evento": data_evento,
            "hora_utc": hora_utc,
            "hora_br": hora_br,
            "latitude": lat,
            "longitude": lon,
            "magnitude": mag,
            "tipo_magnitude": tipo_mag,
            "profundidade_km": prof,
            "estacoes_detectaram": estacoes_detectaram,
            "codigo_estacao": codigo_estacao,
            "local_estacao": local_estacao,
            "autor_analise": autor_analise,
            "event_id": event_id,
            "data_atlas": data_atlas  # Data formatada para o Atlas
        }
    except Exception as e:
        print(f"❌ Erro ao processar linha: {e}")
        print(f"   Dados: {dict(row)}")
        return None

# -------------------------- MAIN --------------------------

def main():
    print("\n🚀 REGISTRADOR DE EVENTOS SÍSMICOS - DATAS CORRIGIDAS")
    print("============================================================\n")
    
    # Verifica se o arquivo existe
    if not os.path.exists(CSV_PATH):
        print(f"❌ Arquivo não encontrado: {CSV_PATH}")
        print("   Verifique o caminho e tente novamente")
        return
    
    try:
        df = pd.read_csv(CSV_PATH, encoding='utf-8')
        print(f"📊 CSV carregado: {len(df)} linhas encontradas")
    except Exception as e:
        print(f"❌ Erro ao ler CSV: {e}")
        return
    
    # Agrupa por evento (eventID) para evitar duplicatas
    eventos_processados = set()
    eventos_criados = 0
    relacoes_estabelecidas = 0
    
    for idx, row in df.iterrows():
        dados = processar_linha_csv(row)
        if not dados or dados["data_atlas"] is None:
            print(f"❌ Dados inválidos ou data não formatada, pulando...")
            continue
            
        event_id = dados["event_id"]
        
        # Pula se já processou este evento
        if event_id in eventos_processados:
            continue
            
        eventos_processados.add(event_id)
        
        print(f"\n🎯 Processando evento: {event_id}")
        print(f"   📅 {dados['data_evento']} {dados['hora_utc']} UTC")
        print(f"   📍 {dados['latitude']}, {dados['longitude']}")
        print(f"   🌋 Magnitude: {dados['magnitude']} {dados['tipo_magnitude']}")
        print(f"   ⏰ Timestamp Atlas: {dados['data_atlas']}")
        
        # Qualified Name único para o evento
        qn_evento = f"sismologia.evento.{event_id.replace('/', '_').replace(':', '_')}"
        
        # Verificar se evento já existe
        evento_guid = atlas_search_by_qn("EventoSismico", qn_evento)
        
        if evento_guid:
            print(f"   ⚠️  Evento já existe no Atlas: {qn_evento}")
        else:
            # Criar evento no Atlas - COM DATA CORRIGIDA
            entidade_evento = {
                "typeName": "EventoSismico",
                "attributes": {
                    "qualifiedName": qn_evento,
                    "name": f"Evento {dados['data_evento']} {dados['hora_utc']}",
                    "eventID": event_id,
                    "data_evento": dados['data_atlas'],  # TIMESTAMP CORRETO
                    "hora_utc": dados['hora_utc'],
                    "hora_brasilia": dados['hora_br'],
                    "latitude": dados['latitude'],
                    "longitude": dados['longitude'],
                    "magnitude": dados['magnitude'],
                    "tipo_magnitude": dados['tipo_magnitude'],
                    "profundidade_km": dados['profundidade_km'],
                    "estacoes_detectaram": dados['estacoes_detectaram'],
                    "autor_analise": dados['autor_analise'],
                    "localidade": dados['local_estacao']
                }
            }
            
            print(f"   📝 Criando evento no Atlas...")
            resp = atlas_create(entidade_evento)
            
            if resp:
                if "guid" in resp:
                    evento_guid = resp["guid"]
                    print(f"   ✅ Evento registrado: {qn_evento} (GUID: {evento_guid})")
                    eventos_criados += 1
                elif "qualifiedName" in resp:
                    # Tenta buscar o GUID após criação
                    time.sleep(1)
                    evento_guid = atlas_search_by_qn("EventoSismico", qn_evento)
                    if evento_guid:
                        print(f"   ✅ Evento criado e encontrado: {evento_guid}")
                        eventos_criados += 1
                    else:
                        print(f"   ⚠️  Evento criado mas GUID não encontrado")
                        continue
                else:
                    print(f"   ⚠️  Resposta inesperada: {resp}")
                    continue
            else:
                print(f"   ❌ Falha ao criar evento")
                continue
        
        # Buscar estações relacionadas a este evento
        estacoes_do_evento = df[df['eventID'] == event_id]['Codigo_Estacao'].unique()
        estacoes_guids = []
        
        for estacao in estacoes_do_evento:
            qn_estacao = f"sismologia.estacao.{estacao}"
            guid_estacao = atlas_search_by_qn("EstacaoSismologica", qn_estacao)
            if guid_estacao:
                estacoes_guids.append(guid_estacao)
            else:
                print(f"   ⚠️  Estação {estacao} não encontrada no Atlas")
        
        # Buscar arquivos MiniSEED relacionados
        arquivos_brutos = []
        for estacao in estacoes_do_evento:
            arquivos = listar_miniseed_data(estacao, dados['data_evento'])
            for caminho_hdfs in arquivos:
                nome_arquivo = caminho_hdfs.split('/')[-1]
                # Tenta diferentes padrões de qualifiedName
                qn_patterns = [
                    f"sismologia.dados.{estacao}.{nome_arquivo.split('.')[2]}.{dados['data_evento'].replace('/', '-')}",
                    f"sismologia.dados.{estacao}.{nome_arquivo.split('.')[3]}.{dados['data_evento'].replace('/', '-')}",
                    f"sismologia.dados.{estacao}.{nome_arquivo.replace('.', '_')}"
                ]
                
                for qn_dados in qn_patterns:
                    guid_dados = atlas_search_by_qn("DadosDeIngestaoV2", qn_dados)
                    if guid_dados:
                        arquivos_brutos.append(guid_dados)
                        break
        
        # Atualizar evento com relações
        if estacoes_guids or arquivos_brutos:
            atributos_atualizacao = {}
            
            if estacoes_guids:
                atributos_atualizacao["estacoes"] = [{"guid": g, "typeName": "EstacaoSismologica"} for g in estacoes_guids]
                print(f"   🔗 Vinculado a {len(estacoes_guids)} estações")
            
            if arquivos_brutos:
                atributos_atualizacao["dados_brutos"] = [{"guid": g, "typeName": "DadosDeIngestaoV2"} for g in arquivos_brutos]
                print(f"   📎 Vinculado a {len(arquivos_brutos)} arquivos MiniSEED")
            
            if atributos_atualizacao:
                print(f"   🔄 Atualizando relações do evento...")
                resp_update = atlas_update(evento_guid, atributos_atualizacao)
                if resp_update:
                    relacoes_estabelecidas += 1
                    print(f"   ✅ Relações atualizadas com sucesso")
                else:
                    print("   ⚠️  Falha ao atualizar relações do evento")
        else:
            print(f"   ℹ️  Nenhuma estação ou arquivo encontrado para vinculação")
    
    print(f"\n📊 RESUMO FINAL:")
    print(f"✅ Eventos criados/processados: {eventos_criados}")
    print(f"✅ Relações estabelecidas: {relacoes_estabelecidas}")
    print(f"📋 Total de eventos únicos: {len(eventos_processados)}")
    print("\n🎉 Execução concluída com sucesso!\n")

if __name__ == "__main__":
    main()
