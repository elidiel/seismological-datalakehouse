#!/usr/bin/env python3
import json
import requests
import csv
import os
import sys
from datetime import datetime

ATLAS_API = "http://localhost:21000/api/atlas/v2"
AUTH = ("admin", "admin")
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

def criar_tipo_muito_simples():
    """Cria um tipo MUITO simples sem atributos complexos"""
    
    print("🆕 CRIANDO TIPO MUITO SIMPLES...")
    
    NOVO_TIPO = "BoletimSismicov3"
    
    # Tipo SUPER simples - sem atributos que possam conflitar
    tipo_def = {
        "entityDefs": [
            {
                "name": NOVO_TIPO,
                "superTypes": ["Referenceable"],  # Usar Referenceable como pai
                "serviceType": "atlas",
                "typeVersion": "1.0",
                "attributeDefs": [
                    # Apenas atributos simples que não existem no pai
                    {
                        "name": "data_inicio_boletim",
                        "typeName": "string",
                        "isOptional": False
                    },
                    {
                        "name": "data_fim_boletim",
                        "typeName": "string",
                        "isOptional": False
                    },
                    {
                        "name": "quantidade_eventos",
                        "typeName": "int",
                        "isOptional": False
                    }
                ]
            }
        ]
    }
    
    print(f"📋 Criando tipo: {NOVO_TIPO}")
    
    try:
        response = requests.post(
            f"{ATLAS_API}/types/typedefs",
            json=tipo_def,
            auth=AUTH,
            headers=HEADERS,
            timeout=30
        )
        
        print(f"📊 Resposta: {response.status_code}")
        
        if response.status_code == 200:
            print(f"✅ Tipo {NOVO_TIPO} criado com sucesso!")
            return NOVO_TIPO
        elif response.status_code == 409:
            print(f"⚠️ Tipo {NOVO_TIPO} já existe")
            return NOVO_TIPO
        else:
            print(f"❌ Falha: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return None

def criar_boletim_simples_direto(nome_tipo, csv_path):
    """Cria boletim de forma direta e simples"""
    
    print(f"\n📁 CRIANDO BOLETIM SIMPLES...")
    print(f"   Tipo: {nome_tipo}")
    print(f"   CSV: {csv_path}")
    
    # Ler CSV apenas para contar eventos
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            eventos = [row for row in reader if row.get('eventID')]
        
        total_eventos = len(eventos)
        print(f"📊 Total de eventos no CSV: {total_eventos}")
        
    except Exception as e:
        print(f"❌ Erro ao ler CSV: {e}")
        total_eventos = 0
    
    # Criar payload SIMPLES
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    payload = {
        "entities": [
            {
                "typeName": nome_tipo,
                "createdBy": "admin",
                "attributes": {
                    "qualifiedName": f"boletim.simples.{timestamp}",
                    "name": f"Boletim Simples {timestamp}",
                    "description": f"Boletim gerado de {os.path.basename(csv_path)}",
                    "data_inicio_boletim": "2025-10-01",
                    "data_fim_boletim": "2025-10-16",
                    "quantidade_eventos": total_eventos
                }
            }
        ]
    }
    
    print("\n📋 Payload SIMPLES:")
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    
    # Salvar payload
    with open("payload_ultra_simples.json", "w") as f:
        json.dump(payload, f, indent=2)
    
    print("💾 Payload salvo em: payload_ultra_simples.json")
    
    # Enviar
    print("\n🚀 Enviando para o Atlas...")
    
    try:
        response = requests.post(
            f"{ATLAS_API}/entity",
            json=payload,
            auth=AUTH,
            headers=HEADERS,
            timeout=30
        )
        
        print(f"\n📊 RESPOSTA DO ATLAS:")
        print(f"Status: {response.status_code}")
        
        if response.status_code in [200, 201]:
            print("🎉🎉🎉 SUCESSO! BOLETIM CRIADO! 🎉🎉🎉")
            
            result = response.json()
            print(f"\n📋 Resposta completa:")
            print(json.dumps(result, indent=2, ensure_ascii=False))
            
            # Salvar sucesso
            if "guidAssignments" in result:
                for qname, guid in result["guidAssignments"].items():
                    with open("SUCESSO_SIMPLES.txt", "w") as f:
                        f.write(f"✅ BOLETIM CRIADO COM SUCESSO!\n")
                        f.write(f"Tipo: {nome_tipo}\n")
                        f.write(f"GUID: {guid}\n")
                        f.write(f"QualifiedName: {qname}\n")
                        f.write(f"CSV: {csv_path}\n")
                        f.write(f"Eventos: {total_eventos}\n")
                    
                    print(f"\n🔑 GUID: {guid}")
                    print(f"📝 Qualified Name: {qname}")
                    break
            
            return True
            
        else:
            print(f"❌ Falha: {response.text}")
            
            # Analisar erro
            if "null entity" in response.text:
                print("\n🔍 PROBLEMA: 'null entity'")
                print("O Atlas não está aceitando a estrutura básica.")
                
            return False
            
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return False

def testar_atlas_basico():
    """Testes básicos do Atlas"""
    
    print("\n🔍 TESTANDO ATLAS...")
    
    # Teste 1: Status
    try:
        response = requests.get(
            f"{ATLAS_API}/admin/status",
            auth=AUTH,
            headers=HEADERS,
            timeout=10
        )
        print(f"✅ Status do Atlas: {response.status_code}")
    except:
        print("❌ Não conseguiu conectar ao Atlas")
    
    # Teste 2: Ver tipos existentes
    try:
        response = requests.get(
            f"{ATLAS_API}/types/typedefs",
            auth=AUTH,
            headers=HEADERS,
            timeout=10
        )
        if response.status_code == 200:
            tipos = response.json()
            print(f"✅ Total de tipos: {len(tipos.get('entityDefs', []))}")
    except:
        print("❌ Não conseguiu listar tipos")
    
    # Teste 3: Tentar criar algo MUITO simples sem tipo personalizado
    print("\n🧪 Testando criação sem tipo personalizado...")
    
    payload_teste = {
        "entities": [
            {
                "typeName": "Referenceable",
                "createdBy": "admin",
                "attributes": {
                    "qualifiedName": f"teste.basico.{datetime.now().strftime('%H%M%S')}",
                    "name": "Teste Básico"
                }
            }
        ]
    }
    
    try:
        response = requests.post(
            f"{ATLAS_API}/entity",
            json=payload_teste,
            auth=AUTH,
            headers=HEADERS,
            timeout=10
        )
        
        print(f"📊 Teste básico: {response.status_code}")
        if response.status_code in [200, 201]:
            print("✅ Atlas aceita criação básica!")
            return True
        else:
            print(f"❌ Falha no teste básico: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Erro no teste: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("SOLUÇÃO ULTRA SIMPLES")
    print("="*60)
    
    # Verificar CSV
    if len(sys.argv) < 2:
        print("Uso: python3 solucao_simples.py <caminho_csv>")
        return
    
    csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        print(f"❌ Arquivo não encontrado: {csv_path}")
        return
    
    # 1. Testar Atlas básico
    print("\n1️⃣ TESTANDO ATLAS BÁSICO...")
    atlas_funciona = testar_atlas_basico()
    
    if not atlas_funciona:
        print("\n❌ Atlas não está funcionando corretamente.")
        print("💡 Reinicie o Atlas: sudo systemctl restart atlas")
        return
    
    # 2. Tentar criar tipo simples
    print("\n2️⃣ CRIANDO TIPO SIMPLES...")
    nome_tipo = criar_tipo_muito_simples()
    
    # 3. Criar boletim
    print("\n3️⃣ CRIANDO BOLETIM...")
    if nome_tipo:
        criar_boletim_simples_direto(nome_tipo, csv_path)
    else:
        # Se não conseguiu criar tipo, tentar com Referenceable
        print("\n⚠️ Usando tipo Referenceable como fallback...")
        criar_boletim_simples_direto("Referenceable", csv_path)
    
    print("\n" + "="*60)
    print("COMANDOS PARA TESTE MANUAL:")
    print("="*60)
    
    print("""
# 1. Criar tipo super simples
curl -X POST "http://localhost:21000/api/atlas/v2/types/typedefs" \\
  -H "Content-Type: application/json" \\
  -u admin:admin \\
  -d '{
    "entityDefs": [
      {
        "name": "MeuBoletimTeste",
        "superTypes": ["Referenceable"],
        "serviceType": "atlas",
        "typeVersion": "1.0",
        "attributeDefs": [
          {
            "name": "minha_data",
            "typeName": "string",
            "isOptional": false
          }
        ]
      }
    ]
  }'

# 2. Criar entidade simples
curl -X POST "http://localhost:21000/api/atlas/v2/entity" \\
  -H "Content-Type: application/json" \\
  -u admin:admin \\
  -d '{
    "entities": [
      {
        "typeName": "MeuBoletimTeste",
        "createdBy": "admin",
        "attributes": {
          "qualifiedName": "meu.teste.001",
          "name": "Meu Teste 001",
          "minha_data": "2025-10-01"
        }
      }
    ]
  }'

# 3. Ver entidades criadas
curl -u admin:admin "http://localhost:21000/api/atlas/v2/search/basic?query=MeuBoletimTeste&typeName=MeuBoletimTeste" | jq
""")
    
    print("="*60)

if __name__ == "__main__":
    main()
