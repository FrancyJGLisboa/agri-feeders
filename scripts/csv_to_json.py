#!/usr/bin/env python3
"""
Converte dados históricos de soja e milho de CSV para JSON compatível com crop-weighted-climate.html.

Usage:
    uv run csv_to_json.py --output ../ibge-data.json
    uv run csv_to_json.py --input-soja ../dataset_soja_2000_2025.csv --input-milho ../dataset_milho_2000_2024.csv --output ../ibge-data.json
    uv run csv_to_json.py --json
"""

# /// script
# dependencies = [
#   "pandas>=2.0.0",
#   "click>=8.0.0",
#   "requests>=2.31.0",
# ]
# ///

import click
import pandas as pd
import json
import sys
import time
import unicodedata
import re
import requests
from datetime import datetime
from typing import Optional, Dict, List, Any

# UF codes to siglas mapping
UF_SIGLAS = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL",
    28: "SE", 29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR",
    42: "SC", 43: "RS", 50: "MS", 51: "MT", 52: "GO", 53: "DF"
}

# State name to UF code mapping
STATE_NAME_TO_UF = {
    "RO": "RO", "RONDONIA": "RO", "AC": "AC", "ACRE": "AC", "AM": "AM", "AMAZONAS": "AM",
    "RR": "RR", "RORAIMA": "RR", "PA": "PA", "PARA": "PA", "AP": "AP", "AMAPA": "AP",
    "TO": "TO", "TOCANTINS": "TO", "MA": "MA", "MARANHAO": "MA", "PI": "PI", "PIAUI": "PI",
    "CE": "CE", "CEARA": "CE", "RN": "RN", "RIO GRANDE DO NORTE": "RN", "PB": "PB",
    "PARAIBA": "PB", "PE": "PE", "PERNAMBUCO": "PE", "AL": "AL", "ALAGOAS": "AL",
    "SE": "SE", "SERGIPE": "SE", "BA": "BA", "BAHIA": "BA", "MG": "MG",
    "MINAS GERAIS": "MG", "ES": "ES", "ESPIRITO SANTO": "ES", "RJ": "RJ",
    "RIO DE JANEIRO": "RJ", "SP": "SP", "SAO PAULO": "SP", "PR": "PR",
    "PARANA": "PR", "SC": "SC", "SANTA CATARINA": "SC", "RS": "RS",
    "RIO GRANDE DO SUL": "RS", "MS": "MS", "MATO GROSSO DO SUL": "MS",
    "MT": "MT", "MATO GROSSO": "MT", "GO": "GO", "GOIAS": "GO", "DF": "DF",
    "DISTRITO FEDERAL": "DF"
}

def normalizar_nome(nome: str) -> str:
    """Remove acentos e normaliza nome para chave."""
    nome = unicodedata.normalize('NFKD', nome)
    nome = ''.join(c for c in nome if not unicodedata.combining(c))
    nome = nome.lower().strip()
    nome = re.sub(r'[^a-z0-9\s-]', '', nome)
    nome = re.sub(r'\s+', '-', nome)
    return nome

def criar_chave_municipio(nome: str, uf: str) -> str:
    """Cria chave padronizada: nome-uf (ex: sorriso-mt)."""
    # Se o nome já contém " - UF", extrair apenas o nome
    if " - " in nome:
        nome = nome.split(" - ")[0].strip()

    nome_norm = normalizar_nome(nome)
    return f"{nome_norm}-{uf.lower()}"

def mapear_uf(nome_uf: str) -> Optional[str]:
    """Mapeia nome do estado para sigla."""
    nome_uf_upper = nome_uf.upper().strip()
    return STATE_NAME_TO_UF.get(nome_uf_upper)

def log(msg: str, level: str = "info"):
    """Log formatado."""
    icons = {"info": "INFO", "success": "OK", "error": "ERR", "warn": "WARN"}
    print(f"[{icons.get(level, 'INFO')}] {msg}", file=sys.stderr)

def baixar_coordenadas_municipios() -> Dict[str, Dict[str, Any]]:
    """Baixa dataset de coordenadas dos municípios brasileiros."""
    log("Baixando dataset de coordenadas...")
    url = "https://raw.githubusercontent.com/kelvins/municipios-brasileiros/main/json/municipios.json"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        coords_lookup = {}
        for m in data:
            uf = UF_SIGLAS.get(m['codigo_uf'], '')
            if not uf:
                continue

            chave = f"{normalizar_nome(m['nome'])}-{uf.lower()}"
            coords_lookup[chave] = {
                'lat': m['latitude'],
                'lon': m['longitude'],
                'cod_ibge': str(m['codigo_ibge'])
            }

        log(f"Coordenadas baixadas: {len(coords_lookup)} municípios")
        return coords_lookup

    except Exception as e:
        log(f"Erro ao baixar coordenadas: {e}", "error")
        return {}

def processar_csv(csv_path: str, cultura: str, coords_lookup: Dict[str, Dict[str, Any]]) -> tuple[Dict, Dict, Dict]:
    """
    Processa CSV de uma cultura específica.

    Retorna: (area_data, producao_data, area_total_data)
    """
    log(f"Processando CSV {cultura}: {csv_path}")

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        log(f"Erro ao ler CSV {csv_path}: {e}", "error")
        return {}, {}, {}

    area_data = {}
    producao_data = {}
    area_total_data = {}

    for _, row in df.iterrows():
        year = str(int(row['year']))
        municipio = str(row['region_name'])
        estado = str(row['state_name'])

        # Mapear UF
        uf = mapear_uf(estado)
        if not uf:
            log(f"UF não reconhecida: {estado}", "warn")
            continue

        # Criar chave do município
        chave = criar_chave_municipio(municipio, uf)

        # Extrair valores
        area_plantada = float(row['area_planted_1000ha']) if pd.notna(row['area_planted_1000ha']) else 0.0
        producao = float(row['production_1000t']) if pd.notna(row['production_1000t']) else 0.0
        area_total_ha = float(row['total_muni_area_ha']) if pd.notna(row['total_muni_area_ha']) else 0.0

        # Pular registros sem produção significativa
        if area_plantada <= 0 and producao <= 0:
            continue

        # Inicializar estruturas se necessário
        if cultura not in area_data:
            area_data[cultura] = {}
        if year not in area_data[cultura]:
            area_data[cultura][year] = {}

        if cultura not in producao_data:
            producao_data[cultura] = {}
        if year not in producao_data[cultura]:
            producao_data[cultura][year] = {}

        # Armazenar dados
        area_data[cultura][year][chave] = round(area_plantada, 2)
        producao_data[cultura][year][chave] = round(producao, 1)

        # Área total (usar sempre a mais recente disponível)
        if area_total_ha > 0:
            area_total_mil_ha = round(area_total_ha / 1000, 2)  # converter para mil hectares
            area_total_data[chave] = area_total_mil_ha

    log(f"{cultura}: {len(area_total_data)} municípios únicos")

    return area_data, producao_data, area_total_data

def adicionar_coordenadas(municipios_data: Dict[str, Dict], coords_lookup: Dict[str, Dict[str, Any]]) -> tuple[int, int]:
    """Adiciona coordenadas aos dados dos municípios."""
    matched = 0
    missing = 0

    for chave, info in municipios_data.items():
        if chave in coords_lookup:
            coord = coords_lookup[chave]
            municipios_data[chave]['lat'] = coord['lat']
            municipios_data[chave]['lon'] = coord['lon']
            if 'cod_ibge' not in info or not info['cod_ibge']:
                municipios_data[chave]['cod_ibge'] = coord['cod_ibge']
            matched += 1
        else:
            missing += 1

    return matched, missing

def validar_dados(area_data: Dict, producao_data: Dict, area_total_data: Dict) -> List[str]:
    """Valida consistência dos dados."""
    warnings = []

    for cultura, anos_data in area_data.items():
        for ano, mun_data in anos_data.items():
            for mun, area_plantada in mun_data.items():
                area_total = area_total_data.get(mun, 0)

                if area_total > 0 and area_plantada > area_total:
                    warnings.append(
                        f"ERRO: {mun} {cultura} {ano}: área plantada ({area_plantada:.1f} mil ha) > "
                        f"área total ({area_total:.1f} mil ha)"
                    )

    return warnings

@click.command()
@click.option('--input-soja', type=click.Path(exists=True),
              default='/Users/francy/ai_agent_tools/brazil_agro/ibge_sidra_scripts/dataset_soja_2000_2025.csv',
              help='Arquivo CSV da soja')
@click.option('--input-milho', type=click.Path(exists=True),
              default='/Users/francy/ai_agent_tools/brazil_agro/ibge_sidra_scripts/dataset_milho_2000_2024.csv',
              help='Arquivo CSV do milho')
@click.option('--output', '-o', type=click.Path(),
              help='Arquivo de saída JSON')
@click.option('--json', 'output_json', is_flag=True,
              help='Output como JSON para stdout')
@click.option('--skip-coords', is_flag=True,
              help='Pular busca de coordenadas (mais rápido)')
def main(input_soja: str, input_milho: str, output: Optional[str], output_json: bool, skip_coords: bool):
    """
    Converte CSVs históricos de soja e milho para JSON compatível com crop-weighted-climate.html
    """

    # Metadata
    resultado = {
        "metadata": {
            "fonte": "CSV Histórico IBGE/SIDRA",
            "extraido_em": datetime.now().isoformat(),
            "periodo": "2000-2024",
            "culturas": ["soja", "milho"],
            "arquivos_entrada": [input_soja, input_milho]
        },
        "municipios": {},
        "areaTotal": {},
        "area": {},
        "producao": {}
    }

    # Baixar coordenadas (se não pular)
    coords_lookup = {}
    if not skip_coords:
        coords_lookup = baixar_coordenadas_municipios()

    # Processar soja
    area_soja, producao_soja, area_total_soja = processar_csv(input_soja, "soja", coords_lookup)
    resultado["area"].update(area_soja)
    resultado["producao"].update(producao_soja)
    resultado["areaTotal"].update(area_total_soja)

    # Processar milho
    area_milho, producao_milho, area_total_milho = processar_csv(input_milho, "milho", coords_lookup)
    resultado["area"].update(area_milho)
    resultado["producao"].update(producao_milho)
    resultado["areaTotal"].update(area_total_milho)

    # Coletar lista de municípios únicos
    municipios_unicos = set()
    for cultura_data in resultado["area"].values():
        for ano_data in cultura_data.values():
            municipios_unicos.update(ano_data.keys())

    log(f"Total municípios únicos: {len(municipios_unicos)}")

    # Criar estrutura de municípios
    for mun in municipios_unicos:
        resultado["municipios"][mun] = {}

    # Adicionar coordenadas (se baixadas)
    if coords_lookup:
        matched, missing = adicionar_coordenadas(resultado["municipios"], coords_lookup)
        log(f"Coordenadas: {matched} encontradas, {missing} faltantes")

    # Validação
    warnings = validar_dados(resultado["area"], resultado["producao"], resultado["areaTotal"])
    if warnings:
        for w in warnings:
            log(w, "warn")
        resultado["metadata"]["warnings"] = warnings
        log(f"Total warnings: {len(warnings)}")
    else:
        log("Todos os dados validados com sucesso!", "success")

    # Estatísticas finais
    total_mun = len(resultado["municipios"])
    total_area = sum(len(a) for c in resultado["area"].values() for a in c.values())
    total_producao = sum(len(p) for c in resultado["producao"].values() for p in c.values())
    anos_soja = list(resultado["area"]["soja"].keys()) if "soja" in resultado["area"] else []
    anos_milho = list(resultado["area"]["milho"].keys()) if "milho" in resultado["area"] else []

    log(f"Estatísticas finais:")
    log(f"  - {total_mun} municípios")
    log(f"  - {total_area} registros de área")
    log(f"  - {total_producao} registros de produção")
    log(f"  - Soja: {len(anos_soja)} anos ({min(anos_soja)}-{max(anos_soja) if anos_soja else 'N/A'})")
    log(f"  - Milho: {len(anos_milho)} anos ({min(anos_milho)}-{max(anos_milho) if anos_milho else 'N/A'})")

    # Output
    resultado_json = json.dumps(resultado, indent=2, ensure_ascii=False)

    if output:
        with open(output, 'w', encoding='utf-8') as f:
            f.write(resultado_json)
        log(f"Dados salvos em: {output}", "success")

    if output_json or not output:
        print(resultado_json)

if __name__ == '__main__':
    main()
