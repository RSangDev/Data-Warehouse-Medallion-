"""
ingestion/ingest.py
Extrai dados públicos do IBGE e DATASUS e salva como CSV em data/raw/.
Em produção: substituir requests reais por chamadas à API do IBGE
(https://servicodados.ibge.gov.br/api/v1/localidades/municipios)
e FTP do DATASUS.
"""

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

# ─── referência IBGE ────────────────────────────────────────────
REGIOES = {
    "Norte":       ["AM","PA","AC","RO","RR","AP","TO"],
    "Nordeste":    ["MA","PI","CE","RN","PB","PE","AL","SE","BA"],
    "Sudeste":     ["MG","ES","RJ","SP"],
    "Sul":         ["PR","SC","RS"],
    "Centro-Oeste":["MT","MS","GO","DF"],
}

UF_CODES = {
    "AM":{"nome":"Amazonas",     "pib_pc":21345,"idh":0.674,"pop":4269995},
    "PA":{"nome":"Pará",         "pib_pc":15234,"idh":0.646,"pop":8777124},
    "AC":{"nome":"Acre",         "pib_pc":16890,"idh":0.663,"pop":906876},
    "RO":{"nome":"Rondônia",     "pib_pc":22100,"idh":0.690,"pop":1815278},
    "RR":{"nome":"Roraima",      "pib_pc":19800,"idh":0.707,"pop":652713},
    "AP":{"nome":"Amapá",        "pib_pc":17200,"idh":0.708,"pop":877613},
    "TO":{"nome":"Tocantins",    "pib_pc":20100,"idh":0.699,"pop":1607363},
    "MA":{"nome":"Maranhão",     "pib_pc":12800,"idh":0.639,"pop":7153262},
    "PI":{"nome":"Piauí",        "pib_pc":13200,"idh":0.646,"pop":3289290},
    "CE":{"nome":"Ceará",        "pib_pc":16891,"idh":0.682,"pop":9187103},
    "RN":{"nome":"Rio Grande do Norte","pib_pc":18900,"idh":0.684,"pop":3534165},
    "PB":{"nome":"Paraíba",      "pib_pc":16700,"idh":0.658,"pop":4059905},
    "PE":{"nome":"Pernambuco",   "pib_pc":19234,"idh":0.673,"pop":9674793},
    "AL":{"nome":"Alagoas",      "pib_pc":13900,"idh":0.631,"pop":3365351},
    "SE":{"nome":"Sergipe",      "pib_pc":18100,"idh":0.665,"pop":2338474},
    "BA":{"nome":"Bahia",        "pib_pc":18234,"idh":0.660,"pop":14985284},
    "MG":{"nome":"Minas Gerais", "pib_pc":32912,"idh":0.731,"pop":21411923},
    "ES":{"nome":"Espírito Santo","pib_pc":34500,"idh":0.740,"pop":4108508},
    "RJ":{"nome":"Rio de Janeiro","pib_pc":43798,"idh":0.761,"pop":17463349},
    "SP":{"nome":"São Paulo",    "pib_pc":58293,"idh":0.783,"pop":46649132},
    "PR":{"nome":"Paraná",       "pib_pc":42345,"idh":0.749,"pop":11597484},
    "SC":{"nome":"Santa Catarina","pib_pc":48329,"idh":0.774,"pop":7762154},
    "RS":{"nome":"Rio Grande do Sul","pib_pc":45718,"idh":0.746,"pop":11466630},
    "MT":{"nome":"Mato Grosso",  "pib_pc":56782,"idh":0.725,"pop":3784239},
    "MS":{"nome":"Mato Grosso do Sul","pib_pc":43210,"idh":0.729,"pop":2839188},
    "GO":{"nome":"Goiás",        "pib_pc":35789,"idh":0.735,"pop":7206589},
    "DF":{"nome":"Distrito Federal","pib_pc":85234,"idh":0.824,"pop":3094325},
}

MUNICIPIOS_SAMPLE = {
    "SP": [("São Paulo",12325000),("Guarulhos",1392121),("Campinas",1213792),("São Bernardo do Campo",844483)],
    "RJ": [("Rio de Janeiro",6748000),("São Gonçalo",1091737),("Duque de Caxias",924624),("Nova Iguaçu",822338)],
    "MG": [("Belo Horizonte",2521564),("Uberlândia",699097),("Contagem",668240),("Juiz de Fora",573285)],
    "BA": [("Salvador",2900319),("Feira de Santana",629978),("Vitória da Conquista",341597)],
    "PR": [("Curitiba",1963726),("Londrina",575377),("Maringá",436472)],
    "RS": [("Porto Alegre",1488252),("Caxias do Sul",474601),("Pelotas",342053)],
    "PE": [("Recife",1653461),("Caruaru",365597),("Petrolina",350916)],
    "CE": [("Fortaleza",2686612),("Caucaia",355954),("Juazeiro do Norte",279267)],
    "AM": [("Manaus",2255903),("Parintins",115777)],
    "GO": [("Goiânia",1555626),("Aparecida de Goiânia",590544)],
    "DF": [("Brasília",3094325)],
    "SC": [("Florianópolis",537213),("Joinville",616287),("Blumenau",364409)],
    "ES": [("Vitória",365855),("Serra",527240),("Vila Velha",501325)],
    "MT": [("Cuiabá",650916),("Várzea Grande",283298)],
    "MS": [("Campo Grande",916001),("Dourados",229910)],
    "PA": [("Belém",1506420),("Ananindeua",535547)],
    "MA": [("São Luís",1108975),("Imperatriz",264963)],
    "PI": [("Teresina",868075),("Parnaíba",153080)],
    "RN": [("Natal",890480),("Mossoró",301390)],
    "PB": [("João Pessoa",817511),("Campina Grande",422006)],
    "AL": [("Maceió",1025360),("Arapiraca",231262)],
    "SE": [("Aracaju",664908),("Nossa Senhora do Socorro",181544)],
    "RO": [("Porto Velho",548952),("Ji-Paraná",134051)],
    "AC": [("Rio Branco",407319),("Cruzeiro do Sul",90283)],
    "RR": [("Boa Vista",399213)],
    "AP": [("Macapá",512902)],
    "TO": [("Palmas",313349),("Araguaína",183381)],
}

CIDS_INTERNACAO = {
    "J18":{"desc":"Pneumonia",              "capitulo":"X",    "dias_base":7,  "gravidade":0.50},
    "I21":{"desc":"Infarto do Miocárdio",   "capitulo":"IX",   "dias_base":10, "gravidade":0.72},
    "I64":{"desc":"AVC",                    "capitulo":"IX",   "dias_base":14, "gravidade":0.68},
    "C34":{"desc":"Neoplasia Pulmão",       "capitulo":"II",   "dias_base":18, "gravidade":0.80},
    "K35":{"desc":"Apendicite",             "capitulo":"XI",   "dias_base":3,  "gravidade":0.35},
    "S72":{"desc":"Fratura de Fêmur",       "capitulo":"XIX",  "dias_base":12, "gravidade":0.55},
    "E11":{"desc":"Diabetes Tipo 2",        "capitulo":"IV",   "dias_base":6,  "gravidade":0.40},
    "N18":{"desc":"Doença Renal Crônica",   "capitulo":"XIV",  "dias_base":9,  "gravidade":0.60},
    "J44":{"desc":"DPOC",                   "capitulo":"X",    "dias_base":8,  "gravidade":0.55},
    "F20":{"desc":"Esquizofrenia",          "capitulo":"V",    "dias_base":25, "gravidade":0.30},
    "O80":{"desc":"Parto Normal",           "capitulo":"XV",   "dias_base":2,  "gravidade":0.15},
    "A41":{"desc":"Septicemia",             "capitulo":"I",    "dias_base":15, "gravidade":0.85},
    "I50":{"desc":"Insuf. Cardíaca",        "capitulo":"IX",   "dias_base":9,  "gravidade":0.65},
    "G40":{"desc":"Epilepsia",              "capitulo":"VI",   "dias_base":5,  "gravidade":0.45},
    "P07":{"desc":"Prematuridade",          "capitulo":"XVI",  "dias_base":21, "gravidade":0.70},
}


def ingest_ibge_estados(seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info("Ingerindo dados IBGE — estados...")
    uf_regiao = {uf: reg for reg, ufs in REGIOES.items() for uf in ufs}
    rows = []
    for uf, info in UF_CODES.items():
        # Gera séries históricas por ano (2019-2023)
        for ano in range(2019, 2024):
            fator_ano = 1 + (ano - 2019) * np.random.uniform(0.01, 0.04)
            rows.append({
                "sigla_uf":    uf,
                "nome_uf":     info["nome"],
                "regiao":      uf_regiao.get(uf, "Desconhecida"),
                "populacao":   int(info["pop"] * fator_ano),
                "pib_per_capita": round(info["pib_pc"] * fator_ano, 2),
                "idh":         round(min(1.0, info["idh"] + (ano - 2019) * 0.002), 3),
                "ano":         ano,
                "fonte":       "IBGE/SIDRA (simulado)",
                "data_carga":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
    df = pd.DataFrame(rows)
    logger.info(f"  → {len(df)} registros de estados")
    return df


def ingest_ibge_municipios(seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info("Ingerindo dados IBGE — municípios...")
    uf_regiao = {uf: reg for reg, ufs in REGIOES.items() for uf in ufs}
    rows = []
    id_counter = 1
    for uf, municipios in MUNICIPIOS_SAMPLE.items():
        uf_info = UF_CODES[uf]
        for nome_mun, pop_base in municipios:
            for ano in range(2019, 2024):
                pop = int(pop_base * (1 + (ano - 2019) * np.random.uniform(0.005, 0.025)))
                pib_var = uf_info["pib_pc"] * np.random.uniform(0.60, 1.50)
                rows.append({
                    "id_municipio":  id_counter * 100 + ano,
                    "cod_ibge":      str(hash(nome_mun + uf))[:7],
                    "nome_municipio": nome_mun,
                    "sigla_uf":      uf,
                    "regiao":        uf_regiao.get(uf, ""),
                    "populacao":     pop,
                    "pib_per_capita": round(pib_var, 2),
                    "idh_municipal": round(min(1.0, uf_info["idh"] + np.random.uniform(-0.05, 0.08)), 3),
                    "area_km2":      round(np.random.uniform(200, 8000), 1),
                    "ano":           ano,
                    "fonte":         "IBGE/SIDRA (simulado)",
                    "data_carga":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                id_counter += 1
    df = pd.DataFrame(rows)
    logger.info(f"  → {len(df)} registros de municípios")
    return df


def ingest_datasus_internacoes(n: int = 3000, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    logger.info(f"Ingerindo DATASUS — {n} internações...")
    ufs = list(UF_CODES.keys())
    cids = list(CIDS_INTERNACAO.keys())
    tipos_hosp = ["Hospital Geral","Hospital Especializado","UPA","Hospital Universitário","Santa Casa"]
    naturezas  = ["Público Federal","Público Estadual","Público Municipal","Privado Conveniado","Privado Puro"]
    caraters   = ["Eletivo","Urgência","Acidente","Outros"]

    rows = []
    data_base = datetime(2019, 1, 1)
    for i in range(n):
        cid  = np.random.choice(cids)
        info = CIDS_INTERNACAO[cid]
        uf   = np.random.choice(ufs)
        uf_i = UF_CODES[uf]

        idade  = int(np.clip(np.random.exponential(45), 0, 100))
        sexo   = "F" if "O80" in cid else np.random.choice(["M","F"])
        if info["capitulo"] == "XVI": idade = 0

        faixa = _faixa(idade)
        dias  = max(1, int(np.random.gamma(2, info["dias_base"] * (1 + max(0,(idade-50)/100)) / 2)))
        custo = round(dias * np.random.uniform(350, 900) * (1 + info["gravidade"] * 0.6), 2)
        obito = 1 if np.random.random() < info["gravidade"] * 0.10 * (1 + max(0,(idade-60)/100)) else 0

        dt_inter = data_base + timedelta(days=np.random.randint(0, 365*5))
        rows.append({
            "id_aih":               f"AIH{2019000000+i:010d}",
            "sigla_uf":             uf,
            "tipo_hospital":        np.random.choice(tipos_hosp, p=[0.4,0.2,0.15,0.15,0.1]),
            "natureza_juridica":    np.random.choice(naturezas,  p=[0.1,0.28,0.22,0.25,0.15]),
            "carater_internacao":   np.random.choice(caraters,   p=[0.35,0.50,0.10,0.05]),
            "cid_principal":        cid,
            "descricao_cid":        info["desc"],
            "capitulo_cid":         info["capitulo"],
            "idade":                idade,
            "faixa_etaria":         faixa,
            "sexo":                 sexo,
            "dias_internacao":      dias,
            "valor_aih":            custo,
            "obito":                obito,
            "ano":                  dt_inter.year,
            "mes":                  dt_inter.month,
            "data_internacao":      dt_inter.strftime("%Y-%m-%d"),
            "fonte":                "DATASUS/SIH (simulado)",
            "data_carga":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.DataFrame(rows)
    logger.info(f"  → {len(df)} internações")
    return df


def _faixa(idade: int) -> str:
    if idade < 1:  return "menor_1"
    if idade < 5:  return "1_4"
    if idade < 15: return "5_14"
    if idade < 30: return "15_29"
    if idade < 45: return "30_44"
    if idade < 60: return "45_59"
    if idade < 75: return "60_74"
    return "75_mais"


def run_ingestion(n_internacoes: int = 3000) -> dict:
    os.makedirs(RAW_DIR, exist_ok=True)

    estados = ingest_ibge_estados()
    municipios = ingest_ibge_municipios()
    internacoes = ingest_datasus_internacoes(n=n_internacoes)

    estados.to_csv(os.path.join(RAW_DIR, "ibge_estados.csv"), index=False)
    municipios.to_csv(os.path.join(RAW_DIR, "ibge_municipios.csv"), index=False)
    internacoes.to_csv(os.path.join(RAW_DIR, "datasus_internacoes.csv"), index=False)

    logger.info("Arquivos salvos em data/raw/")
    return {"estados": estados, "municipios": municipios, "internacoes": internacoes}


if __name__ == "__main__":
    run_ingestion()
