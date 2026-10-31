"""
Ingestão: arquivo do detector -> registros RAW.

Implementa o `load_data(spec, file_full_path)` do §5.1, com os cinco passos que o
§5.3 exige:

    1. ler o arquivo bruto          -> hats_binary_reader
    2. interpretar o formato        -> hats_binary_reader
    3. converter em registros       -> aqui
    4. garantir consistência temporal -> aqui
    5. retornar no formato padrão   -> TimeSeriesLoadDataResult

Duas fontes, como o SST faz com FAST e INTG: o `.rbd` com o sinal do detector a
1 kHz e o `.aux` com o apontamento a ~1,05 s. Cada uma é um modo de aquisição e
vira uma fonte RAW própria no catálogo; o `.ws` ainda não entra.

O apontamento traz as doze variáveis que o Guigue listou: husec, jd, sid,
elevation, azimuth, right_ascension, declination, ra_rate, dec_rate, object,
opmode e o carimbo — que no `HATS.py` se chama `time` e aqui é o `UTC_TIME` que o
§6 exige.

Procedência
-----------
Cada registro carrega `FULL_FILE_PATH`, por orientação do suporte: identifica
diretamente o arquivo de origem. Os outros dois campos que o POEMAS grava ficam de
fora, também por orientação dele — `FILEPATH` atende compatibilidade legada do
POEMAS, e `TELESCOPE` é redundante porque o pipeline acrescenta a identificação do
instrumento em `META.INSTRUMENT`.

É a mesma string em todos os registros do arquivo, então custa um ponteiro e não
uma cópia do caminho.


O passo 4 é o "Ponto crítico" do §6
-----------------------------------
No POEMAS o problema era outro: o arquivo só traz segundo inteiro, e o plugin
precisou *inventar* o sub-segundo somando 10 ms por registro. O HATS carimba cada
amostra em husec — décimos de milissegundo (100 µs) desde 0 UT — então aqui não há nada
a inventar. O trabalho é **verificar**, e a verificação distingue dois casos:

  - husec repetido ou em ordem decrescente **falha**. Uma série temporal com
    carimbo duplicado ou fora de ordem não é utilizável, e o banco não tem como
    reparar depois. Medido em 7,2 milhões de registros reais: nunca acontece.

  - lacuna — passo diferente de 10 husec — **avisa**. Acontece: numa hora medida
    há um salto de 1007 husec no meio do arquivo, cerca de 100 amostras que o
    sistema de aquisição perdeu. É informação, não motivo para recusar o dia.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from external_contracts.instrument_ingestion_runtime_spec import InstrumentIngestionRuntimeSpec
from external_contracts.time_series_ingestion_adapter import TimeSeriesIngestionAdapter
from external_contracts.time_series_load_data_result import TimeSeriesLoadDataResult

from hats import constants

import hats_binary_reader as reader

UTC = timezone.utc

# O guia §6 exige um campo com este nome exato, contendo a data completa.
TIME_FIELD = "UTC_TIME"

# Um modo por tipo de arquivo, como o SST declara FAST e INTG. O POEMAS usa um
# modo único chamado DEFAULT porque tem uma fonte só.
MODE_DETECTOR = "DETECTOR"
MODE_POINTING = "POINTING"

MODE_BY_SUFFIX = {".rbd": MODE_DETECTOR, ".aux": MODE_POINTING}

# Resolução nativa de cada modo, quando o spec não a traz. A do apontamento é
# medida: 1,0507 s em média, alternando entre 10480 e 10520 husec.
FALLBACK_RESOLUTION = {MODE_DETECTOR: "1ms", MODE_POINTING: "1050ms"}

# Passo esperado entre amostras consecutivas do detector: 1 kHz = 1 ms = 10 husec.
# O apontamento não tem passo fixo — é um laço de software com jitter de ±2 ms —,
# então para ele não há lacuna a reportar, só monotonicidade a exigir.
EXPECTED_HUSEC_STEP = {MODE_DETECTOR: constants.HUSEC_PER_SAMPLE, MODE_POINTING: None}


class HatsIngestion(TimeSeriesIngestionAdapter):
    """
    Responsabilidade única: converter um binário do HATS em registros RAW.

    `xml_dir` é repassado ao reader; sem ele vale HATSXMLPATH, e sem ela o reader
    falha em vez de adivinhar o layout.

    `record_limit` existe para teste e inspeção. Em produção fica None: recortar um
    arquivo pela metade produziria um dia incompleto sem nada no caminho acusando.
    """

    def __init__(self, xml_dir: Optional[str] = None, record_limit: Optional[int] = None) -> None:
        self._xml_dir = xml_dir
        self._record_limit = record_limit

    # ---------------------------------------------------------------- contrato

    def load_data(
        self,
        spec: InstrumentIngestionRuntimeSpec,
        file_full_path: str,
    ) -> TimeSeriesLoadDataResult:
        path = Path(file_full_path)

        mode = MODE_BY_SUFFIX.get(path.suffix.lower())
        if mode is None:
            raise ValueError(
                "esta ingestao le .rbd (detector) e .aux (apontamento). O {} nao tem "
                "fonte RAW declarada".format(path.suffix or path.name))

        # Passos 1 e 2 do §5.3: ler e interpretar o formato.
        cabecalho, schema, blocos = reader.read_and_decode_hats_file(
            str(path), xml_dir=self._xml_dir, record_limit=self._record_limit)

        if not cabecalho["DATE"]:
            raise ValueError(
                "nome fora da convencao do CRAAM em {}: esperado "
                "hats-YYYY-MM-DDTHHMM.rbd".format(path.name))

        # Passos 3 e 4: converter e verificar.
        registros, consistencia = self._build_records(cabecalho, blocos, mode)

        if not registros:
            descartados = consistencia["records_dropped_before_hour"]
            if descartados:
                # Acontece de verdade: no arquivo das 11:00 de 2026-03-17 o husec
                # comeca em 10, ou seja o relogio do instrumento nao estava ajustado,
                # e 3082 dos 4000 primeiros registros ficam antes da hora nominal.
                # Com record_limit cortando dentro desse prefixo, sobra zero.
                raise ValueError(
                    "HATS load_data gerou 0 registros para {}: todos os {} registros lidos "
                    "estao antes da hora nominal do arquivo e foram descartados{}".format(
                        path.name, descartados,
                        "; o record_limit de {} pode estar cortando dentro do prefixo "
                        "defasado".format(self._record_limit) if self._record_limit else ""))
            raise ValueError("HATS load_data gerou 0 registros para {}".format(path.name))

        # Passo 5: retornar no formato padrão.
        return TimeSeriesLoadDataResult(
            instrument_name=getattr(spec, "instrument_name", None) or "HATS",
            acquisition_mode=mode,
            data_time_resolution=self._resolution(spec, mode),
            data_level="raw",
            observation_day=self._observation_day(cabecalho["DATE"]),
            data_records=registros,
            instrument_metadata=self._metadata(cabecalho, schema, registros, consistencia, mode),
            source_file_path=str(path),
            processing_warnings=self._warnings(consistencia),
        )

    # ------------------------------------------------------------- utilidades

    def _resolution(self, spec: InstrumentIngestionRuntimeSpec, mode: str) -> str:
        """Resolução nativa, vinda do spec; o padrão descreve o instrumento."""
        declarado = getattr(spec, "native_resolutions_by_mode", None) or {}
        return declarado.get(mode) or FALLBACK_RESOLUTION[mode]

    @staticmethod
    def _observation_day(date_text: str) -> datetime:
        """
        Meia-noite UTC do dia observado.

        O POEMAS passa o menor UTC_TIME do arquivo, que é um instante e não um dia.
        O campo se chama observation_day, então aqui vai o dia.
        """
        return datetime(int(date_text[0:4]), int(date_text[5:7]), int(date_text[8:10]),
                        tzinfo=UTC)

    @staticmethod
    def _hour_threshold(hour: Optional[str]) -> Optional[int]:
        """
        Primeiro husec que pertence à hora nominal do arquivo.

        O HATS.py descarta o que está abaixo disso. Aqui o motivo é mais forte do
        que seguir a referência: ingeridos, esses registros fariam o mesmo instante
        aparecer em dois arquivos de horas consecutivas, e o banco ganharia carimbo
        duplicado — exatamente o que o passo 4 existe para impedir.
        """
        if hour and hour[:2].isdigit():
            return int(hour[:2]) * constants.HUSEC_PER_HOUR
        return None

    # ------------------------------------------------- passos 3 e 4 do §5.3

    def _build_records(self, cabecalho: Dict[str, Any], blocos,
                       mode: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Converte blocos de colunas em registros, verificando o tempo no caminho.

        A verificação é feita aqui, e não numa passada separada, porque uma passada
        a mais sobre 3,6 milhões de registros por hora custaria mais do que a
        conversão inteira.
        """
        date_text = cabecalho["DATE"]
        day_base = datetime(int(date_text[0:4]), int(date_text[5:7]), int(date_text[8:10]),
                            tzinfo=UTC)
        threshold = self._hour_threshold(cabecalho["HOUR"])
        passo_esperado = EXPECTED_HUSEC_STEP[mode]

        # Uma string, referenciada por todos os registros do arquivo.
        caminho_de_origem = cabecalho["FULL_FILE_PATH"]

        registros: List[Dict[str, Any]] = []
        descartados = 0
        sequencia = 0
        anterior: Optional[int] = None
        lacunas: List[Tuple[int, int]] = []

        for bloco, quantidade in blocos:
            husecs = bloco["husec"]
            # Os campos científicos são todos menos o carimbo, que vira UTC_TIME.
            cientificos = [(nome.upper(), coluna) for nome, coluna in bloco.items()
                           if nome != "husec"]

            for posicao in range(quantidade):
                husec = husecs[posicao]

                if threshold is not None and husec < threshold:
                    descartados += 1
                    continue

                # Passo 4: consistência temporal.
                if anterior is not None:
                    passo = husec - anterior
                    if passo <= 0:
                        raise ValueError(
                            "carimbo temporal inconsistente em {}: husec {} vem depois de "
                            "{} (passo {}). Registro {} repetido ou fora de ordem nao pode "
                            "ser ingerido — a serie ficaria irrecuperavel no banco".format(
                                cabecalho["FILENAME"], husec, anterior, passo,
                                "duplicado" if passo == 0 else "decrescente"))
                    # O apontamento não tem passo fixo, então lacuna não se
                    # aplica a ele: só monotonicidade, exigida acima.
                    if passo_esperado is not None and passo != passo_esperado:
                        lacunas.append((anterior, husec))
                anterior = husec

                registro = {
                    # O suporte confirmou: mantido como datetime. A persistencia
                    # guarda com precisao de milissegundo, e por isso o HUSEC abaixo
                    # vai como inteiro — e ele que preserva os 0,1 ms do contador.
                    TIME_FIELD: day_base + timedelta(microseconds=int(husec) * 100),
                    "HUSEC": int(husec),
                    "PROC_SEQ": sequencia,
                    "FULL_FILE_PATH": caminho_de_origem,
                }
                for chave, coluna in cientificos:
                    registro[chave] = coluna[posicao]
                registros.append(registro)
                sequencia += 1

        # jd == 0 marca o registro de apontamento defasado: o husec está certo, mas
        # a solução é de cerca de 1053 s antes. De 1,8% a 95% dos registros por hora,
        # nas 32 horas medidas, em rajadas de minutos inteiros — e numa hora, todos.
        # Quem for interpolar o apontamento precisa saber disso.
        defasados = sum(1 for r in registros if r.get("JD") == 0.0) if mode == MODE_POINTING else 0

        consistencia = {
            "records_dropped_before_hour": descartados,
            "gaps": lacunas,
            "stale_pointing": defasados,
            "expected_step": passo_esperado,
            "detector_channel": cabecalho.get("DETECTOR_CHANNEL"),
            "husec_first": registros[0]["HUSEC"] if registros else None,
            "husec_last": registros[-1]["HUSEC"] if registros else None,
        }
        return registros, consistencia

    # ------------------------------------------------------ metadados e avisos

    @staticmethod
    def _metadata(cabecalho: Dict[str, Any], schema: Dict[str, Any],
                  registros: List[Dict[str, Any]], consistencia: Dict[str, Any],
                  mode: str) -> Dict[str, Any]:
        passo = EXPECTED_HUSEC_STEP[mode]
        perdidas = (sum((fim - inicio) // passo - 1 for inicio, fim in consistencia["gaps"])
                    if passo else 0)
        extra = ({"stale_pointing_records": consistencia["stale_pointing"]}
                 if mode == MODE_POINTING else
                 {"detector_channel": consistencia["detector_channel"],
                  "acquisition": {
                      "sampling_frequency_hz": constants.SAMPLING_FREQUENCY,
                      "chopper_frequency_hz": constants.TARGET_FREQUENCY}})
        return dict(extra, **{
            "source": cabecalho["FULL_FILE_PATH"],
            "acquisition_mode": mode,
            "observation_date": cabecalho["DATE"],
            "observation_hour": cabecalho["HOUR"],
            "time_field": TIME_FIELD,
            "schema": dict(cabecalho["SCHEMA"], record_size=cabecalho["RECORD_SIZE"]),
            "records_in_file": cabecalho["NREC"],
            "records_kept": len(registros),
            "records_dropped_before_hour": consistencia["records_dropped_before_hour"],
            "husec_first": consistencia["husec_first"],
            "husec_last": consistencia["husec_last"],
            "time_gaps": len(consistencia["gaps"]),
            "samples_missing_in_gaps": perdidas,
        })

    @staticmethod
    def _warnings(consistencia: Dict[str, Any]) -> List[str]:
        avisos: List[str] = []

        defasados = consistencia.get("stale_pointing")
        if defasados:
            avisos.append(
                "{} registros de apontamento trazem jd == 0: o husec esta correto, mas a "
                "solucao de apontamento e de cerca de 1053 s antes. Interpolar sem "
                "filtra-los contamina o resultado".format(defasados))

        descartados = consistencia["records_dropped_before_hour"]
        if descartados:
            avisos.append(
                "{} registros anteriores a hora nominal do arquivo foram descartados, "
                "como o HATS.py faz, para o mesmo instante nao aparecer em duas "
                "horas".format(descartados))

        lacunas = consistencia["gaps"]
        if lacunas:
            passo = consistencia["expected_step"]
            perdidas = sum((fim - inicio) // passo - 1 for inicio, fim in lacunas)
            primeira = lacunas[0]
            avisos.append(
                "{} lacuna(s) na serie temporal, somando cerca de {} amostras que o "
                "sistema de aquisicao nao registrou; a primeira entre os husec {} e "
                "{}".format(len(lacunas), perdidas, primeira[0], primeira[1]))

        return avisos
