"""
Exportação: registros RAW -> arquivos FITS e CSV.

Os seis métodos obrigatórios do §7.2: dois RAW, que geram "arquivos sem alterar os
dados" e "mantêm resolução original", e quatro derivados, que agregam.

Média, e não mediana
--------------------
O §7.2 diz "Calcula mediana" nos dois produtos derivados, e o plugin do POEMAS
implementa com `np.median`. Perguntado, o suporte do MackSun **confirmou por
escrito que é média aritmética**. A palavra de quem mantém a especificação vale
sobre o texto dela, então é média — e fica registrado aqui porque quem ler só o
PDF vai achar que este código está errado.

O tamanho da janela vem da resolução alvo
-----------------------------------------
O §7.2 diz 100 ms = 10 registros e 1 s = 100. Esses números são do POEMAS, que
amostra a 10 ms. O suporte confirmou que o alvo é a **resolução**: para o HATS, a
1 kHz, são 100 e 1000 amostras. Por isso as janelas são calculadas a partir da
frequência de amostragem, e não escritas à mão.

O §11 delimita o que NÃO é nosso: renomear, mover para o NFS, gerar URL e
registrar são responsabilidade exclusiva do pipeline. O plugin escreve num
diretório temporário, devolve o caminho, e acabou.

Uma tabela de colunas para os dois escritores
---------------------------------------------
FITS e CSV saem da mesma lista de colunas. No plugin do POEMAS a lista
aparece duas vezes, uma em cada escritor, e as duas precisam ser mantidas em
acordo à mão — é o tipo de duplicação que fica errada em silêncio.

O que é publicado, e o que não é
--------------------------------
O registro que a ingestão produz tem mais campos do que o arquivo publicado. Vão
para o arquivo o carimbo de tempo nas duas formas (ISO e husec) e os cinco canais
científicos. Ficam de fora `PROC_SEQ`, que é contador interno da ingestão, e
`SAMPLE`, `SEC` e `MS`, que são escrituração da aquisição e redundantes com o
husec. O POEMAS faz a mesma distinção: o registro dele tem trinta campos e o CSV
tem dez.
"""

from __future__ import annotations

import csv
import os
import statistics
from datetime import timedelta
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from hats import constants, demodulation

from external_contracts.time_series_specialist_generated_file import (
    TimeSeriesSpecialistGeneratedFile)

import hats_utils as utils


@dataclass(frozen=True)
class Coluna:
    nome: str
    chave: Optional[str]      # chave no registro; None para a coluna de tempo
    fits: str                 # código de formato do FITS
    descricao: str
    tipo: str = "float"       # float | int | time


COLUNA_DE_TEMPO = Coluna(
    "ISO_DATETIME", None, "A26",
    "Date and time in ISO 8601 format, UTC (YYYY-MM-DDTHH:MM:SS.ssssss). Six "
    "decimal places; records loaded from persistence carry millisecond precision",
    tipo="time")

COLUNAS_RAW: Sequence[Coluna] = (
    COLUNA_DE_TEMPO,
    Coluna("HUSEC", "HUSEC", "K",
           "Hundredths of a millisecond since 0 UT, as stamped by the instrument. "
           "Preserves the 0.1 ms resolution of the acquisition counter",
           tipo="int"),
    Coluna("GOLAY", "GOLAY", "D", "Golay cell detector signal (mV)"),
    Coluna("CHOPPER", "CHOPPER", "D", "Chopper reference signal (mV)"),
    Coluna("TEMP_HICS", "TEMP_HICS", "D", "HICS temperature (Celsius)"),
    Coluna("TEMP_ENV", "TEMP_ENV", "D", "Enclosure temperature (Celsius)"),
    Coluna("TEMP_GOLAY", "TEMP_GOLAY", "D", "Golay cell temperature (Celsius)"),
)

# Janelas derivadas da resolução alvo, como o suporte confirmou. A 1 kHz, 0,1 s
# são 100 amostras e 1 s são 1000.
# As três colunas da estrutura Deconv do HATS.py: time, husec e amplitude.
COLUNAS_DECONV: Sequence[Coluna] = (
    COLUNA_DE_TEMPO,
    Coluna("HUSEC", "HUSEC", "K",
           "Hundredths of a millisecond since 0 UT, at the centre of the analysis "
           "window", tipo="int"),
    Coluna("AMPLITUDE", "AMPLITUDE", "D",
           "Amplitude of the 20 Hz modulation of the detector signal (mV)"),
)

JANELA_100MS = int(constants.SAMPLING_FREQUENCY * 0.1)
JANELA_1S = int(constants.SAMPLING_FREQUENCY * 1.0)

# Campos agregados pela derivação. Os demais vêm do registro central da janela.
CANAIS_AGREGADOS = ("GOLAY", "CHOPPER", "TEMP_HICS", "TEMP_ENV", "TEMP_GOLAY")


@dataclass(frozen=True)
class Produto:
    chave: str
    resolucao: str
    nivel: str
    janela: Optional[int]      # None para RAW e para o demodulado, que não agregam


PRODUTO_RAW = Produto(utils.PRODUTO_RAW, utils.RESOLUCAO_RAW, "raw", None)
PRODUTO_DECONV = Produto(utils.PRODUTO_DECONV, utils.RESOLUCAO_DECONV, "derived", None)
PRODUTO_100MS = Produto(utils.PRODUTO_100MS, utils.RESOLUCAO_100MS, "derived", JANELA_100MS)
PRODUTO_1S = Produto(utils.PRODUTO_1S, utils.RESOLUCAO_1S, "derived", JANELA_1S)


class InsufficientRecords(ValueError):
    """
    A entrada é curta demais para o produto pedido.

    Distinta de um defeito: um arquivo com 441 amostras simplesmente não contém uma
    janela de 1 s, e nenhum produto de 1 s pode sair dele. Quem chama precisa poder
    separar "este produto não se aplica a esta entrada" de "algo quebrou", e com um
    ValueError genérico não conseguiria. Continua sendo ValueError para o pipeline,
    que não conhece a subclasse.
    """


# Aviso que viaja com o arquivo, não só com o código. Sem ele, quem baixar o
# produto bruto pode tomar a portadora por série de fluxo.
# Os números vêm de 299 s de observação real de 2026-03-17T1800: as médias em
# janelas de 100 ms têm média 0,015 mV e desvio 1,532 mV, e a amplitude demodulada
# no mesmo intervalo tem média 124,167 mV.
AVISO_DO_SINAL_PICADO = (
    "CAUTION: the HATS detector is a Golay cell behind a 20 Hz chopper, so GOLAY "
    "is a modulated carrier and not a flux series. Averaging it over a whole number "
    "of chopper cycles cancels the carrier: measured on real data, means over 100 ms "
    "fluctuate around zero with a standard deviation of 1.5 mV, while the amplitude "
    "of the modulation, which is the physical quantity, is about 124 mV. Recovering "
    "the amplitude requires demodulation.")


AVISO_DA_MEDIA = (
    "CAUTION: this product is the arithmetic mean of the raw detector signal over "
    "non-overlapping windows. Because the signal is a 20 Hz modulated carrier and "
    "each window spans a whole number of chopper cycles, the carrier cancels out and "
    "the mean does NOT carry the solar signal: measured on real data it fluctuates "
    "around zero with a standard deviation of 1.5 mV, while the amplitude of the "
    "modulation is about 124 mV. The timestamp of each record refers to the centre "
    "of its aggregation window.")


class HatsExport(object):
    """Responsabilidade única: gerar FITS e CSV a partir de registros RAW."""

    # ------------------------------------------------- contrato obrigatório

    def GenerateFitsFileRawFormat(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(raw_records, PRODUTO_RAW, "FITS", "fits")

    def GenerateCSVFileRawFormat(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(raw_records, PRODUTO_RAW, "CSV", "csv")

    def GenerateFitsFileDerivedFormat100ms(self, raw_records: List[Dict[str, Any]]):
        derivados = self._derive_raw_to_fixed_windows(raw_records, JANELA_100MS)
        return self._escreve(derivados, PRODUTO_100MS, "FITS", "fits")

    def GenerateCSVFileDerivedFormat100ms(self, raw_records: List[Dict[str, Any]]):
        derivados = self._derive_raw_to_fixed_windows(raw_records, JANELA_100MS)
        return self._escreve(derivados, PRODUTO_100MS, "CSV", "csv")

    def GenerateFitsFileDerivedFormat1s(self, raw_records: List[Dict[str, Any]]):
        derivados = self._derive_raw_to_fixed_windows(raw_records, JANELA_1S)
        return self._escreve(derivados, PRODUTO_1S, "FITS", "fits")

    def GenerateCSVFileDerivedFormat1s(self, raw_records: List[Dict[str, Any]]):
        derivados = self._derive_raw_to_fixed_windows(raw_records, JANELA_1S)
        return self._escreve(derivados, PRODUTO_1S, "CSV", "csv")

    # ------------------------------------------- produto científico do HATS

    def GenerateFitsFileDeconvFormat32ms(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(self._demodulate(raw_records), PRODUTO_DECONV, "FITS", "fits")

    def GenerateCSVFileDeconvFormat32ms(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(self._demodulate(raw_records), PRODUTO_DECONV, "CSV", "csv")

    @staticmethod
    def _demodulate(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        A amplitude da modulação de 20 Hz, que é o sinal científico do HATS.

        Equivale à estrutura Deconv que o `getFFT()` do HATS.py cria: Goertzel com
        janela flat-top de 128 amostras deslocando de 32 em 32, saída a 31,25 Hz, e
        o husec de cada janela é o do seu centro.

        A conta inteira é do pacote `hats`, que reproduz o `HATS_fft.c` bit a bit —
        confrontado com o binário que o CRAAM distribui. Aqui não há cálculo, só o
        empacotamento no formato de registro.
        """
        if len(raw_records) < constants.WINDOW_SIZE:
            raise InsufficientRecords(
                "a demodulacao precisa de ao menos {} amostras para fechar uma janela, "
                "e veio {}".format(constants.WINDOW_SIZE, len(raw_records)))

        husec = [int(r["HUSEC"]) for r in raw_records]

        # A janela deslizante pressupõe amostras contíguas e em ordem. A ingestão
        # garante isso, mas estes registros vêm do banco, e uma consulta que os
        # devolvesse fora de ordem produziria uma amplitude sem significado — em
        # silêncio, porque a conta não falha, só erra.
        for posicao in range(1, len(husec)):
            if husec[posicao] <= husec[posicao - 1]:
                raise ValueError(
                    "registros fora de ordem na posicao {}: husec {} vem depois de {}. "
                    "A demodulacao pressupoe amostragem contigua e crescente".format(
                        posicao, husec[posicao], husec[posicao - 1]))

        sinal = [float(r["GOLAY"]) for r in raw_records]
        husecs, amplitudes = demodulation.demodulate(
            sinal, husec,
            window_size=constants.WINDOW_SIZE, steps=constants.STEPS,
            target_frequency=constants.TARGET_FREQUENCY,
            sampling_frequency=constants.SAMPLING_FREQUENCY)

        # O carimbo é reconstruído do husec da janela, e não escolhido entre os
        # registros de entrada, para a coluna de tempo seguir coerente com a de husec.
        primeiro = utils.as_utc(raw_records[0][utils.TIME_FIELD])
        meia_noite = primeiro.replace(hour=0, minute=0, second=0, microsecond=0)
        return [{utils.TIME_FIELD: meia_noite + timedelta(microseconds=int(h) * 100),
                 "HUSEC": int(h), "AMPLITUDE": float(a)}
                for h, a in zip(husecs, amplitudes)]

    # ------------------------------------------------------------- derivação

    @staticmethod
    def _derive_raw_to_fixed_windows(raw_records: List[Dict[str, Any]],
                                     window_size: int) -> List[Dict[str, Any]]:
        """
        Média aritmética em janelas fixas, sem sobreposição.

        Mesmo nome do método central do POEMAS, que o §8 nomeia explicitamente.

        O carimbo do registro derivado é o do **centro** da janela, como no POEMAS,
        e a última janela incompleta é descartada: publicar uma média de menos
        amostras com o mesmo rótulo de resolução daria uma estatística diferente
        sem nada no arquivo indicando.
        """
        total = len(raw_records)
        if total < window_size:
            raise InsufficientRecords(
                "a derivacao precisa de ao menos {} registros para fechar uma janela, "
                "e veio {}".format(window_size, total))

        derivados: List[Dict[str, Any]] = []
        inicio = 0
        while inicio + window_size <= total:
            janela = raw_records[inicio:inicio + window_size]
            central = dict(janela[window_size // 2])
            for canal in CANAIS_AGREGADOS:
                valores = [registro[canal] for registro in janela if canal in registro]
                if valores:
                    central[canal] = statistics.fmean(valores)
            derivados.append(central)
            inicio += window_size

        return derivados

    # ------------------------------------------------------------ escritores

    @staticmethod
    def _colunas(produto: Produto) -> Sequence[Coluna]:
        return COLUNAS_DECONV if produto.chave == utils.PRODUTO_DECONV else COLUNAS_RAW

    def _escreve(self, registros: List[Dict[str, Any]], produto: Produto,
                 fmt: str, extensao: str):
        if not registros:
            raise ValueError("records vazio: nao ha o que exportar")

        diretorio = utils.get_specialist_tmp_dir()
        primeiro = utils.as_utc(registros[0][utils.TIME_FIELD])
        nome = utils.build_output_filename(primeiro, produto.chave, fmt, extensao)
        caminho = os.path.join(diretorio, nome)

        cabecalho = self._cabecalho(nome, registros, produto)
        if fmt == "FITS":
            self._escreve_fits(caminho, registros, cabecalho, produto)
        else:
            self._escreve_csv(caminho, registros, cabecalho, produto)

        return TimeSeriesSpecialistGeneratedFile(
            instrument_name=utils.INSTRUMENT_NAME,
            target_date=utils.extract_target_date(registros),
            level=produto.nivel,
            scientific_product_key=produto.chave,
            product_resolution=produto.resolucao,
            generated_file_format=fmt,
            generated_file_full_path=os.path.abspath(caminho),
        )

    @staticmethod
    def _cabecalho(nome: str, registros: List[Dict[str, Any]], produto: Produto):
        """
        As mesmas chaves que o POEMAS usa, para a ferramenta que lê um ler o outro.

        TZ é UTC nos dois formatos. O POEMAS escreve o CSV em GMT-3 e o FITS em
        UTC, ou seja o mesmo instante em dois fusos em dois arquivos do mesmo
        produto; não há leitura de "seguir o POEMAS" que seja consistente, e o
        husec do HATS é contado desde 0 UT.
        """
        primeiro = utils.as_utc(registros[0][utils.TIME_FIELD])
        ultimo = utils.as_utc(registros[-1][utils.TIME_FIELD])
        data = primeiro.strftime("%Y-%m-%d")
        return [
            ("FILENAME", nome),
            ("ORIGIN", utils.ORIGIN),
            ("TELESCOP", utils.TELESCOPE),
            ("INSTRUME", utils.TELESCOPE),
            ("OBSERVAT", utils.OBSERVATORY),
            ("STATION", utils.STATION),
            ("TZ", "UTC"),
            ("DATE-OBS", data),
            ("T_START", utils.iso_second(primeiro)),
            ("T_END", utils.iso_second(ultimo)),
            ("N_RECORD", len(registros)),
            ("FREQUEN", utils.FREQUENCY),
            ("FILERES", produto.resolucao),
            ("LEVEL", produto.nivel),
        ]

    @staticmethod
    def _documentacao(produto: Produto):
        """Bloco de documentação, montado a partir da tabela de colunas."""
        linhas = [
            "COPYRIGHT. Grant of use.",
            "These data are property of Universidade Presbiteriana Mackenzie.",
            "The Centro de Radio Astronomia e Astrofisica Mackenzie is responsible",
            "for their distribution. Grant of use permission is given for Academic",
            "purposes only.",
            "",
            "Main Header: General metadata about the observation export",
            "   FILENAME: Name of the generated file",
            "   ORIGIN: Producing institution",
            "   TELESCOP / INSTRUME: Telescope and instrument name",
            "   OBSERVAT: Observatory site name",
            "   STATION: Latitude, longitude and altitude",
            "   TZ: Time zone of every timestamp in this file",
            "   DATE-OBS: Date of the observation (YYYY-MM-DD)",
            "   T_START / T_END: Observation start/end in ISO 8601 format",
            "   N_RECORD: Number of data rows in this file",
            "   FREQUEN: Observing frequency",
            "   FILERES: Temporal resolution of this file",
            "   LEVEL: Data level of this file",
            "",
            "PRODUCT NOTES",
            "   SCIENTIFIC PRODUCT: {}".format(produto.chave),
            "   LEVEL: {}   RESOLUTION: {}".format(produto.nivel, produto.resolucao),
            "",
        ]
        if produto.janela is None:
            linhas += [
                "   LEVEL = raw: the time series exactly as registered by the instrument",
                "   acquisition system, with no transformation applied.",
                "",
                "   " + AVISO_DO_SINAL_PICADO,
            ]
        else:
            linhas += [
                "   Derived by the arithmetic mean of each set of {} consecutive samples "
                "within".format(produto.janela),
                "   non-overlapping {} intervals.".format(produto.resolucao),
                "",
                "   " + AVISO_DA_MEDIA,
            ]
        linhas += [
            "",
            "ITEMS - Contains detailed observation data records",
        ]
        linhas.extend("   {}: {}".format(coluna.nome, coluna.descricao)
                      for coluna in HatsExport._colunas(produto))
        return linhas

    @staticmethod
    def _celula(coluna: Coluna, registro: Dict[str, Any]) -> str:
        if coluna.tipo == "time":
            return utils.iso_micro(utils.as_utc(registro[utils.TIME_FIELD]))
        valor = registro.get(coluna.chave)
        if valor is None:
            return ""
        if coluna.tipo == "int":
            return str(int(valor))
        return utils.format_float(valor)

    def _escreve_csv(self, caminho: str, registros, cabecalho, produto: Produto) -> None:
        with open(caminho, "w", encoding="utf-8", newline="") as arquivo:
            escritor = csv.writer(arquivo, delimiter="|", quoting=csv.QUOTE_MINIMAL)

            escritor.writerow(["#DOCUMENTATION", ""])
            for linha in self._documentacao(produto):
                escritor.writerow([linha, ""])
            escritor.writerow([])

            escritor.writerow(["#HEADER", ""])
            for chave, valor in cabecalho:
                escritor.writerow([chave, str(valor)])
            escritor.writerow([])

            escritor.writerow(["#ITEMS", ""])
            colunas = self._colunas(produto)
            escritor.writerow([coluna.nome for coluna in colunas])
            for registro in registros:
                escritor.writerow([self._celula(coluna, registro) for coluna in colunas])

    def _escreve_fits(self, caminho: str, registros, cabecalho, produto: Produto) -> None:
        try:
            from astropy.io import fits
        except ImportError as erro:                     # pragma: no cover
            raise RuntimeError(
                "a geracao de FITS exige astropy no ambiente do job; o CSV nao depende "
                "dela") from erro

        primario = fits.Header()
        primario.add_blank()
        for chave, valor in cabecalho:
            primario[chave] = valor
        for linha in self._documentacao(produto):
            primario.add_comment(linha)

        colunas = [fits.Column(name=coluna.nome, format=coluna.fits,
                               array=self._vetor(coluna, registros))
                   for coluna in self._colunas(produto)]
        tabela = fits.BinTableHDU.from_columns(fits.ColDefs(colunas), name="DataTable")
        fits.HDUList([fits.PrimaryHDU(header=primario), tabela]).writeto(
            caminho, overwrite=True)

    @staticmethod
    def _vetor(coluna: Coluna, registros) -> List[Any]:
        if coluna.tipo == "time":
            return [utils.iso_micro(utils.as_utc(r[utils.TIME_FIELD])) for r in registros]
        if coluna.tipo == "int":
            return [int(r.get(coluna.chave) or 0) for r in registros]
        return [float(r.get(coluna.chave) or 0.0) for r in registros]
