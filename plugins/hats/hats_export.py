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

Quatro estruturas, e o que cabe em cada arquivo
-----------------------------------------------
O Guigue pediu o level 0 com as quatro estruturas do `HATS.py`: `rData` (a
contagem do ADC), `cData` (o valor convertido), `aux` (o apontamento) e `Deconv`
(a amplitude demodulada) — **em camadas no FITS**, e **um arquivo CSV por
estrutura**.

O `rData` e o `cData` saem do mesmo `.rbd`, na mesma chamada, e por isso cabem no
mesmo FITS, em duas HDU. O `aux` chega em outra chamada, porque é outra fonte RAW,
e o `Deconv` é produto derivado: cada um sai no seu arquivo, com a camada de mesmo
nome. As quatro num único FITS exigiriam o pipeline entregar as três fontes de uma
vez, e o contrato é de uma fonte por chamada — isto é limite do pipeline, não
escolha nossa, e está anotado para a conversa com ele.

No CSV, que é plano, cada estrutura é um arquivo. É por isso que há um método para
o CSV do `cData` e outro para o do `rData` sobre a mesma entrada.

Nomes de método e chaves de produto
-----------------------------------
Os seis nomes obrigatórios vêm do §7.2. Os outros quatro — o demodulado, o
apontamento e o CSV das contagens — não estão na especificação, porque ela não
previu instrumento com mais de uma estrutura. Eles seguem o padrão dos
obrigatórios e **dependem de o catálogo declarar**; as chaves e resoluções
correspondentes ficam isoladas em `hats_utils.py`, marcadas como pendentes.

Uma tabela de colunas para os dois escritores
---------------------------------------------
FITS e CSV saem da mesma lista de colunas. No plugin do POEMAS a lista
aparece duas vezes, uma em cada escritor, e as duas precisam ser mantidas em
acordo à mão — é o tipo de duplicação que fica errada em silêncio.

O que é publicado, e o que não é
--------------------------------
O registro que a ingestão produz tem mais campos do que o arquivo publicado. Vão
para o arquivo o carimbo de tempo nas duas formas (ISO e husec) e os canais de
cada estrutura. Ficam de fora `PROC_SEQ`, que é contador interno da ingestão, e
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

import hats_level1 as level1
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

COLUNA_DE_HUSEC = Coluna(
    "HUSEC", "HUSEC", "K",
    "Hundredths of a millisecond since 0 UT, as stamped by the instrument. "
    "Preserves the 0.1 ms resolution of the acquisition counter",
    tipo="int")

# cData: os cinco canais com a reta de calibração aplicada.
COLUNAS_RAW: Sequence[Coluna] = (
    COLUNA_DE_TEMPO,
    COLUNA_DE_HUSEC,
    Coluna("GOLAY", "GOLAY", "D", "Golay cell detector signal (mV)"),
    Coluna("CHOPPER", "CHOPPER", "D", "Chopper reference signal (mV)"),
    Coluna("TEMP_HICS", "TEMP_HICS", "D", "HICS temperature (Celsius)"),
    Coluna("TEMP_ENV", "TEMP_ENV", "D", "Enclosure temperature (Celsius)"),
    Coluna("TEMP_GOLAY", "TEMP_GOLAY", "D", "Golay cell temperature (Celsius)"),
)

# rData: as mesmas grandezas em contagem do ADC, antes da reta. Inteiro de 24 bits
# com sinal, então "J" (32 bits) cobre a faixa com folga.
COLUNAS_CONTAGENS: Sequence[Coluna] = (
    COLUNA_DE_TEMPO,
    COLUNA_DE_HUSEC,
    Coluna("GOLAY_ADCU", "GOLAY_ADCU", "J",
           "Golay cell detector channel, raw ADC counts", tipo="int"),
    Coluna("CHOPPER_ADCU", "CHOPPER_ADCU", "J",
           "Chopper reference channel, raw ADC counts", tipo="int"),
    Coluna("TEMP_HICS_ADCU", "TEMP_HICS_ADCU", "J",
           "HICS temperature channel, raw ADC counts", tipo="int"),
    Coluna("TEMP_ENV_ADCU", "TEMP_ENV_ADCU", "J",
           "Enclosure temperature channel, raw ADC counts", tipo="int"),
    Coluna("TEMP_GOLAY_ADCU", "TEMP_GOLAY_ADCU", "J",
           "Golay cell temperature channel, raw ADC counts", tipo="int"),
)

# aux: as onze variáveis do HATSAuxFormat.xml, mais o carimbo em ISO. As unidades
# são as dos dados, não as declaradas no XML, que traz "degrees" em quatro campos
# onde o HATS.py usa hora e arcsec/s.
COLUNAS_APONTAMENTO: Sequence[Coluna] = (
    COLUNA_DE_TEMPO,
    COLUNA_DE_HUSEC,
    Coluna("JD", "JD", "D",
           "Julian date of the pointing solution. Zero marks a record whose "
           "solution was not recomputed in this cycle"),
    Coluna("SID", "SID", "D", "Local sidereal time (hours)"),
    Coluna("ELEVATION", "ELEVATION", "D", "Telescope elevation (degrees)"),
    Coluna("AZIMUTH", "AZIMUTH", "D", "Telescope azimuth (degrees)"),
    Coluna("RIGHT_ASCENSION", "RIGHT_ASCENSION", "D",
           "Right ascension of the observed object (hours)"),
    Coluna("DECLINATION", "DECLINATION", "D",
           "Declination of the observed object (degrees)"),
    Coluna("RA_RATE", "RA_RATE", "D",
           "Rate of change of right ascension (arcsec/s)"),
    Coluna("DEC_RATE", "DEC_RATE", "D",
           "Rate of change of declination (arcsec/s)"),
    Coluna("OBJECT", "OBJECT", "J",
           "Identifier of the observed object, as coded by the control software",
           tipo="int"),
    Coluna("OPMODE", "OPMODE", "J",
           "Identifier of the telescope operation mode, as coded by the control "
           "software", tipo="int"),
)

# Deconv: as três colunas da estrutura de mesmo nome do HATS.py.
COLUNAS_DECONV: Sequence[Coluna] = (
    COLUNA_DE_TEMPO,
    Coluna("HUSEC", "HUSEC", "K",
           "Hundredths of a millisecond since 0 UT, at the centre of the analysis "
           "window", tipo="int"),
    Coluna("AMPLITUDE", "AMPLITUDE", "D",
           "Amplitude of the 20 Hz modulation of the detector signal (mV)"),
)


# Level 1: as doze variáveis que o Guigue enumerou, na ordem em que ele as
# enumerou. São catorze colunas porque o item 3 da lista dele são as três
# temperaturas. O `husec` não entra: ele não o pediu, a grade é uniforme e ancorada
# em 0 UT, então o instante de cada ponto se recupera exatamente do carimbo ISO.
COLUNAS_NIVEL1: Sequence[Coluna] = (
    COLUNA_DE_TEMPO,
    Coluna("AMPLITUDE", "AMPLITUDE", "D",
           "Amplitude of the 20 Hz modulation of the detector signal (mV), "
           "interpolated onto the uniform time grid"),
    Coluna("TEMP_HICS", "TEMP_HICS", "D",
           "HICS temperature (Celsius), interpolated onto the uniform time grid"),
    Coluna("TEMP_ENV", "TEMP_ENV", "D",
           "Enclosure temperature (Celsius), interpolated onto the uniform time grid"),
    Coluna("TEMP_GOLAY", "TEMP_GOLAY", "D",
           "Golay cell temperature (Celsius), interpolated onto the uniform time grid"),
    Coluna("SID", "SID", "D",
           "Local sidereal time (hours), interpolated onto the uniform time grid"),
    Coluna("ELEVATION", "ELEVATION", "D",
           "Telescope elevation (degrees), interpolated onto the uniform time grid"),
    Coluna("AZIMUTH", "AZIMUTH", "D",
           "Telescope azimuth (degrees), interpolated onto the uniform time grid"),
    Coluna("RIGHT_ASCENSION", "RIGHT_ASCENSION", "D",
           "Right ascension of the observed object (hours), interpolated onto the "
           "uniform time grid"),
    Coluna("DECLINATION", "DECLINATION", "D",
           "Declination of the observed object (degrees), interpolated onto the "
           "uniform time grid"),
    Coluna("RA_RATE", "RA_RATE", "D",
           "Rate of change of right ascension (arcsec/s), interpolated onto the "
           "uniform time grid"),
    Coluna("DEC_RATE", "DEC_RATE", "D",
           "Rate of change of declination (arcsec/s), interpolated onto the uniform "
           "time grid"),
    Coluna("OBJECT", "OBJECT", "J",
           "Identifier of the observed object, as coded by the control software, "
           "resampled by nearest neighbour", tipo="int"),
    Coluna("OPMODE", "OPMODE", "J",
           "Identifier of the telescope operation mode, as coded by the control "
           "software, resampled by nearest neighbour", tipo="int"),
)


@dataclass(frozen=True)
class Camada:
    """
    Uma estrutura do HATS.py dentro de um arquivo.

    No FITS vira uma HDU com este `nome` em EXTNAME; no CSV, que é plano, vira o
    arquivo inteiro. O nome é o da estrutura no `HATS.py`, para que quem abrir o
    arquivo reconheça de onde o dado veio.
    """
    nome: str
    colunas: Sequence[Coluna]
    descricao: str


CAMADA_RDATA = Camada(
    "rData", COLUNAS_CONTAGENS,
    "ADC counts as registered by the acquisition system, before the calibration "
    "lines are applied (HATS.py rbd.rData)")

CAMADA_CDATA = Camada(
    "cData", COLUNAS_RAW,
    "Detector channels in physical units, obtained by applying the calibration "
    "lines of the instrument format tables to the ADC counts (HATS.py rbd.cData)")

CAMADA_APONTAMENTO = Camada(
    "aux", COLUNAS_APONTAMENTO,
    "Telescope pointing solution, as registered by the control loop (HATS.py aux)")

CAMADA_DECONV = Camada(
    "Deconv", COLUNAS_DECONV,
    "Amplitude of the 20 Hz modulation of the detector signal (HATS.py rbd.Deconv)")

# Esta não tem nome no HATS.py: é o conjunto reduzido que o Guigue descreveu para o
# dado integrado, e ele pediu "um arquivo único com essas colunas".
CAMADA_NIVEL1 = Camada(
    "Level1", COLUNAS_NIVEL1,
    "Reduced set of variables on a uniform time grid, interpolated from the "
    "detector, the demodulated amplitude and the pointing")


# Janelas derivadas da resolução alvo, como o suporte confirmou. A 1 kHz, 0,1 s
# são 100 amostras e 1 s são 1000.
JANELA_100MS = int(constants.SAMPLING_FREQUENCY * 0.1)
JANELA_1S = int(constants.SAMPLING_FREQUENCY * 1.0)

# Campos agregados pela derivação. Os demais vêm do registro central da janela.
CANAIS_AGREGADOS = ("GOLAY", "CHOPPER", "TEMP_HICS", "TEMP_ENV", "TEMP_GOLAY")

# Passo da grade do level 1, em husec. Inteiro exato: 0,1 s são 1000 husec.
PASSO_NIVEL1_100MS = int(constants.HUSEC_PER_SECOND * 0.1)
PASSO_NIVEL1_1S = constants.HUSEC_PER_SECOND

# De onde vem cada coluna do level 1, e como ela atravessa a grade.
CAMPOS_DE_TEMPERATURA = ("TEMP_HICS", "TEMP_ENV", "TEMP_GOLAY")
CAMPOS_CONTINUOS_DO_APONTAMENTO = ("SID", "ELEVATION", "AZIMUTH", "RIGHT_ASCENSION",
                                   "DECLINATION", "RA_RATE", "DEC_RATE")
CAMPOS_CATEGORICOS = ("OBJECT", "OPMODE")


# ------------------------------------------------------------------ avisos
#
# Avisos que viajam com o arquivo, não só com o código. Sem eles, quem baixar o
# produto não tem como saber o que tem na mão.

# Os números vêm de 299 s de observação real de 2026-03-17T1800: as médias em
# janelas de 100 ms têm média 0,015 mV e desvio 1,532 mV, e a amplitude demodulada
# no mesmo intervalo tem média 124,167 mV.
AVISO_DO_SINAL_PICADO = (
    "CAUTION: the HATS detector is a Golay cell behind a 20 Hz chopper, so the "
    "detector channel is a modulated carrier and not a flux series. Averaging it "
    "over a whole number of chopper cycles cancels the carrier: measured on real "
    "data, means over 100 ms fluctuate around zero with a standard deviation of "
    "1.5 mV, while the amplitude of the modulation, which is the physical quantity, "
    "is about 124 mV. Recovering the amplitude requires demodulation.")

AVISO_DA_MEDIA = (
    "CAUTION: this product is the arithmetic mean of the raw detector signal over "
    "non-overlapping windows. Because the signal is a 20 Hz modulated carrier and "
    "each window spans a whole number of chopper cycles, the carrier cancels out and "
    "the mean does NOT carry the solar signal: measured on real data it fluctuates "
    "around zero with a standard deviation of 1.5 mV, while the amplitude of the "
    "modulation is about 124 mV. The timestamp of each record refers to the centre "
    "of its aggregation window.")

# Medido sobre as 32 horas de .aux que temos (2026-03-17 a 19): a fração de
# registros defasados vai de 1,8% a 95% por hora, e numa delas — a das 11:00 de
# 2026-03-18 — nenhum registro tem jd preenchido. Eles não se espalham: vêm em
# rajadas em que quase todo registro do minuto é defasado, de dois a sete minutos
# na maioria das horas e cobrindo metade ou mais da hora em duas delas.
AVISO_DO_APONTAMENTO_DEFASADO = (
    "CAUTION: in records whose JD is zero the pointing solution was not recomputed "
    "in that cycle and carries coordinates from about 1053 s earlier; the timestamp "
    "is current but the position is not. Measured over 32 hours of real observation, "
    "the fraction of records of an hour in this state ranges from 1.8% to 95%, and in "
    "one further hour every record was in this state. They do not occur uniformly: "
    "they come in bursts in which nearly every record of the minute is stale, lasting "
    "from two to seven minutes in most hours and covering half or more of the hour in "
    "two of those measured. Interpolating the pointing onto a uniform time grid without "
    "filtering these records propagates the stale coordinates into every interpolated "
    "value around them. OBJECT and OPMODE are categorical identifiers and admit "
    "nearest-neighbour resampling only.")

NOTA_DO_CDATA = (
    "LEVEL = raw: the detector time series as registered by the instrument "
    "acquisition system, with the calibration lines declared in the instrument "
    "format tables applied to the ADC counts and no other transformation. "
    "Corresponds to the cData structure of HATS.py.")

NOTA_DO_RDATA = (
    "LEVEL = raw: the ADC counts as registered, sign-extended from the 24 "
    "significant bits of the AD7770 word, with no calibration applied. Corresponds "
    "to the rData structure of HATS.py; the same samples in physical units are "
    "published as the {} product.".format(utils.PRODUTO_RAW))

NOTA_DO_APONTAMENTO = (
    "LEVEL = raw: the telescope pointing solution as registered by the control "
    "loop, at its native cadence. The cadence is a software loop and not a hardware "
    "clock: measured on real data it averages 1.0507 s, alternating between 1.048 "
    "and 1.052 s. Corresponds to the aux structure of HATS.py.")

# O XML do apontamento declara grau em três campos onde o dado está em hora e
# arcsec/s. O pacote `hats` corrige isso no esquema (AUX_UNIT_FIXES) e o arquivo
# publica o valor como está gravado, com a unidade real na descrição da coluna —
# mas quem for conferir contra o XML precisa saber da discrepância.
NOTA_DAS_UNIDADES_DO_APONTAMENTO = (
    "NOTE ON UNITS: the instrument format table declares degrees for "
    "RIGHT_ASCENSION, RA_RATE and DEC_RATE, while the values actually stored are "
    "hours, arcsec/s and arcsec/s. They are published exactly as stored, in the "
    "units stated in the column descriptions below.")

NOTA_DO_DECONV = (
    "LEVEL = derived: amplitude of the 20 Hz modulation of the detector signal, "
    "recovered by the Goertzel algorithm over a flat-top window of 128 samples "
    "stepping 32 samples at a time, which yields one value every 32 ms. The "
    "timestamp of each record is the centre of its analysis window. Corresponds to "
    "the Deconv structure of HATS.py, and this is the physical quantity the "
    "instrument measures.")


def _nota_do_nivel1(resolucao: str) -> str:
    return ("LEVEL = derived: the reduced set of variables on a uniform time grid of "
            "{}. The grid points are exact multiples of that interval counted from "
            "0 UT, so the grids of consecutive files join with no overlap and no "
            "repeated instant. Every column is interpolated onto the grid from its "
            "source series, because the sources have different native cadences: the "
            "demodulated amplitude is produced every 32 ms, the temperatures every "
            "1 ms and the pointing about every 1.05 s. Each grid point therefore "
            "carries the value interpolated AT that instant, not an average over the "
            "interval. The grid covers only the span where every source series has "
            "samples; nothing is extrapolated.".format(resolucao))


NOTA_DA_INTERPOLACAO = (
    "INTERPOLATION: linear for the continuous variables, which are AMPLITUDE, the "
    "three temperatures, SID, ELEVATION, AZIMUTH, RIGHT_ASCENSION, DECLINATION, "
    "RA_RATE and DEC_RATE. OBJECT and OPMODE are categorical identifiers written by "
    "the control software, so they are resampled by nearest neighbour, ties resolved "
    "towards the earlier sample: no value appears in those columns that the "
    "instrument did not write.")


def _nota_do_apontamento_no_nivel1() -> str:
    """
    O arquivo declara qual das duas decisões pendentes foi tomada.

    Gerada da constante, e não escrita à mão, para o arquivo não poder dizer uma
    coisa enquanto o código faz outra.
    """
    comum = ("STALE POINTING: records of the pointing file whose JD is zero carry a "
             "solution computed about 1053 s earlier (the timestamp is current, the "
             "coordinates are not). Measured over 32 hours of real observation, the "
             "fraction of records of an hour in this state ranges from 1.8% to 95%, "
             "and they come in bursts rather than spread out: two to seven minutes in "
             "most hours, half the hour or more in two of those measured. "
             "This product does not publish JD, so the grid points affected by them "
             "cannot be identified from the file. ")
    if utils.NIVEL1_DESCARTA_APONTAMENTO_DEFASADO:
        return comum + (
            "They are DISCARDED as interpolation nodes here. Enough records survive "
            "inside each burst to keep the remaining gaps short: over the same 32 "
            "hours the longest gap left between two usable pointing records is about "
            "67 s, and while the telescope tracks, the elevation changes by at most "
            "0.23 degree across such a gap, which is the size of the error the linear "
            "interpolation can introduce there. Keeping the stale records instead "
            "shifts the published elevation by up to 2 degrees, measured on one hour "
            "of real data. This choice is pending confirmation by the instrument team.")
    return comum + (
        "They are KEPT as interpolation nodes here, so the pointing columns around "
        "them carry coordinates from about 17 minutes earlier: measured on one hour of "
        "real data, the published elevation differs by up to 2 degrees from what the "
        "same grid gives when they are discarded. This choice is pending confirmation "
        "by the instrument team.")


def _nota_da_media(janela: int, resolucao: str) -> str:
    return ("LEVEL = derived: the arithmetic mean of each set of {} consecutive "
            "samples, within non-overlapping intervals of {}.".format(janela, resolucao))


@dataclass(frozen=True)
class Produto:
    chave: str
    resolucao: str
    nivel: str
    janela: Optional[int]      # None para RAW e para o demodulado, que não agregam

    # O que vai no bloco de documentação do arquivo. Cada item é um texto, ou uma
    # função sem argumento que devolve o texto. A segunda forma existe porque há
    # nota que depende de decisão pendente: resolvê-la no import deixaria o arquivo
    # declarando uma coisa enquanto o código faz outra, se a decisão mudar.
    notas: Sequence[Any] = ()


PRODUTO_RAW = Produto(utils.PRODUTO_RAW, utils.RESOLUCAO_RAW, "raw", None,
                      (NOTA_DO_CDATA, AVISO_DO_SINAL_PICADO))
PRODUTO_CONTAGENS = Produto(utils.PRODUTO_CONTAGENS, utils.RESOLUCAO_RAW, "raw", None,
                            (NOTA_DO_RDATA, AVISO_DO_SINAL_PICADO))
PRODUTO_APONTAMENTO = Produto(utils.PRODUTO_APONTAMENTO, utils.RESOLUCAO_APONTAMENTO,
                              "raw", None,
                              (NOTA_DO_APONTAMENTO, AVISO_DO_APONTAMENTO_DEFASADO,
                               NOTA_DAS_UNIDADES_DO_APONTAMENTO))
PRODUTO_DECONV = Produto(utils.PRODUTO_DECONV, utils.RESOLUCAO_DECONV, "derived", None,
                         (NOTA_DO_DECONV,))
PRODUTO_NIVEL1_100MS = Produto(
    utils.PRODUTO_NIVEL1_100MS, utils.RESOLUCAO_100MS, "derived", None,
    (_nota_do_nivel1(utils.RESOLUCAO_100MS), NOTA_DA_INTERPOLACAO,
     _nota_do_apontamento_no_nivel1, NOTA_DAS_UNIDADES_DO_APONTAMENTO))
PRODUTO_NIVEL1_1S = Produto(
    utils.PRODUTO_NIVEL1_1S, utils.RESOLUCAO_1S, "derived", None,
    (_nota_do_nivel1(utils.RESOLUCAO_1S), NOTA_DA_INTERPOLACAO,
     _nota_do_apontamento_no_nivel1, NOTA_DAS_UNIDADES_DO_APONTAMENTO))
PRODUTO_100MS = Produto(utils.PRODUTO_100MS, utils.RESOLUCAO_100MS, "derived", JANELA_100MS,
                        (_nota_da_media(JANELA_100MS, utils.RESOLUCAO_100MS),
                         AVISO_DA_MEDIA))
PRODUTO_1S = Produto(utils.PRODUTO_1S, utils.RESOLUCAO_1S, "derived", JANELA_1S,
                     (_nota_da_media(JANELA_1S, utils.RESOLUCAO_1S), AVISO_DA_MEDIA))


class InsufficientRecords(ValueError):
    """
    A entrada é curta demais para o produto pedido.

    Distinta de um defeito: um arquivo com 441 amostras simplesmente não contém uma
    janela de 1 s, e nenhum produto de 1 s pode sair dele. Quem chama precisa poder
    separar "este produto não se aplica a esta entrada" de "algo quebrou", e com um
    ValueError genérico não conseguiria. Continua sendo ValueError para o pipeline,
    que não conhece a subclasse.
    """


class WrongStructure(ValueError):
    """
    Os registros recebidos não são da estrutura que este produto publica.

    Existe porque agora há quatro estruturas e quatro famílias de método. Sem esta
    checagem, chamar o CSV do apontamento com registros do detector escreveria um
    arquivo com onze colunas vazias — bem formado, publicável, e sem dado nenhum.
    """


class HatsExport(object):
    """Responsabilidade única: gerar FITS e CSV a partir de registros RAW."""

    # ------------------------------------------------- contrato obrigatório

    def GenerateFitsFileRawFormat(self, raw_records: List[Dict[str, Any]]):
        """
        O level 0 do detector: as contagens e os valores convertidos, em camadas.

        Duas HDU no mesmo arquivo, na ordem em que o Guigue listou as estruturas.
        """
        return self._escreve(raw_records, PRODUTO_RAW, "FITS", "fits",
                             (CAMADA_RDATA, CAMADA_CDATA))

    def GenerateCSVFileRawFormat(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(raw_records, PRODUTO_RAW, "CSV", "csv", (CAMADA_CDATA,))

    def GenerateFitsFileDerivedFormat100ms(self, raw_records: List[Dict[str, Any]]):
        derivados = self._derive_raw_to_fixed_windows(raw_records, JANELA_100MS)
        return self._escreve(derivados, PRODUTO_100MS, "FITS", "fits", (CAMADA_CDATA,))

    def GenerateCSVFileDerivedFormat100ms(self, raw_records: List[Dict[str, Any]]):
        derivados = self._derive_raw_to_fixed_windows(raw_records, JANELA_100MS)
        return self._escreve(derivados, PRODUTO_100MS, "CSV", "csv", (CAMADA_CDATA,))

    def GenerateFitsFileDerivedFormat1s(self, raw_records: List[Dict[str, Any]]):
        derivados = self._derive_raw_to_fixed_windows(raw_records, JANELA_1S)
        return self._escreve(derivados, PRODUTO_1S, "FITS", "fits", (CAMADA_CDATA,))

    def GenerateCSVFileDerivedFormat1s(self, raw_records: List[Dict[str, Any]]):
        derivados = self._derive_raw_to_fixed_windows(raw_records, JANELA_1S)
        return self._escreve(derivados, PRODUTO_1S, "CSV", "csv", (CAMADA_CDATA,))

    # ------------------------------- um arquivo CSV por estrutura do level 0

    def GenerateCSVFileRawCountsFormat(self, raw_records: List[Dict[str, Any]]):
        """
        O `rData` em arquivo próprio, porque o CSV é plano e não tem camada.

        No FITS esta estrutura é a primeira HDU do produto RAW; aqui é um arquivo.
        """
        return self._escreve(raw_records, PRODUTO_CONTAGENS, "CSV", "csv",
                             (CAMADA_RDATA,))

    def GenerateFitsFilePointingFormat(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(raw_records, PRODUTO_APONTAMENTO, "FITS", "fits",
                             (CAMADA_APONTAMENTO,))

    def GenerateCSVFilePointingFormat(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(raw_records, PRODUTO_APONTAMENTO, "CSV", "csv",
                             (CAMADA_APONTAMENTO,))

    # ------------------------------------------------ level 1: grade uniforme

    def GenerateFitsFileLevel1Format100ms(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(self._integra(raw_records, PASSO_NIVEL1_100MS),
                             PRODUTO_NIVEL1_100MS, "FITS", "fits", (CAMADA_NIVEL1,))

    def GenerateCSVFileLevel1Format100ms(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(self._integra(raw_records, PASSO_NIVEL1_100MS),
                             PRODUTO_NIVEL1_100MS, "CSV", "csv", (CAMADA_NIVEL1,))

    def GenerateFitsFileLevel1Format1s(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(self._integra(raw_records, PASSO_NIVEL1_1S),
                             PRODUTO_NIVEL1_1S, "FITS", "fits", (CAMADA_NIVEL1,))

    def GenerateCSVFileLevel1Format1s(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(self._integra(raw_records, PASSO_NIVEL1_1S),
                             PRODUTO_NIVEL1_1S, "CSV", "csv", (CAMADA_NIVEL1,))

    @staticmethod
    def _separa_por_estrutura(raw_records: List[Dict[str, Any]]):
        """
        Separa os registros do detector dos registros do apontamento.

        O level 1 é a única coisa que o Guigue pediu que precisa das DUAS fontes na
        mesma conta: a amplitude e as temperaturas vêm do `.rbd`, as nove variáveis
        de apontamento vêm do `.aux`. O contrato entrega uma fonte por chamada, então
        este método só funciona se a tarefa do catálogo for declarada sobre as duas
        fontes juntas. Não há como o plugin contornar isso por conta própria: ele não
        acessa banco e não conhece as regras do pipeline (§11).
        """
        detector = [registro for registro in raw_records if "GOLAY" in registro]
        apontamento = [registro for registro in raw_records if "JD" in registro]

        if not detector or not apontamento:
            faltando = "do detector (.rbd)" if not detector else "do apontamento (.aux)"
            raise WrongStructure(
                "o level 1 precisa das duas fontes na mesma chamada e nao veio registro "
                "{}. Dos {} registros recebidos, {} sao do detector e {} do apontamento. "
                "A tarefa do catalogo tem de ser declarada sobre as duas fontes".format(
                    faltando, len(raw_records), len(detector), len(apontamento)))

        return detector, apontamento

    def _integra(self, raw_records: List[Dict[str, Any]], passo: int) -> List[Dict[str, Any]]:
        """
        As colunas do level 1 sobre a grade uniforme, como ele descreveu.

        Cinco passos: separar as fontes, demodular o detector, escolher os nós do
        apontamento, montar a grade na interseção das séries e interpolar cada coluna
        sobre ela.
        """
        detector, apontamento = self._separa_por_estrutura(raw_records)

        for campo in CAMPOS_DE_TEMPERATURA:
            if campo not in detector[0]:
                raise WrongStructure(
                    "o level 1 publica as tres temperaturas e o registro do detector nao "
                    "tem {}. O formato de 26 bytes do .rbd nao tem canal de temperatura, "
                    "e dele nao sai este produto".format(campo))
        for campo in CAMPOS_CONTINUOS_DO_APONTAMENTO + CAMPOS_CATEGORICOS:
            if campo not in apontamento[0]:
                raise WrongStructure(
                    "o level 1 publica {} e o registro do apontamento nao tem esse "
                    "campo".format(campo))

        demodulado = self._demodulate(detector)

        if utils.NIVEL1_DESCARTA_APONTAMENTO_DEFASADO:
            total = len(apontamento)
            apontamento = [registro for registro in apontamento
                           if float(registro.get("JD") or 0.0) != 0.0]
            if len(apontamento) < 2:
                raise InsufficientRecords(
                    "sobraram {} de {} registros de apontamento depois de descartar os "
                    "de jd zerado, e a interpolacao precisa de ao menos dois".format(
                        len(apontamento), total))

        nos = {
            "amplitude": [int(registro["HUSEC"]) for registro in demodulado],
            "detector": [int(registro["HUSEC"]) for registro in detector],
            "apontamento": [int(registro["HUSEC"]) for registro in apontamento],
        }
        for nome, husecs in nos.items():
            level1.verifica_nos(nome, husecs)

        primeiro = max(husecs[0] for husecs in nos.values())
        ultimo = min(husecs[-1] for husecs in nos.values())
        if ultimo < primeiro:
            raise InsufficientRecords(
                "as series nao se sobrepoem: o ultimo instante comum ({}) vem antes do "
                "primeiro ({}). Sem intervalo comum nao ha grade a preencher sem "
                "extrapolar".format(ultimo, primeiro))

        pontos = level1.grade(primeiro, ultimo, passo)
        if not pontos:
            raise InsufficientRecords(
                "nenhum multiplo de {} husec desde 0 UT cai no intervalo comum das "
                "series [{}, {}]: a entrada e mais curta que um ponto da grade".format(
                    passo, primeiro, ultimo))

        colunas: Dict[str, List[Any]] = {
            "AMPLITUDE": level1.interpola_linear(
                nos["amplitude"],
                [float(registro["AMPLITUDE"]) for registro in demodulado], pontos)}

        # Uma coluna de cada vez: os nós do detector são 3,6 milhões por hora de
        # observação, e materializar as três temperaturas juntas dobraria o pico.
        for campo in CAMPOS_DE_TEMPERATURA:
            colunas[campo] = level1.interpola_linear(
                nos["detector"],
                [float(registro[campo]) for registro in detector], pontos)
        for campo in CAMPOS_CONTINUOS_DO_APONTAMENTO:
            colunas[campo] = level1.interpola_linear(
                nos["apontamento"],
                [float(registro[campo]) for registro in apontamento], pontos)

        if utils.NIVEL1_MODO_DOS_CATEGORICOS != "nearest":
            raise ValueError(
                "NIVEL1_MODO_DOS_CATEGORICOS vale {!r} e a unica forma implementada e "
                "'nearest'. Interpolar um identificador linearmente produziria valor "
                "que o software de controle nunca escreveu".format(
                    utils.NIVEL1_MODO_DOS_CATEGORICOS))
        for campo in CAMPOS_CATEGORICOS:
            colunas[campo] = level1.interpola_vizinho(
                nos["apontamento"],
                [int(registro[campo]) for registro in apontamento], pontos)

        meia_noite = utils.as_utc(detector[0][utils.TIME_FIELD]).replace(
            hour=0, minute=0, second=0, microsecond=0)
        return [
            dict({utils.TIME_FIELD: meia_noite + timedelta(microseconds=husec * 100)},
                 **{campo: valores[posicao] for campo, valores in colunas.items()})
            for posicao, husec in enumerate(pontos)]

    # ------------------------------------------- produto científico do HATS

    def GenerateFitsFileDeconvFormat32ms(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(self._demodulate(raw_records), PRODUTO_DECONV,
                             "FITS", "fits", (CAMADA_DECONV,))

    def GenerateCSVFileDeconvFormat32ms(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(self._demodulate(raw_records), PRODUTO_DECONV,
                             "CSV", "csv", (CAMADA_DECONV,))

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
    def _confere_a_estrutura(registros: List[Dict[str, Any]],
                             camadas: Sequence[Camada]) -> None:
        """
        Os registros têm de pertencer à estrutura que o produto publica.

        Basta uma coluna de dado para reconhecer a estrutura; não se exigem todas,
        porque o formato de 26 bytes do `.rbd` não tem os canais de temperatura e
        um arquivo daqueles continua sendo detector. O que esta checagem recusa é a
        troca de estrutura: registros de apontamento no CSV do detector sairiam com
        todas as colunas vazias, num arquivo bem formado e sem dado nenhum.
        """
        presentes = set(registros[0])
        for camada in camadas:
            dados = [coluna.chave for coluna in camada.colunas
                     if coluna.chave and coluna.chave != "HUSEC"]
            if not presentes.intersection(dados):
                raise WrongStructure(
                    "nenhuma coluna de dado da estrutura {} existe nestes registros. "
                    "Esperado ao menos um de {}; o registro tem {}".format(
                        camada.nome, ", ".join(dados), ", ".join(sorted(presentes))))

    def _escreve(self, registros: List[Dict[str, Any]], produto: Produto,
                 fmt: str, extensao: str, camadas: Sequence[Camada]):
        if not registros:
            raise ValueError("records vazio: nao ha o que exportar")
        if fmt == "CSV" and len(camadas) != 1:
            raise ValueError(
                "o CSV e plano e guarda uma estrutura por arquivo; o produto {} pediu "
                "{}".format(produto.chave, len(camadas)))
        self._confere_a_estrutura(registros, camadas)

        diretorio = utils.get_specialist_tmp_dir()
        primeiro = utils.as_utc(registros[0][utils.TIME_FIELD])
        nome = utils.build_output_filename(primeiro, produto.chave, fmt, extensao)
        caminho = os.path.join(diretorio, nome)

        cabecalho = self._cabecalho(nome, registros, produto)
        if fmt == "FITS":
            self._escreve_fits(caminho, registros, cabecalho, produto, camadas)
        else:
            self._escreve_csv(caminho, registros, cabecalho, produto, camadas)

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
    def _documentacao(produto: Produto, camadas: Sequence[Camada]):
        """Bloco de documentação, montado a partir das camadas do arquivo."""
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
        for nota in produto.notas:
            texto = nota if isinstance(nota, str) else nota()
            linhas += ["   " + texto, ""]

        linhas.append("ITEMS - Contains detailed observation data records")
        if len(camadas) > 1:
            linhas.append("   This file carries {} structures, one per table "
                          "extension.".format(len(camadas)))
        for camada in camadas:
            linhas.append("   STRUCTURE {}: {}".format(camada.nome, camada.descricao))
            linhas.extend("      {}: {}".format(coluna.nome, coluna.descricao)
                          for coluna in camada.colunas)
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

    def _escreve_csv(self, caminho: str, registros, cabecalho, produto: Produto,
                     camadas: Sequence[Camada]) -> None:
        colunas = camadas[0].colunas
        with open(caminho, "w", encoding="utf-8", newline="") as arquivo:
            escritor = csv.writer(arquivo, delimiter="|", quoting=csv.QUOTE_MINIMAL)

            escritor.writerow(["#DOCUMENTATION", ""])
            for linha in self._documentacao(produto, camadas):
                escritor.writerow([linha, ""])
            escritor.writerow([])

            escritor.writerow(["#HEADER", ""])
            for chave, valor in cabecalho:
                escritor.writerow([chave, str(valor)])
            escritor.writerow([])

            escritor.writerow(["#ITEMS", ""])
            escritor.writerow([coluna.nome for coluna in colunas])
            for registro in registros:
                escritor.writerow([self._celula(coluna, registro) for coluna in colunas])

    def _escreve_fits(self, caminho: str, registros, cabecalho, produto: Produto,
                      camadas: Sequence[Camada]) -> None:
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
        for linha in self._documentacao(produto, camadas):
            primario.add_comment(linha)

        tabelas = []
        for camada in camadas:
            colunas = [fits.Column(name=coluna.nome, format=coluna.fits,
                                   array=self._vetor(coluna, registros))
                       for coluna in camada.colunas]
            tabelas.append(fits.BinTableHDU.from_columns(
                fits.ColDefs(colunas), name=camada.nome))

        fits.HDUList([fits.PrimaryHDU(header=primario)] + tabelas).writeto(
            caminho, overwrite=True)

    @staticmethod
    def _vetor(coluna: Coluna, registros) -> List[Any]:
        if coluna.tipo == "time":
            return [utils.iso_micro(utils.as_utc(r[utils.TIME_FIELD])) for r in registros]
        if coluna.tipo == "int":
            return [int(r.get(coluna.chave) or 0) for r in registros]
        return [float(r.get(coluna.chave) or 0.0) for r in registros]
