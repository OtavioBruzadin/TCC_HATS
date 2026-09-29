"""
Exportação: registros RAW -> arquivos FITS e CSV.

Escopo desta etapa: os dois métodos RAW do §7.2, que geram "arquivos sem alterar
os dados" e "mantêm resolução original". Os quatro derivados vêm em seguida.

O §11 delimita o que NÃO é nosso: renomear, mover para o NFS, gerar URL e
registrar são responsabilidade exclusiva do pipeline. O plugin escreve num
diretório temporário, devolve o caminho, e acabou.

Uma tabela de colunas para os dois escritores
---------------------------------------------
FITS e CSV saem da mesma lista em `COLUNAS_RAW`. No plugin do POEMAS a lista
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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

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
    "Date and time in ISO 8601 format with microseconds "
    "(YYYY-MM-DDTHH:MM:SS.ssssss), UTC",
    tipo="time")

COLUNAS_RAW: Sequence[Coluna] = (
    COLUNA_DE_TEMPO,
    Coluna("HUSEC", "HUSEC", "K",
           "Hundredths of a millisecond since 0 UT, as stamped by the instrument",
           tipo="int"),
    Coluna("GOLAY", "GOLAY", "D", "Golay cell detector signal (mV)"),
    Coluna("CHOPPER", "CHOPPER", "D", "Chopper reference signal (mV)"),
    Coluna("TEMP_HICS", "TEMP_HICS", "D", "HICS temperature (Celsius)"),
    Coluna("TEMP_ENV", "TEMP_ENV", "D", "Enclosure temperature (Celsius)"),
    Coluna("TEMP_GOLAY", "TEMP_GOLAY", "D", "Golay cell temperature (Celsius)"),
)

# Aviso que viaja com o arquivo, não só com o código. Sem ele, quem baixar o
# produto bruto pode tomar a portadora por série de fluxo.
AVISO_DO_SINAL_PICADO = (
    "CAUTION: the HATS detector is a Golay cell behind a 20 Hz chopper, so GOLAY "
    "is a modulated carrier and not a flux series. Over a 100 ms interval its mean "
    "is about 2 mV while the amplitude of the modulation, which is the physical "
    "quantity, is about 124 mV. Recovering the amplitude requires demodulation.")


class HatsExport(object):
    """Responsabilidade única: gerar FITS e CSV a partir de registros RAW."""

    # ------------------------------------------------- contrato obrigatório

    def GenerateFitsFileRawFormat(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(raw_records, "FITS", "fits")

    def GenerateCSVFileRawFormat(self, raw_records: List[Dict[str, Any]]):
        return self._escreve(raw_records, "CSV", "csv")

    # ------------------------------------------------------------ escritores

    def _escreve(self, registros: List[Dict[str, Any]], fmt: str, extensao: str):
        if not registros:
            raise ValueError("records vazio: nao ha o que exportar")

        diretorio = utils.get_specialist_tmp_dir()
        primeiro = utils.as_utc(registros[0][utils.TIME_FIELD])
        nome = utils.build_output_filename(primeiro, utils.PRODUTO_RAW, fmt, extensao)
        caminho = os.path.join(diretorio, nome)

        cabecalho = self._cabecalho(nome, registros)
        if fmt == "FITS":
            self._escreve_fits(caminho, registros, cabecalho)
        else:
            self._escreve_csv(caminho, registros, cabecalho)

        return TimeSeriesSpecialistGeneratedFile(
            instrument_name=utils.INSTRUMENT_NAME,
            target_date=utils.extract_target_date(registros),
            level="raw",
            scientific_product_key=utils.PRODUTO_RAW,
            product_resolution=utils.RESOLUCAO_RAW,
            generated_file_format=fmt,
            generated_file_full_path=os.path.abspath(caminho),
        )

    @staticmethod
    def _cabecalho(nome: str, registros: List[Dict[str, Any]]):
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
            ("FILERES", utils.RESOLUCAO_RAW),
            ("LEVEL", "raw"),
        ]

    @staticmethod
    def _documentacao():
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
            "   LEVEL = raw: the time series exactly as registered by the instrument",
            "   acquisition system, with no transformation applied.",
            "",
            "   " + AVISO_DO_SINAL_PICADO,
            "",
            "ITEMS - Contains detailed observation data records",
        ]
        linhas.extend("   {}: {}".format(coluna.nome, coluna.descricao)
                      for coluna in COLUNAS_RAW)
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

    def _escreve_csv(self, caminho: str, registros, cabecalho) -> None:
        with open(caminho, "w", encoding="utf-8", newline="") as arquivo:
            escritor = csv.writer(arquivo, delimiter="|", quoting=csv.QUOTE_MINIMAL)

            escritor.writerow(["#DOCUMENTATION", ""])
            for linha in self._documentacao():
                escritor.writerow([linha, ""])
            escritor.writerow([])

            escritor.writerow(["#HEADER", ""])
            for chave, valor in cabecalho:
                escritor.writerow([chave, str(valor)])
            escritor.writerow([])

            escritor.writerow(["#ITEMS", ""])
            escritor.writerow([coluna.nome for coluna in COLUNAS_RAW])
            for registro in registros:
                escritor.writerow([self._celula(coluna, registro) for coluna in COLUNAS_RAW])

    def _escreve_fits(self, caminho: str, registros, cabecalho) -> None:
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
        for linha in self._documentacao():
            primario.add_comment(linha)

        colunas = [fits.Column(name=coluna.nome, format=coluna.fits,
                               array=self._vetor(coluna, registros))
                   for coluna in COLUNAS_RAW]
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
