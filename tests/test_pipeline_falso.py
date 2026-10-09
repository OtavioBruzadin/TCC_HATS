"""
Testes do executor de referência — o `tools/pipeline_falso.py`.

Ele faz o que o §1 do guia descreve que o pipeline faz, e serve para exercitar o
plugin por fora: carregado pelo caminho, com os registros passando pelo banco antes
da exportação, e com o retorno confrontado contra o plano de execução.

Dois testes aqui valem mais que os outros.

`test_a_ciencia_sobrevive_a_volta_pelo_banco` fecha o circuito: a amplitude
publicada DEPOIS da persistência tem de ser exatamente a que o `hats.rbd` produz.
É o único teste do repositório em que o dado atravessa serialização antes de virar
produto.

`TestForaDoRepositorio` roda o executor em subprocesso, com o plugin copiado para
fora do repositório e o PYTHONPATH limpo. É a única forma de testar isso: a suíte
roda da raiz do repositório, então `hats` está sempre importável dentro do processo
de teste, e o defeito que isso esconde é justamente o que aparece em produção.

Os planos de execução aqui são fixtures montadas em Python. O catálogo do
instrumento é documento da equipe de suporte — o §13 diz que não é implementado
pelo plugin e não deve ser alterado pelo desenvolvedor — então não há nenhum no
repositório, nem para teste.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from tests import fixtures

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
TOOLS_DIR = REPO_ROOT / "tools"
for entrada in (str(REPO_ROOT), str(PLUGIN_DIR), str(TOOLS_DIR)):
    if entrada not in sys.path:
        sys.path.insert(0, entrada)

import pipeline_falso                                                        # noqa: E402

from hats import constants, rbd, schema as schema_module                     # noqa: E402

XML_DIR = REPO_ROOT / "XMLTables"
INSTRUMENT_PY = PLUGIN_DIR / "instrument.py"


def um_plano(tarefas, instrumento="HATS"):
    """Plano ad hoc, da mesma forma que a linha de comando monta."""
    return pipeline_falso.catalogo_de_tarefas(tarefas, instrumento)


def um_plano_no_formato_do_catalogo(produtos, fontes, descoberta=None,
                                    modos=None, instrumento="HATS"):
    """
    Um plano com a FORMA do documento deles, montado em Python.

    Fixture, não catálogo: existe dentro do teste, onde fica óbvio o que é.
    """
    return {
        "instrument": instrumento,
        "ingestion": {
            "time_field": "UTC_TIME",
            "native_resolution_modes": modos or {},
            "file_discovery": descoberta or {},
        },
        "export": {"raw_sources": fontes, "products": produtos},
        "publication": {"path_pattern": "{instrument_lower}/{yyyy}/{mm}/{dd}"},
        "plugin": {"source_file": str(INSTRUMENT_PY),
                   "entrypoint": "build_instrument",
                   "adapter_class_name": "HatsInstrument"},
    }


class ExecutorTestCase(unittest.TestCase):

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.root = Path(self._temp.name)
        self.addCleanup(self._temp.cleanup)

        self.dia = fixtures.build_day(self.root, samples=20000)
        self.data_dir = self.dia["day_dir"]
        self.saida = self.root / "saida"

        self._xml_anterior = os.environ.get("HATSXMLPATH")
        os.environ["HATSXMLPATH"] = str(XML_DIR)
        self._tmp_anterior = os.environ.get("CRAAM_SPECIALIST_TMP_ROOT")
        os.environ["CRAAM_SPECIALIST_TMP_ROOT"] = str(self.root / "tmp_plugin")
        self.addCleanup(self._restaura)

    def _restaura(self):
        for chave, valor in (("HATSXMLPATH", self._xml_anterior),
                             ("CRAAM_SPECIALIST_TMP_ROOT", self._tmp_anterior)):
            if valor is None:
                os.environ.pop(chave, None)
            else:
                os.environ[chave] = valor

    def executa(self, plano, limite=None):
        return pipeline_falso.executa(
            plano, self.data_dir, self.saida, INSTRUMENT_PY,
            limite=limite, verboso=False)


# ------------------------------------------------------------------- catálogo

class TestLeituraDoCatalogo(unittest.TestCase):
    """
    O documento deles não é JSON puro: é o dialeto do shell do mongo.

    Se o leitor não aceitar `NumberInt(...)` e companhia, o executor não serve para
    o dia em que o documento do HATS chegar — que é a razão de ele existir.
    """

    def _grava(self, texto):
        arquivo = Path(tempfile.mkdtemp()) / "catalogo.json"
        arquivo.write_text(texto, encoding="utf-8")
        return arquivo

    def test_aceita_o_dialeto_do_mongo(self):
        arquivo = self._grava('''
        {
          "_id": ObjectId("64f0c0ffee0000000000beef"),
          "instrument": "EXEMPLO",
          "updated_at": ISODate("2026-04-29T00:00:00Z"),
          "partitioning": {"target_docs_per_collection": NumberInt(150000000),
                           "sun_hours_per_day": NumberInt(13)}
        }''')
        documento = pipeline_falso.carrega_catalogo(arquivo)
        self.assertEqual(documento["instrument"], "EXEMPLO")
        self.assertEqual(documento["partitioning"]["target_docs_per_collection"],
                         150000000)
        self.assertEqual(documento["_id"], "64f0c0ffee0000000000beef")

    def test_escolhe_entre_documentos_concatenados(self):
        """O arquivo de referência deles traz POEMAS e SST um depois do outro."""
        arquivo = self._grava('{"instrument": "UM"}\n{"instrument": "DOIS"}\n')
        self.assertEqual(
            pipeline_falso.carrega_catalogo(arquivo, "dois")["instrument"], "DOIS")
        with self.assertRaises(SystemExit) as capturado:
            pipeline_falso.carrega_catalogo(arquivo)
        self.assertIn("--instrumento", str(capturado.exception))

    def test_instrumento_inexistente_lista_os_que_ha(self):
        arquivo = self._grava('{"instrument": "UM"}\n{"instrument": "DOIS"}\n')
        with self.assertRaises(SystemExit) as capturado:
            pipeline_falso.carrega_catalogo(arquivo, "HATS")
        mensagem = str(capturado.exception)
        self.assertIn("UM", mensagem)
        self.assertIn("DOIS", mensagem)


class TestSpecMontadoDoCatalogo(ExecutorTestCase):
    """
    §13.2: "o pipeline transforma essas configuracoes em um objeto spec".

    Inclusive a tradução de nome, que é deles: o catálogo declara
    `native_resolution_modes` e o contrato recebe `native_resolutions_by_mode`.
    """

    def setUp(self):
        super().setUp()
        self.modulo, _ = pipeline_falso.carrega_plugin(INSTRUMENT_PY)

    def test_traduz_native_resolution_modes(self):
        plano = um_plano_no_formato_do_catalogo(
            [], [], modos={"DETECTOR": "1ms", "POINTING": "1050ms"})
        spec = pipeline_falso.monta_spec(plano, self.modulo)
        self.assertEqual(spec.native_resolutions_by_mode,
                         {"DETECTOR": "1ms", "POINTING": "1050ms"})
        self.assertEqual(spec.instrument_name, "HATS")
        self.assertEqual(spec.time_field, "UTC_TIME")

    def test_o_formato_de_fonte_unica_vira_o_modo_DEFAULT(self):
        """É o que o POEMAS declara: `native_resolution`, sem modo nenhum."""
        plano = {"instrument": "POEMAS",
                 "ingestion": {"time_field": "UTC_TIME", "native_resolution": "10ms"}}
        spec = pipeline_falso.monta_spec(plano, self.modulo)
        self.assertEqual(spec.native_resolutions_by_mode, {"DEFAULT": "10ms"})

    def test_os_nomes_de_pasta_tem_padrao(self):
        spec = pipeline_falso.monta_spec({"instrument": "HATS"}, self.modulo)
        self.assertEqual(spec.processed_dir_name, "00_processed")
        self.assertEqual(spec.invalid_format_dir_name, "99_formato_invalid")


# ------------------------------------------------------------------ descoberta

class TestDescoberta(ExecutorTestCase):
    """O bloco `ingestion.file_discovery`, aplicado como o pipeline aplicaria."""

    def test_sem_regra_aceita_tudo(self):
        aceitos, recusados = pipeline_falso.descobre_arquivos(self.data_dir, {})
        self.assertEqual(recusados, [])
        self.assertIn("hats-2026-03-17T1800.rbd", [c.name for c in aceitos])

    def test_prefixo_filtra(self):
        aceitos, recusados = pipeline_falso.descobre_arquivos(
            self.data_dir, {"include_prefixes": ["hats-"]})
        for caminho in aceitos:
            self.assertTrue(caminho.name.startswith("hats-"))

    def test_extensao_filtra(self):
        aceitos, _ = pipeline_falso.descobre_arquivos(
            self.data_dir, {"include_extensions": [".rbd", ".aux"]})
        self.assertEqual({c.suffix for c in aceitos}, {".rbd", ".aux"})

    def test_extensao_excluida_e_recusada_com_motivo(self):
        (self.data_dir / "nota.txt").write_text("x", encoding="utf-8")
        _, recusados = pipeline_falso.descobre_arquivos(
            self.data_dir, {"exclude_extensions": [".txt"]})
        nomes = {c.name: motivo for c, motivo in recusados}
        self.assertIn("nota.txt", nomes)
        self.assertIn("excluido", nomes["nota.txt"])

    def test_diretorio_ignorado_nao_e_varrido(self):
        processados = self.data_dir / "00_processed"
        processados.mkdir(exist_ok=True)
        (processados / "hats-2026-03-17T1700.rbd").write_bytes(b"x" * 38)
        aceitos, _ = pipeline_falso.descobre_arquivos(
            self.data_dir, {"scan_ignore_dirs": ["00_processed"]})
        self.assertNotIn("hats-2026-03-17T1700.rbd", [c.name for c in aceitos])


# ---------------------------------------------------------------- persistência

class TestVoltaPeloBanco(ExecutorTestCase):
    """
    A exportação não recebe o que a ingestão produziu: recebe o que voltou do banco.

    O suporte confirmou que a persistência guarda o `UTC_TIME` com precisão de
    milissegundo. Para o HATS, que amostra a 1 ms e carimba em husec, isso é
    exatamente a razão de o `HUSEC` ir no registro como inteiro.
    """

    def test_o_carimbo_perde_o_que_esta_abaixo_do_milissegundo(self):
        registros = [{"UTC_TIME": datetime(2026, 3, 17, 18, 0, 0, 300,
                                           tzinfo=timezone.utc),
                      "HUSEC": 648000003}]
        voltaram = pipeline_falso.persiste_e_recarrega(
            registros, self.root / "banco.jsonl")
        self.assertEqual(voltaram[0]["UTC_TIME"].microsecond, 0)

    def test_o_husec_atravessa_intacto(self):
        """É ele que preserva os 0,1 ms do contador do instrumento."""
        registros = [{"UTC_TIME": datetime(2026, 3, 17, tzinfo=timezone.utc),
                      "HUSEC": 648000003}]
        voltaram = pipeline_falso.persiste_e_recarrega(
            registros, self.root / "banco.jsonl")
        self.assertEqual(voltaram[0]["HUSEC"], 648000003)
        self.assertIsInstance(voltaram[0]["HUSEC"], int)

    def test_os_tipos_numericos_atravessam(self):
        registros = [{"UTC_TIME": datetime(2026, 3, 17, tzinfo=timezone.utc),
                      "GOLAY": 108.50715800000001, "GOLAY_ADCU": -94029,
                      "OPMODE": 7, "FULL_FILE_PATH": "/x/y.rbd"}]
        voltaram = pipeline_falso.persiste_e_recarrega(
            registros, self.root / "banco.jsonl")
        self.assertEqual(voltaram[0]["GOLAY"], 108.50715800000001)
        self.assertEqual(voltaram[0]["GOLAY_ADCU"], -94029)
        self.assertEqual(voltaram[0]["OPMODE"], 7)
        self.assertEqual(voltaram[0]["FULL_FILE_PATH"], "/x/y.rbd")

    def test_a_ciencia_sobrevive_a_volta_pelo_banco(self):
        """
        O teste que fecha o circuito.

        A amplitude publicada pelo executor — depois de os registros terem sido
        serializados, truncados no carimbo e lidos de volta — tem de ser EXATAMENTE
        a que o `hats.rbd` produz, que é o caminho confrontado com o binário do
        CRAAM. Qualquer coisa que a persistência altere e que a demodulação use
        aparece aqui.
        """
        resultado = self.executa(um_plano(["DETECTOR:GenerateCSVFileDeconvFormat32ms"]))
        self.assertEqual(resultado["falhas"], [])
        self.assertEqual(len(resultado["gerados"]), 1)

        esquema = schema_module.load(XML_DIR, "rbd")
        opcoes = {"record_limit": None, "demodulate": True,
                  "window_size": constants.WINDOW_SIZE, "steps": constants.STEPS,
                  "target_frequency": constants.TARGET_FREQUENCY,
                  "sampling_frequency": constants.SAMPLING_FREQUENCY,
                  "bin_mode": "reference"}
        referencia = rbd.analyse_stdlib(self.dia["rbd"], esquema, opcoes)["deconv"]

        _, _, publicado, _ = resultado["gerados"][0]
        linhas = publicado.read_text(encoding="utf-8").splitlines()
        inicio = linhas.index("#ITEMS|") + 2
        husecs, amplitudes = [], []
        for linha in linhas[inicio:]:
            if not linha.strip("|"):
                continue
            partes = linha.split("|")
            husecs.append(int(partes[1]))
            amplitudes.append(partes[2])

        self.assertEqual(husecs, [int(h) for h in referencia[0]])
        self.assertEqual(amplitudes, ["{:.6f}".format(a) for a in referencia[1]])


# -------------------------------------------------------------- carga do plugin

class TestCargaDoPlugin(ExecutorTestCase):
    """§13.2: carregado pelo caminho, sem import tradicional."""

    def test_carrega_e_o_entrypoint_devolve_o_adapter(self):
        _, adapter = pipeline_falso.carrega_plugin(INSTRUMENT_PY)
        self.assertEqual(type(adapter).__name__, "HatsInstrument")

    def test_entrypoint_inexistente_falha_dizendo_qual(self):
        with self.assertRaises(SystemExit) as capturado:
            pipeline_falso.carrega_plugin(INSTRUMENT_PY, "nao_existe")
        self.assertIn("nao_existe", str(capturado.exception))

    def test_arquivo_inexistente_falha(self):
        with self.assertRaises(SystemExit) as capturado:
            pipeline_falso.carrega_plugin(self.root / "nada.py")
        self.assertIn("nao existe", str(capturado.exception))

    def test_o_adapter_declarado_no_plano_e_conferido(self):
        plano = um_plano_no_formato_do_catalogo([], [])
        plano["plugin"]["adapter_class_name"] = "OutraClasse"
        resultado = pipeline_falso.executa(
            plano, self.data_dir, self.saida, INSTRUMENT_PY, verboso=False)
        self.assertTrue(any("adapter_class_name" in f for f in resultado["falhas"]))


# ------------------------------------------------- confronto com o plano

class TestConfrontoComOPlano(ExecutorTestCase):
    """
    O §13.2 avisa que divergência de NOME de método é falha em tempo de execução.
    Os outros quatro campos do retorno têm o mesmo problema, e ninguém os confere.
    """

    def test_metodo_que_nao_existe_e_reportado_e_nao_estoura(self):
        plano = um_plano_no_formato_do_catalogo(
            [{"scientific_product_key": "x", "level": "raw",
              "product_resolution": "1ms", "source_key": "RBD",
              "tasks": [{"format": "CSV", "call": "GenerateCSVFileQueNaoExiste"}]}],
            [{"source_key": "RBD", "mode": "DETECTOR", "resolution": "1ms"}],
            modos={"DETECTOR": "1ms", "POINTING": "1050ms"})
        resultado = self.executa(plano)
        self.assertTrue(any("GenerateCSVFileQueNaoExiste" in f
                            for f in resultado["falhas"]))
        self.assertEqual(resultado["gerados"], [])

    def test_resolucao_divergente_e_apontada(self):
        plano = um_plano_no_formato_do_catalogo(
            [{"scientific_product_key": "raw_1ms", "level": "raw",
              "product_resolution": "10ms",            # o plugin devolve 1ms
              "source_key": "RBD",
              "tasks": [{"format": "CSV", "call": "GenerateCSVFileRawFormat"}]}],
            [{"source_key": "RBD", "mode": "DETECTOR", "resolution": "1ms"}],
            modos={"DETECTOR": "1ms", "POINTING": "1050ms"})
        resultado = self.executa(plano)
        divergencias = " ".join(resultado["falhas"])
        self.assertIn("product_resolution", divergencias)
        self.assertIn("10ms", divergencias)

    def test_chave_de_produto_divergente_e_apontada(self):
        plano = um_plano_no_formato_do_catalogo(
            [{"scientific_product_key": "outro_nome", "level": "raw",
              "product_resolution": "1ms", "source_key": "RBD",
              "tasks": [{"format": "CSV", "call": "GenerateCSVFileRawFormat"}]}],
            [{"source_key": "RBD", "mode": "DETECTOR", "resolution": "1ms"}],
            modos={"DETECTOR": "1ms", "POINTING": "1050ms"})
        resultado = self.executa(plano)
        self.assertTrue(any("scientific_product_key" in f
                            for f in resultado["falhas"]))

    def test_source_key_orfa_e_apontada(self):
        plano = um_plano_no_formato_do_catalogo(
            [{"scientific_product_key": "raw_1ms", "level": "raw",
              "product_resolution": "1ms", "source_key": "NAO_DECLARADA",
              "tasks": [{"format": "CSV", "call": "GenerateCSVFileRawFormat"}]}],
            [{"source_key": "RBD", "mode": "DETECTOR", "resolution": "1ms"}],
            modos={"DETECTOR": "1ms"})
        resultado = self.executa(plano)
        self.assertTrue(any("NAO_DECLARADA" in f for f in resultado["falhas"]))

    def test_o_plano_coerente_nao_tem_divergencia(self):
        plano = um_plano_no_formato_do_catalogo(
            [{"scientific_product_key": "raw_1ms", "level": "raw",
              "product_resolution": "1ms", "source_key": "RBD",
              "tasks": [{"format": "CSV", "call": "GenerateCSVFileRawFormat"},
                        {"format": "FITS", "call": "GenerateFitsFileRawFormat"}]},
             {"scientific_product_key": "raw_pointing_1050ms", "level": "raw",
              "product_resolution": "1050ms", "source_key": "AUX",
              "tasks": [{"format": "CSV", "call": "GenerateCSVFilePointingFormat"}]}],
            [{"source_key": "RBD", "mode": "DETECTOR", "resolution": "1ms"},
             {"source_key": "AUX", "mode": "POINTING", "resolution": "1050ms"}],
            modos={"DETECTOR": "1ms", "POINTING": "1050ms"})
        resultado = self.executa(plano)
        self.assertEqual(resultado["falhas"], [])
        self.assertEqual(len(resultado["gerados"]), 3)


class TestOLevel1NaoCabeNoEsquema(ExecutorTestCase):
    """
    O achado que o executor existe para demonstrar.

    No documento deles um produto aponta para UMA fonte RAW, pelo `source_key`. O
    level 1 precisa das duas — amplitude e temperaturas do `.rbd`, apontamento do
    `.aux`. Declarado do jeito que o esquema permite, ele falha, e a mensagem diz
    o que faltou.
    """

    def test_declarado_sobre_uma_fonte_so_o_level1_falha_explicando(self):
        plano = um_plano_no_formato_do_catalogo(
            [{"scientific_product_key": "level1_100ms", "level": "derived",
              "product_resolution": "100ms", "source_key": "RBD",
              "tasks": [{"format": "CSV", "call": "GenerateCSVFileLevel1Format100ms"}]}],
            [{"source_key": "RBD", "mode": "DETECTOR", "resolution": "1ms"},
             {"source_key": "AUX", "mode": "POINTING", "resolution": "1050ms"}],
            modos={"DETECTOR": "1ms", "POINTING": "1050ms"})
        resultado = self.executa(plano)
        divergencias = " ".join(resultado["falhas"])
        self.assertIn("duas fontes", divergencias)
        self.assertIn("catalogo", divergencias)

    def test_com_as_duas_fontes_juntas_ele_sai(self):
        resultado = self.executa(
            um_plano(["DETECTOR+POINTING:GenerateCSVFileLevel1Format100ms"]))
        self.assertEqual(resultado["falhas"], [])
        self.assertEqual(len(resultado["gerados"]), 1)

    def test_pedir_um_modo_que_nao_foi_ingerido_e_reportado(self):
        resultado = self.executa(um_plano(["METEOROLOGIA:GenerateCSVFileRawFormat"]))
        self.assertTrue(any("METEOROLOGIA" in f for f in resultado["falhas"]))


# ------------------------------------------------- roteamento e publicação

class TestRoteamentoEPublicacao(ExecutorTestCase):

    def test_arquivo_de_formato_recusado_e_roteado_e_nao_derruba_a_execucao(self):
        """
        O pipeline tem uma pasta para isso — `invalid_format_dir_name`. O que se
        exige da recusa do plugin é que ela diga o motivo.
        """
        (self.data_dir / "leia-me.txt").write_text("nota", encoding="utf-8")
        resultado = self.executa(um_plano(["DETECTOR:GenerateCSVFileRawFormat"]))
        roteados = {c.name: motivo for c, motivo in resultado["formato_invalido"]}
        self.assertIn("leia-me.txt", roteados)
        self.assertIn("fonte RAW", roteados["leia-me.txt"])
        self.assertEqual(resultado["falhas"], [])

    def test_o_arquivo_sai_do_diretorio_temporario_do_plugin(self):
        """
        §11: renomear e mover é do pipeline. Se o plugin dependesse do caminho onde
        escreveu, ou do nome que deu, quebraria aqui.
        """
        resultado = self.executa(um_plano(["DETECTOR:GenerateCSVFileRawFormat"]))
        _, gerado, publicado, _ = resultado["gerados"][0]
        self.assertFalse(Path(gerado.generated_file_full_path).exists())
        self.assertTrue(publicado.exists())
        self.assertGreater(publicado.stat().st_size, 0)

    def test_o_caminho_publicado_segue_o_padrao_do_plano(self):
        plano = um_plano_no_formato_do_catalogo(
            [{"scientific_product_key": "raw_1ms", "level": "raw",
              "product_resolution": "1ms", "source_key": "RBD",
              "tasks": [{"format": "CSV", "call": "GenerateCSVFileRawFormat"}]}],
            [{"source_key": "RBD", "mode": "DETECTOR", "resolution": "1ms"}],
            modos={"DETECTOR": "1ms"})
        resultado = self.executa(plano)
        _, _, publicado, _ = resultado["gerados"][0]
        self.assertEqual(publicado.parent.relative_to(self.saida / "publicado"),
                         Path("hats/2026/03/17"))


# --------------------------------------------------------- fora do repositório

class TestForaDoRepositorio(unittest.TestCase):
    """
    O plugin carregado de fora do repositório, em subprocesso com PYTHONPATH limpo.

    Tem de ser subprocesso: a suíte roda da raiz do repositório, então o pacote
    `hats` está sempre importável dentro deste processo, e é exatamente isso que
    esconde o defeito. Em produção o pipeline carrega de `/app/plugins/hats/` e o
    repositório não existe.
    """

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.fora = Path(self._temp.name) / "app" / "plugins" / "hats"
        self.addCleanup(self._temp.cleanup)
        shutil.copytree(PLUGIN_DIR, self.fora,
                        ignore=shutil.ignore_patterns("__pycache__"))

        self.dados = Path(self._temp.name) / "dados"
        self.dados.mkdir()
        self.dia = fixtures.build_day(self.dados, samples=2000)

    def _roda(self, pythonpath):
        ambiente = {"PATH": os.environ.get("PATH", ""),
                    "HATSXMLPATH": str(XML_DIR),
                    "CRAAM_SPECIALIST_TMP_ROOT": str(Path(self._temp.name) / "tmp")}
        if pythonpath:
            ambiente["PYTHONPATH"] = pythonpath
        return subprocess.run(
            [sys.executable, str(TOOLS_DIR / "pipeline_falso.py"),
             "--data-dir", str(self.dia["day_dir"]),
             "--saida", str(Path(self._temp.name) / "saida"),
             "--source-file", str(self.fora / "instrument.py"),
             "--tarefas", "DETECTOR:GenerateCSVFileRawFormat"],
            capture_output=True, text=True, env=ambiente, cwd=self._temp.name)

    def test_sem_a_dependencia_no_ambiente_falha_explicando(self):
        concluido = self._roda(pythonpath=None)
        self.assertNotEqual(concluido.returncode, 0)
        saida = concluido.stdout + concluido.stderr
        self.assertIn("`hats`", saida)
        self.assertIn("PYTHONPATH", saida)
        self.assertNotIn("Traceback", saida)

    def test_com_a_dependencia_no_ambiente_roda_de_qualquer_lugar(self):
        """
        O requisito é o do catálogo — a dependência no ambiente do job —, e atendido
        ele, o diretório do plugin pode estar em qualquer lugar. Nada precisa ser
        empacotado: o pacote `hats` é Python puro.
        """
        concluido = self._roda(pythonpath=str(REPO_ROOT))
        self.assertEqual(concluido.returncode, 0,
                         concluido.stdout + concluido.stderr)
        self.assertIn("1 arquivos publicados", concluido.stdout)

    def test_o_executor_recusa_rodar_sem_plano(self):
        ambiente = {"PATH": os.environ.get("PATH", ""),
                    "PYTHONPATH": str(REPO_ROOT)}
        concluido = subprocess.run(
            [sys.executable, str(TOOLS_DIR / "pipeline_falso.py"),
             "--data-dir", str(self.dia["day_dir"])],
            capture_output=True, text=True, env=ambiente, cwd=self._temp.name)
        self.assertNotEqual(concluido.returncode, 0)
        saida = concluido.stdout + concluido.stderr
        self.assertIn("--catalogo", saida)
        self.assertIn("suporte", saida)


class TestNaoHaCatalogoNoRepositorio(unittest.TestCase):
    """
    §13: o catálogo não é implementado pelo plugin e não deve ser alterado pelo
    desenvolvedor. Este teste existe para que ninguém o adicione por conveniência.
    """

    def test_o_repositorio_nao_tem_documento_de_catalogo(self):
        encontrados = [caminho for caminho in REPO_ROOT.rglob("*catalog*.json")
                       if ".refvenv" not in caminho.parts
                       and "node_modules" not in caminho.parts]
        self.assertEqual(encontrados, [], "o catalogo e documento do suporte")


if __name__ == "__main__":
    unittest.main()
