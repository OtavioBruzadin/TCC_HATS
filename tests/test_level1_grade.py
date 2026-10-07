"""
Testes da grade uniforme e das duas interpolações — a conta, sem arquivo.

Cada um compara contra uma resposta que se sabe de antemão: uma reta interpolada
linearmente tem de devolver a própria reta, e o vizinho mais próximo de um
identificador tem de devolver um identificador que existe na entrada.
"""

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = REPO_ROOT / "plugins" / "hats"
if str(PLUGIN_DIR) not in sys.path:
    sys.path.insert(0, str(PLUGIN_DIR))

import hats_level1 as level1                                                 # noqa: E402


class TestGrade(unittest.TestCase):

    def test_os_pontos_sao_multiplos_do_passo_desde_0_UT(self):
        pontos = level1.grade(1234, 9876, 1000)
        self.assertEqual(pontos, [2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000])
        for ponto in pontos:
            self.assertEqual(ponto % 1000, 0)

    def test_nao_passa_dos_limites(self):
        pontos = level1.grade(2000, 4000, 1000)
        self.assertEqual(pontos[0], 2000)
        self.assertEqual(pontos[-1], 4000)

    def test_a_ancoragem_em_0_UT_alinha_horas_consecutivas(self):
        """
        Duas horas seguidas, cada uma com seu husec inicial arbitrário: as grades
        caem nos mesmos instantes, não se sobrepõem e não repetem nada.

        O instante 72000000 não aparece em nenhuma das duas porque nenhum dos dois
        arquivos o cobre — a primeira hora termina 7 husec antes dele e a segunda
        começa 4 husec depois. É a regra de não extrapolar, e não desalinhamento.
        """
        passo = 10000                                     # 1 s
        uma = level1.grade(36000007, 71999993, passo)      # hora 1, husec quebrado
        outra = level1.grade(72000004, 107999991, passo)   # hora 2, outro quebrado

        self.assertFalse(set(uma).intersection(outra))
        for ponto in uma + outra:
            self.assertEqual(ponto % passo, 0)
        self.assertEqual(outra[0] - uma[-1], 2 * passo)

    def test_intervalo_curto_demais_nao_tem_ponto(self):
        self.assertEqual(level1.grade(1001, 1999, 1000), [])

    def test_passo_invalido_falha(self):
        for passo in (0, -1000):
            with self.subTest(passo=passo):
                with self.assertRaises(ValueError):
                    level1.grade(0, 1000, passo)

    def test_intervalo_invertido_falha(self):
        with self.assertRaises(ValueError) as capturado:
            level1.grade(5000, 1000, 1000)
        self.assertIn("vem antes", str(capturado.exception))


class TestInterpolacaoLinear(unittest.TestCase):

    def test_uma_reta_interpolada_devolve_a_reta(self):
        """Resposta conhecida: y = 3x + 7 em qualquer ponto é 3x + 7."""
        nos_x = [0, 10, 20, 30, 40]
        nos_y = [3.0 * x + 7.0 for x in nos_x]
        alvos = [0, 5, 7, 12, 25, 33, 40]
        obtido = level1.interpola_linear(nos_x, nos_y, alvos)
        for alvo, valor in zip(alvos, obtido):
            self.assertAlmostEqual(valor, 3.0 * alvo + 7.0, places=12)

    def test_no_no_devolve_o_valor_do_no(self):
        nos_x = [0, 10, 20]
        nos_y = [1.5, -2.5, 100.0]
        self.assertEqual(level1.interpola_linear(nos_x, nos_y, [0, 10, 20]), nos_y)

    def test_o_ponto_medio_e_a_media_dos_vizinhos(self):
        obtido = level1.interpola_linear([0, 100], [10.0, 20.0], [50])
        self.assertAlmostEqual(obtido[0], 15.0)

    def test_extrapolar_falha(self):
        for alvo in (-1, 101):
            with self.subTest(alvo=alvo):
                with self.assertRaises(ValueError) as capturado:
                    level1.interpola_linear([0, 100], [1.0, 2.0], [alvo])
                self.assertIn("extrapolar", str(capturado.exception))

    def test_o_cursor_nao_volta_mas_acerta_alvos_espalhados(self):
        """
        A varredura é O(nós + alvos) porque o cursor não retrocede. Se isso
        estivesse errado, alvos distantes entre si sairiam presos ao primeiro par
        de nós.
        """
        nos_x = list(range(0, 10001, 10))
        nos_y = [float(x) for x in nos_x]
        alvos = [5, 2505, 7495, 9999]
        obtido = level1.interpola_linear(nos_x, nos_y, alvos)
        for alvo, valor in zip(alvos, obtido):
            self.assertAlmostEqual(valor, float(alvo), places=9)


class TestInterpolacaoVizinho(unittest.TestCase):

    def test_so_devolve_valor_que_existe_na_entrada(self):
        nos_x = [0, 10, 20, 30]
        nos_y = [7, 8, 7, 11]
        obtido = level1.interpola_vizinho(nos_x, nos_y, [0, 3, 6, 11, 17, 24, 30])
        self.assertTrue(set(obtido).issubset(set(nos_y)), obtido)

    def test_pega_o_mais_proximo(self):
        obtido = level1.interpola_vizinho([0, 100], [7, 8], [10, 90])
        self.assertEqual(obtido, [7, 8])

    def test_empate_vai_para_o_no_anterior(self):
        obtido = level1.interpola_vizinho([0, 100], [7, 8], [50])
        self.assertEqual(obtido, [7])

    def test_nao_produz_media_entre_identificadores(self):
        """7 e 8 são modos de operação; 7,5 não é modo de operação nenhum."""
        obtido = level1.interpola_vizinho([0, 100], [7, 8], [49, 51])
        self.assertEqual(obtido, [7, 8])


class TestVerificaNos(unittest.TestCase):

    def test_um_no_so_nao_serve(self):
        with self.assertRaises(ValueError) as capturado:
            level1.verifica_nos("teste", [10])
        self.assertIn("dois nos", str(capturado.exception))

    def test_no_duplicado_falha(self):
        with self.assertRaises(ValueError) as capturado:
            level1.verifica_nos("teste", [10, 20, 20, 30])
        self.assertIn("fora de ordem", str(capturado.exception))

    def test_no_decrescente_falha(self):
        with self.assertRaises(ValueError):
            level1.verifica_nos("teste", [10, 30, 20])

    def test_crescente_passa(self):
        level1.verifica_nos("teste", [10, 20, 30])


if __name__ == "__main__":
    unittest.main()
