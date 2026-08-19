"""
Extração da amplitude do sinal picado, em 20 Hz.

A célula Golay só responde a radiação modulada, então o feixe é picado por um
chopper a 20 Hz e amostrado a 1 kHz. O sinal científico é a amplitude dessa
modulação ao longo do tempo — um lock-in feito em software.

O CRAAM faz isso no `HATS_fft.c`, com janela flat-top e o algoritmo de Goertzel
sobre janelas de 128 amostras deslocando de 32 em 32, o que dá uma saída a
31,25 Hz. Aqui o resultado é o mesmo, calculado como uma DFT de bin único, que é
matematicamente equivalente e bem mais rápida em Python.

Divergências conhecidas em relação ao código C
----------------------------------------------
1. O `windowed_dft.c` encerra a recursão de Goertzel um passo antes do devido:
   devolve s[N-2] e s[N-3] no lugar de s[N-1] e s[N-2]. Medido contra uma DFT
   direta, isso vale 0,003%. Aqui usa-se a forma correta, e é essa a única
   diferença numérica entre este pipeline e o do CRAAM.
2. O bin é escolhido com floor(f*N/fs), o que dá 2 e não 2,56 — ou seja
   15,625 Hz em vez de 20 Hz. A janela flat-top tem lóbulo principal largo e
   absorve quase todo o erro, mas sobra um scalloping dependente de fase. O modo
   'reference' reproduz esse comportamento; 'exact' usa o bin fracionário.
"""

import math
import operator

from hats import constants


def flattop_window(length):
    """Janela flat-top simétrica, ISO 18431-1 tipo 0, como no windowed_dft.c."""
    if length < 2:
        return [constants.FLATTOP_CORRECTION] * length
    denominator = float(length - 1)
    window = []
    for index in range(length):
        weight = (1.0
                  - 1.9330 * math.cos(2.0 * math.pi * index / denominator)
                  + 1.2860 * math.cos(4.0 * math.pi * index / denominator)
                  - 0.3880 * math.cos(6.0 * math.pi * index / denominator)
                  + 0.0322 * math.cos(8.0 * math.pi * index / denominator))
        window.append(constants.FLATTOP_CORRECTION * weight)
    return window


def frequency_bin(target_frequency, window_size, sampling_frequency, bin_mode):
    """
    Bin de análise. 'reference' reproduz o floor() do HATS_fft.c.

    Nos parâmetros nominais o bin exato é 20*128/1000 = 2,56 e o floor dá 2. Com
    seno sintético de 100 mV, o modo 'reference' recupera 99,018 mV com desvio de
    0,104 entre janelas, e o 'exact' recupera 99,223 mV com desvio de 0,0016.
    Em sinal real a dispersão do 'exact' é maior, porque o sinal picado não é
    monocromático e o bin exato acompanha a variação em vez de suavizá-la.
    """
    exact = target_frequency * window_size / sampling_frequency
    if bin_mode == "exact":
        return exact
    return float(math.floor(exact))


def projection_vectors(window_size, target_frequency, sampling_frequency, bin_mode):
    """
    Janela flat-top já multiplicada pelos twiddles de análise.

    Deixa a amplitude de cada janela como dois produtos internos, que
    `sum(map(mul, ...))` avalia em C em vez de no interpretador. Cerca de três
    vezes mais rápido que a recursão, com concordância de 3e-14.
    """
    window = flattop_window(window_size)
    angle = 2.0 * math.pi * frequency_bin(
        target_frequency, window_size, sampling_frequency, bin_mode) / window_size
    cosines = [window[i] * math.cos(angle * i) for i in range(window_size)]
    sines = [window[i] * math.sin(angle * i) for i in range(window_size)]
    return cosines, sines


def numpy_projection(window_size, target_frequency, sampling_frequency, bin_mode):
    """Mesma projeção, como vetor complexo, para a multiplicação matriz-vetor."""
    import numpy as np

    indices = np.arange(window_size, dtype=np.float64)
    denominator = float(window_size - 1) if window_size > 1 else 1.0
    window = constants.FLATTOP_CORRECTION * (
        1.0
        - 1.9330 * np.cos(2.0 * np.pi * indices / denominator)
        + 1.2860 * np.cos(4.0 * np.pi * indices / denominator)
        - 0.3880 * np.cos(6.0 * np.pi * indices / denominator)
        + 0.0322 * np.cos(8.0 * np.pi * indices / denominator))
    k = frequency_bin(target_frequency, window_size, sampling_frequency, bin_mode)
    return window * np.exp(-1j * 2.0 * np.pi * k * indices / window_size)


def window_count(total_samples, window_size, steps):
    """
    Quantas janelas o HATS_fft.c produziria: floor((N - janela + 1) / passo).

    A fórmula descarta a última janela válida. Mantida como está para as saídas
    terem o mesmo comprimento das do CRAAM.
    """
    if total_samples < window_size or steps <= 0:
        return 0
    return max(0, (total_samples - window_size + 1) // steps)


def goertzel_amplitude(signal, offset, window, coefficient, window_size):
    """
    Recursão de Goertzel sobre signal[offset:offset+window_size].

    Mantida como implementação de referência e para testes; o caminho de produção
    usa os produtos internos de projection_vectors(), que dão o mesmo resultado.
    """
    previous = 0.0
    current = 0.0
    for index in range(window_size):
        latest = window[index] * signal[offset + index] + coefficient * current - previous
        previous = current
        current = latest
    return math.sqrt(max(0.0, current * current + previous * previous
                         - coefficient * current * previous))


class SlidingDemodulator(object):
    """
    Demodula janelas conforme as amostras chegam, sem guardar o arquivo inteiro.

    Recebe blocos, emite toda janela que já esteja inteiramente disponível e
    descarta o prefixo consumido, então a memória não acompanha o tamanho do
    arquivo. O `husec` de cada janela é o do seu centro, como no HATS_fft.c.
    """

    def __init__(self, window_size, steps, target_frequency, sampling_frequency,
                 bin_mode, max_windows):
        self.window_size = window_size
        self.steps = steps
        self.half = window_size // 2
        self.max_windows = max_windows
        self.cosines, self.sines = projection_vectors(
            window_size, target_frequency, sampling_frequency, bin_mode)
        self.amplitudes = []
        self.husecs = []
        self._signal = []
        self._husec = []
        self._consumed = 0
        self._next_start = 0

    def feed(self, signal_block, husec_block, total_samples_read):
        """Absorve um bloco e emite as janelas que ficaram completas."""
        self._signal.extend(signal_block)
        self._husec.extend(husec_block)

        multiply = operator.mul
        hypot = math.hypot
        cosines = self.cosines
        sines = self.sines
        window_size = self.window_size
        last_start = total_samples_read - window_size

        while len(self.amplitudes) < self.max_windows and self._next_start <= last_start:
            local = self._next_start - self._consumed
            piece = self._signal[local:local + window_size]
            self.amplitudes.append(hypot(sum(map(multiply, cosines, piece)),
                                         sum(map(multiply, sines, piece))) / window_size)
            self.husecs.append(self._husec[local + self.half])
            self._next_start += self.steps

        if self._next_start > self._consumed:
            drop = self._next_start - self._consumed
            del self._signal[:drop]
            del self._husec[:drop]
            self._consumed = self._next_start


def demodulate(signal, husec, window_size=constants.WINDOW_SIZE, steps=constants.STEPS,
               target_frequency=constants.TARGET_FREQUENCY,
               sampling_frequency=constants.SAMPLING_FREQUENCY,
               bin_mode="reference"):
    """
    Demodula uma série já inteira em memória.

    Conveniência para comparações e testes; o pipeline usa SlidingDemodulator.
    Devolve (husec das janelas, amplitudes), já divididas pelo comprimento da
    janela, ou seja na mesma unidade da entrada.
    """
    total = len(signal)
    count = window_count(total, window_size, steps)
    if not count:
        return [], []

    cosines, sines = projection_vectors(window_size, target_frequency,
                                        sampling_frequency, bin_mode)
    multiply = operator.mul
    hypot = math.hypot
    half = window_size // 2
    samples = signal if isinstance(signal, list) else list(signal)

    amplitudes = []
    husecs = []
    for index in range(count):
        offset = index * steps
        piece = samples[offset:offset + window_size]
        amplitudes.append(hypot(sum(map(multiply, cosines, piece)),
                                sum(map(multiply, sines, piece))) / window_size)
        husecs.append(husec[offset + half])
    return husecs, amplitudes
