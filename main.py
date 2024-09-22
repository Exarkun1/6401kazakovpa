import numpy as np
import sys
import typing as tp
import numpy.typing as ntp

def read_properties(path : str) -> dict[str, float]:
    '''
    Функция для чтения параметров из yaml файла.

    Принимаемые параметры:
    path - строка с путём к файлу с параметрами.

    Возвращаемые значения:
    properties - словарь с параметрами из файла.
    '''
    file = open(path, 'r')
    property_lines = file.readlines()
    file.close()
    properties = {}
    for line in property_lines:
        entry = line.split(':') 
        key = entry[0].replace(' ', '')
        value = float(entry[1].replace(' ', ''))
        properties[key] = value
    return properties

def write_results(path : str, arr : tp.Iterable[float]) -> None:
    '''
    Функция для записи резулятатов вычислений в файл.

    Принимаемые параметры:
    path - строка с путём к файлу для записи.
    arr - итератор по множеству результата.
    '''
    file = open(path, 'w')
    for elem in arr:
        file.write(str(elem) + '\n')
    file.close()

def function_init(a : float, b : float, c : float) -> tp.Callable[[ntp.NDArray[np.float64]], ntp.NDArray[np.float64]]:
    '''
    Функция для инициализации функции из варианта параметрами a, b, c.

    Принимаемые параметры:
    a, b, c - параметры функции из варианта.

    Возвращаемое значение:
    Инициализированная параметрами функция, зависящая только от x.
    '''
    return lambda x: a*np.exp(-b*x**2 + c*x)

def config_properties() -> tuple[float, ...]:
    '''
    Функция для получения параметров из файла config.yaml

    Возвращаемое значение:
    Кортеж из параметров n0, h, nk, a, b, c
    '''
    properties = read_properties("config.yaml")
    n0 = properties['n0']
    h = properties['h']
    nk = properties['nk']
    a = properties['a']
    b = properties['b']
    c = properties['c']
    return (n0, h, nk, a, b, c)

def params_properties(params : list[str]) -> tuple[float, ...]:
    '''
    Функция для получения параметров из массива строк.

    Принимаемые значения:
    params - cписок параметрок в виде строк.    

    Возвращаемое значение:
    Кортеж из параметров n0, h, nk, a, b, c.
    '''
    n0 = float(params[0])
    h = float(params[1])
    nk = float(params[2])
    a = float(params[3])
    b = float(params[4])
    c = float(params[5])
    return (n0, h, nk, a, b, c)

def calculate_and_write(n0 : float, h : float, nk : float, a : float, b : float, c : float) -> None:
    '''
    Функция для вычисления результа и записи в файл results.txt.

    Принимаемые значения:
    n0, h, nk, a, b, c - параметры для вычисления значений функции.
    '''
    n = int((nk - n0) / h)
    x = np.linspace(n0, nk, n)
    y = function_init(a, b, c)
    write_results("results.txt", y(x))

def run_with_config() -> None:
    '''
    Функция для запуска программы с параметрами из файла конфигурации.
    '''
    properties = config_properties()
    calculate_and_write(*properties)

def run_with_params(params : list[str]) -> None:
    '''
    Функция для запуска программы с параметрами из консоли.

    Принимаемые значения:
    params - cписок параметрок в виде строк.
    '''
    properties = params_properties(params)
    calculate_and_write(*properties)

# Точка входа в программу
if __name__ == '__main__':
    if len(sys.argv) == 1:
        run_with_config()
    elif len(sys.argv) == 7:
        run_with_params(sys.argv[1:])