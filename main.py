import numpy as np
import sys

def read_properties(path):
    '''Чтение параметров из файла'''
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

def write_results(path, arr):
    '''Запись результатов'''
    file = open(path, 'w')
    for elem in arr:
        file.write(str(elem) + '\n')
    file.close()
        

def function_init(a, b, c):
    '''Инициализация функции параметрами a, b, c'''
    return lambda x: a*np.exp(-b*x**2 + c*x)

def config_properties():
    '''Получение параметров из файла'''
    properties = read_properties("config.yaml")
    n0 = properties['n0']
    h = properties['h']
    nk = properties['nk']
    a = properties['a']
    b = properties['b']
    c = properties['c']
    return (n0, h, nk, a, b, c)

def params_properties(params):
    '''Получение параметров из массива'''
    n0 = float(params[0])
    h = float(params[1])
    nk = float(params[2])
    a = float(params[3])
    b = float(params[4])
    c = float(params[5])
    return (n0, h, nk, a, b, c)

def calculate_and_write(n0, h, nk, a, b, c):
    '''Вычисление результата и записть в файл'''
    n = int((nk - n0) / h)
    x = np.linspace(n0, nk, n)
    y = function_init(a, b, c)
    write_results("results.txt", y(x))

def run_with_config():
    '''Запуск программы с параметрами из файла конфигурации'''
    properties = config_properties()
    calculate_and_write(*properties)

def run_with_params(params):
    '''Запуск программы с параметрами из консоли'''
    properties = params_properties(params)
    calculate_and_write(*properties)

# Точка входа в программу
if __name__ == '__main__':
    if len(sys.argv) == 1:
        run_with_config()
    elif len(sys.argv) == 7:
        run_with_params(sys.argv[1:])