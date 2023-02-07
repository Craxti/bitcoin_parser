from selenium import webdriver
import numpy as np
import scipy.interpolate as si

def get_options(is_firefox=False, is_headless=False):
    '''Получаем необходимую конфигурацию для использования в браузере'''

    options = None

    if is_firefox:
        options = webdriver.FirefoxOptions()
    else:
        # [!] Chrome не запускает расширения в --headlless моде
        # [!] add_experimental_option имеется только у chrome
        options = webdriver.ChromeOptions()
        options.add_experimental_option('useAutomationExtension', False)
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])

    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument("--disable-infobars")

    if is_headless: 
        options.add_argument("--headless")

    return options

def force_exit(browser):
    '''Завершение и закрытие браузеры и отправки SystemExit (используется для полного завершения скрипта)'''
    print('[!] Принудительное завершение работы')
    browser.destroy()

    raise SystemExit

def curve_base():
    points = [[0, 0], [0, 2], [2, 3], [4, 0], [6, 3], [8, 2], [8, 0]];
    points = np.array(points)

    x = points[:,0]
    y = points[:,1]

    t = range(len(points))
    ipl_t = np.linspace(0.0, len(points) - 1, 100)

    x_tup = si.splrep(t, x, k=3)
    y_tup = si.splrep(t, y, k=3)

    x_list = list(x_tup)
    xl = x.tolist()
    x_list[1] = xl + [0.0, 0.0, 0.0, 0.0]

    y_list = list(y_tup)
    yl = y.tolist()
    y_list[1] = yl + [0.0, 0.0, 0.0, 0.0]

    x_i = si.splev(ipl_t, x_list)
    y_i = si.splev(ipl_t, y_list)

    return x_i, y_i