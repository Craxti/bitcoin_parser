from selenium import webdriver

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