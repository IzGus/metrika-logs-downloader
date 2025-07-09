import requests
import time
import csv
import os
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

# Добавляем после импортов, перед основным кодом
def format_date(date_str):
    """
    Форматирует дату в строку формата YYYY-MM-DD.
    Поддерживает форматы: YYYY-MM-DD, today, yesterday, NdaysAgo
    """
    if isinstance(date_str, datetime):
        return date_str.strftime("%Y-%m-%d")
    elif date_str == "today":
        return datetime.now().strftime("%Y-%m-%d")
    elif date_str == "yesterday":
        return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    elif isinstance(date_str, str) and date_str.endswith("daysAgo"):
        try:
            days = int(date_str.replace("daysAgo", ""))
            return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        except ValueError:
            return date_str
    return date_str

# Настройка логирования
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, f'metrika_{datetime.now().strftime("%Y%m%d")}.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы для задержек
API_DELAY = 1.0      # Базовая задержка 1 секунда между запросами
RETRY_DELAY = 10.0   # Увеличенная задержка до 10 секунд при повторе
MAX_RETRIES = 3      # Максимальное количество попыток

# Настройка повторных попыток для requests
retry_strategy = Retry(
    total=3,
    backoff_factor=3,  # Увеличиваем множитель для экспоненциальной задержки
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)

# Константы для атрибуции
ATTRIBUTION_TYPES = {
    'last': 'Последний переход',
    'first': 'Первый переход',
    'last_significant': 'Последний значимый переход',
    'last_yandex_direct': 'Последний переход из Яндекс.Директа',
    'first_yandex_direct': 'Первый переход из Яндекс.Директа'
}

# Константы для метрик
VISITS_METRICS = [
    # Базовые параметры визита
    "ym:s:visitID", "ym:s:counterID", "ym:s:watchIDs", "ym:s:date", "ym:s:dateTime",
    "ym:s:dateTimeUTC", "ym:s:isNewUser", "ym:s:startURL", "ym:s:endURL",
    "ym:s:pageViews", "ym:s:visitDuration", "ym:s:bounce", "ym:s:ipAddress",
    
    # Геоданные
    "ym:s:regionCountry", "ym:s:regionCity", "ym:s:regionCountryID", 
    "ym:s:regionCityID", 
    
    # Идентификация пользователя
    "ym:s:clientID", "ym:s:counterUserIDHash", "ym:s:networkType",
    
    # Цели и конверсии
    "ym:s:goalsID", "ym:s:goalsSerialNumber", "ym:s:goalsDateTime",
    "ym:s:goalsPrice", "ym:s:goalsOrder", "ym:s:goalsCurrency",
    
    # Источники трафика
    "ym:s:<attribution>TrafficSource", "ym:s:<attribution>AdvEngine",
    "ym:s:<attribution>ReferalSource", "ym:s:<attribution>SearchEngineRoot",
    "ym:s:<attribution>SearchEngine", "ym:s:<attribution>SocialNetwork",
    "ym:s:<attribution>SocialNetworkProfile", "ym:s:referer",
    
    # Яндекс.Директ
    "ym:s:<attribution>DirectClickOrder", "ym:s:<attribution>DirectBannerGroup",
    "ym:s:<attribution>DirectClickBanner", "ym:s:<attribution>DirectClickOrderName",
    "ym:s:<attribution>ClickBannerGroupName", "ym:s:<attribution>DirectClickBannerName",
    "ym:s:<attribution>DirectPhraseOrCond", "ym:s:<attribution>DirectPlatformType",
    "ym:s:<attribution>DirectPlatform", "ym:s:<attribution>DirectConditionType",
    "ym:s:<attribution>CurrencyID",
    
    # Параметры рекламы
    "ym:s:from", "ym:s:<attribution>UTMCampaign", "ym:s:<attribution>UTMContent",
    "ym:s:<attribution>UTMMedium", "ym:s:<attribution>UTMSource", "ym:s:<attribution>UTMTerm",
    "ym:s:<attribution>openstatAd", "ym:s:<attribution>openstatCampaign",
    "ym:s:<attribution>openstatService", "ym:s:<attribution>openstatSource",
    "ym:s:<attribution>hasGCLID", "ym:s:<attribution>GCLID",
    
    # Параметры браузера и устройства
    "ym:s:browserLanguage", "ym:s:browserCountry", "ym:s:clientTimeZone",
    "ym:s:deviceCategory", "ym:s:mobilePhone", "ym:s:mobilePhoneModel",
    "ym:s:operatingSystemRoot", "ym:s:operatingSystem", "ym:s:browser",
    "ym:s:browserMajorVersion", "ym:s:browserMinorVersion", "ym:s:browserEngine",
    "ym:s:browserEngineVersion1", "ym:s:browserEngineVersion2",
    "ym:s:browserEngineVersion3", "ym:s:browserEngineVersion4",
    "ym:s:cookieEnabled", "ym:s:javascriptEnabled",
    
    # Параметры экрана
    "ym:s:screenFormat", "ym:s:screenColors", "ym:s:screenOrientation",
    "ym:s:screenOrientationName", "ym:s:screenWidth", "ym:s:screenHeight",
    "ym:s:physicalScreenWidth", "ym:s:physicalScreenHeight",
    "ym:s:windowClientWidth", "ym:s:windowClientHeight",
    
    # Электронная коммерция
    "ym:s:purchaseID", "ym:s:purchaseDateTime", "ym:s:purchaseAffiliation",
    "ym:s:purchaseRevenue", "ym:s:purchaseTax", "ym:s:purchaseShipping",
    "ym:s:purchaseCoupon", "ym:s:purchaseCurrency", "ym:s:purchaseProductQuantity",
    
    # События с товарами
    "ym:s:eventsProductID", "ym:s:eventsProductList", "ym:s:eventsProductBrand",
    "ym:s:eventsProductCategory", "ym:s:eventsProductCategory1",
    "ym:s:eventsProductCategory2", "ym:s:eventsProductCategory3",
    "ym:s:eventsProductCategory4", "ym:s:eventsProductCategory5",
    "ym:s:eventsProductVariant", "ym:s:eventsProductPosition",
    "ym:s:eventsProductPrice", "ym:s:eventsProductCurrency",
    "ym:s:eventsProductCoupon", "ym:s:eventsProductQuantity",
    "ym:s:eventsProductEventTime", "ym:s:eventsProductType",
    "ym:s:eventsProductDiscount", "ym:s:eventsProductName",
    
    # Купленные товары
    "ym:s:productsPurchaseID", "ym:s:productsID", "ym:s:productsName",
    "ym:s:productsBrand", "ym:s:productsCategory", "ym:s:productsCategory1",
    "ym:s:productsCategory2", "ym:s:productsCategory3", "ym:s:productsCategory4",
    "ym:s:productsCategory5", "ym:s:productsVariant", "ym:s:productsPosition",
    "ym:s:productsPrice", "ym:s:productsCurrency", "ym:s:productsCoupon",
    "ym:s:productsQuantity", "ym:s:productsList", "ym:s:productsEventTime",
    "ym:s:productsDiscount",
    
    # Показы товаров
    "ym:s:impressionsURL", "ym:s:impressionsDateTime", "ym:s:impressionsProductID",
    "ym:s:impressionsProductName", "ym:s:impressionsProductBrand",
    "ym:s:impressionsProductCategory", "ym:s:impressionsProductCategory1",
    "ym:s:impressionsProductCategory2", "ym:s:impressionsProductCategory3",
    "ym:s:impressionsProductCategory4", "ym:s:impressionsProductCategory5",
    "ym:s:impressionsProductVariant", "ym:s:impressionsProductPrice",
    "ym:s:impressionsProductCurrency", "ym:s:impressionsProductCoupon",
    "ym:s:impressionsProductList", "ym:s:impressionsProductQuantity",
    "ym:s:impressionsProductEventTime", "ym:s:impressionsProductDiscount",
    
    # Промо-акции
    "ym:s:promotionID", "ym:s:promotionName", "ym:s:promotionCreative",
    "ym:s:promotionPosition", "ym:s:promotionCreativeSlot",
    "ym:s:promotionEventTime", "ym:s:promotionType",
    
    # Офлайн-звонки
    "ym:s:offlineCallTalkDuration", "ym:s:offlineCallHoldDuration",
    "ym:s:offlineCallMissed", "ym:s:offlineCallTag",
    "ym:s:offlineCallFirstTimeCaller", "ym:s:offlineCallURL",
    
    # Пользовательские параметры
    "ym:s:parsedParamsKey1", "ym:s:parsedParamsKey2", "ym:s:parsedParamsKey3",
    "ym:s:parsedParamsKey4", "ym:s:parsedParamsKey5", "ym:s:parsedParamsKey6",
    "ym:s:parsedParamsKey7", "ym:s:parsedParamsKey8", "ym:s:parsedParamsKey9",
    "ym:s:parsedParamsKey10",
    
    # Дополнительные источники трафика
    "ym:s:<attribution>RecommendationSystem", "ym:s:<attribution>Messenger"
]

HITS_METRICS = [
    # Базовые параметры хита
    "ym:pv:date", "ym:pv:dateTime", "ym:pv:clientID", "ym:pv:watchID",
    "ym:pv:counterID", "ym:pv:title", "ym:pv:URL", "ym:pv:referer",
    
    # Параметры страницы
    "ym:pv:browserLanguage", "ym:pv:browserCountry", "ym:pv:regionCountry",
    "ym:pv:regionCity", "ym:pv:pageCharset", "ym:pv:pageHeight", "ym:pv:pageWidth",
    "ym:pv:pagePath", "ym:pv:pageQuery", "ym:pv:shareService", "ym:pv:shareTitle",
    "ym:pv:shareURL", "ym:pv:viewportHeight", "ym:pv:viewportWidth",
    
    # Параметры устройства
    "ym:pv:deviceCategory", "ym:pv:deviceBrand", "ym:pv:deviceModel",
    "ym:pv:deviceModelVersion", "ym:pv:operatingSystem", "ym:pv:browser",
    "ym:pv:browserVersion", "ym:pv:browserMajorVersion", "ym:pv:browserEngine",
    "ym:pv:cookieEnabled", "ym:pv:javascriptEnabled", "ym:pv:flashMajor",
    "ym:pv:flashMinor", "ym:pv:screenFormat", "ym:pv:screenColors",
    "ym:pv:screenOrientation", "ym:pv:screenWidth", "ym:pv:screenHeight",
    "ym:pv:physicalScreenWidth", "ym:pv:physicalScreenHeight",
    
    # Рекламные параметры
    "ym:pv:visitID", "ym:pv:UTMCampaign", "ym:pv:UTMContent", "ym:pv:UTMMedium",
    "ym:pv:UTMSource", "ym:pv:UTMTerm", "ym:pv:openstatAd", "ym:pv:openstatCampaign",
    "ym:pv:openstatService", "ym:pv:openstatSource", "ym:pv:hasGCLID",
    "ym:pv:GCLID", "ym:pv:from", "ym:pv:YMCLID",
    
    # События и цели
    "ym:pv:eventType", "ym:pv:eventCategory", "ym:pv:eventAction",
    "ym:pv:eventLabel", "ym:pv:eventValue", "ym:pv:goalsID",
    
    # Параметры взаимодействия
    "ym:pv:link", "ym:pv:download", "ym:pv:notBounce", "ym:pv:lastTrafficSource",
    "ym:pv:lastSearchEngine", "ym:pv:lastSearchEngineRoot", "ym:pv:lastAdvEngine",
    "ym:pv:artificial", "ym:pv:pageUrlHash", "ym:pv:lastSocialNetwork",
    
    # Параметры загрузки
    "ym:pv:loadReachGoal", "ym:pv:loadRegion", "ym:pv:loadOrderAmount",
    "ym:pv:startLayerLoadTime", "ym:pv:userClientID", "ym:pv:networkType",
    
    # Яндекс.Директ
    "ym:pv:lastDirectClickOrder", "ym:pv:lastDirectBannerGroup",
    "ym:pv:lastDirectClickBanner", "ym:pv:lastDirectPhraseOrCond",
    "ym:pv:lastDirectPlatformType", "ym:pv:lastDirectOrderID",
    "ym:pv:lastDirectClickPageRef", "ym:pv:lastClickBannerGroupName",
    "ym:pv:lastClickBannerGroupOrder", "ym:pv:lastClickBannerName",
    "ym:pv:lastClickBannerState", "ym:pv:lastClickOrderID",
    "ym:pv:lastClickOrderName", "ym:pv:lastClickPageNum",
    "ym:pv:lastClickResourceID", "ym:pv:lastDirectAdID", 
    "ym:pv:lastDirectClickBannerID", "ym:pv:lastDirectClickOrderName",
    "ym:pv:lastDirectClickResourceID", "ym:pv:lastDirectCondID",
    "ym:pv:lastDirectSource",
    
    # Пользовательские параметры
    "ym:pv:parsedParamsKey1", "ym:pv:parsedParamsKey2", "ym:pv:parsedParamsKey3",
    "ym:pv:parsedParamsKey4", "ym:pv:parsedParamsKey5", "ym:pv:parsedParamsKey6",
    "ym:pv:parsedParamsKey7", "ym:pv:parsedParamsKey8", "ym:pv:parsedParamsKey9",
    "ym:pv:parsedParamsKey10",
    
    # Дополнительные параметры
    "ym:pv:params", "ym:pv:paramsLevel1", "ym:pv:paramsLevel2",
    "ym:pv:paramsLevel3", "ym:pv:paramsLevel4", "ym:pv:paramsLevel5",
    "ym:pv:paramsLevel6", "ym:pv:paramsLevel7", "ym:pv:paramsLevel8"
]

def get_available_metrics(report_type='visits'):
    """Returns available metrics based on report type with validation"""
    if not isinstance(report_type, str):
        raise ValueError("Report type must be a string")
        
    metrics_map = {
        'visits': VISITS_METRICS,
        'hits': HITS_METRICS
    }
    
    if report_type not in metrics_map:
        raise ValueError("Unsupported report type. Use 'visits' or 'hits'")
        
    logger.debug(f"Available metrics for {report_type}: {len(metrics_map[report_type])} metrics")
    return metrics_map[report_type]

def validate_fields(fields, report_type='visits'):
    """Validates fields based on report type"""
    if not isinstance(fields, list) or not fields:
        raise ValueError("Fields must be a non-empty list")
        
    # Check if we have mixed metrics
    has_visits_metrics = any(f.startswith("ym:s:") for f in fields)
    has_hits_metrics = any(f.startswith("ym:pv:") for f in fields)
    
    if has_visits_metrics and has_hits_metrics:
        raise ValueError(
            "Cannot mix metrics from different report types.\n"
            "Use either 'ym:s:' (visits) or 'ym:pv:' (hits) metrics."
        )
    
    # Determine actual report type based on metrics
    actual_report_type = report_type
    if has_visits_metrics and report_type == 'hits':
        logger.warning("Changing report type to 'visits' based on metrics prefixes")
        actual_report_type = 'visits'
    elif has_hits_metrics and report_type == 'visits':
        logger.warning("Changing report type to 'hits' based on metrics prefixes")
        actual_report_type = 'hits'
    
    # Validate metrics are available
    available_metrics = get_available_metrics(actual_report_type)
    invalid_fields = [field for field in fields if field not in available_metrics]
    
    if invalid_fields:
        raise ValueError(
            f"Following metrics are not available for {actual_report_type}:\n"
            f"{', '.join(invalid_fields)}"
        )
    
    logger.info(f"Field validation successful for {actual_report_type}")
    return actual_report_type

# Создание запроса на выгрузку
def create_log_request(token, counter_id, fields, date1, date2, attribution='last'):
    """Создание запроса к Logs API"""
    base_url = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequests"
    
    # Определяем тип источника
    source = "visits" if any(f.startswith("ym:s:") for f in fields) else "hits"
    
    # Форматируем даты
    formatted_date1 = format_date(date1)
    formatted_date2 = format_date(date2)
    
    # Создаем строку полей
    fields_str = ','.join(fields)
    
    # URL параметры
    params = {
        'date1': formatted_date1,
        'date2': formatted_date2,
        'fields': fields_str,  # Добавляем поля в URL
        'source': source
    }
    
    # Тело запроса
    request_data = {
        'date1': formatted_date1,
        'date2': formatted_date2,
        'source': source,
        'fields': fields,
        'attribution': attribution
    }
    
    headers = {
        "Authorization": f"OAuth {token}",
        "Content-Type": "application/json"
    }
    
    logger.info("=== Создание запроса к Logs API ===")
    logger.info(f"Тип отчета: {source}")
    logger.info(f"Метрики: {', '.join(fields)}")
    logger.info(f"Даты: {formatted_date1} - {formatted_date2}")
    logger.debug(f"Тело запроса: {request_data}")
    
    try:
        response = session.post(
            url=base_url,
            params=params,  # Параметры в URL
            json=request_data,  # Параметры в теле
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            error_msg = f"Ошибка API {response.status_code}"
            try:
                error_details = response.json()
                if 'message' in error_details:
                    error_msg += f": {error_details['message']}"
                if 'errors' in error_details:
                    for error in error_details['errors']:
                        if 'message' in error:
                            error_msg += f"\n- {error['message']}"
            except:
                error_msg += f": {response.text}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        response_data = response.json()
        request_id = response_data.get("log_request", {}).get("request_id")
        
        if not request_id:
            raise ValueError("API не вернул request_id")
            
        logger.info(f"Успешно создан запрос типа {source}, ID: {request_id}")
        return request_id
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка сети: {str(e)}")
        raise

# Проверка статуса готовности запроса
def wait_for_request_ready(token, counter_id, request_id):
    url = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}"
    headers = {
        "Authorization": f"OAuth {token}",
    }

    while True:
        time.sleep(API_DELAY)  # Добавляем задержку перед каждым запросом
        response = session.get(url, headers=headers)
        response.raise_for_status()
        status = response.json()["log_request"]["status"]
        logger.info(f"Статус: {status}")
        
        if status == "processed":
            return
        elif status == "created" or status == "processing":
            time.sleep(RETRY_DELAY)  # Увеличенная задержка при ожидании обработки
        else:
            logger.error(f"Неожиданный статус запроса: {status}")
            raise Exception(f"Неожиданный статус запроса: {status}")

# Загрузка данных
def download_parts(token, counter_id, request_id):
    """Загрузка данных отчета согласно документации"""
    try:
        headers = {
            "Authorization": f"OAuth {token}",
            "Accept-Encoding": "gzip"  # Поддержка сжатия
        }
        
        # URL для прямой загрузки данных
        download_url = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}/part/0/download"
        
        logger.info(f"Попытка загрузки данных с URL: {download_url}")
        
        all_rows = []
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                logger.info(f"Попытка загрузки данных {attempt + 1}/{max_attempts}")
                
                response = session.get(
                    download_url, 
                    headers=headers,
                    stream=True  # Потоковая загрузка для больших файлов
                )
                response.raise_for_status()
                
                # Обработка TSV данных построчно
                lines = response.text.strip().split('\n')
                if not lines:
                    logger.warning("Получен пустой ответ")
                    continue
                    
                # Первая строка - заголовки
                headers = lines[0].split('\t')
                logger.info(f"Получены заголовки: {headers}")
                
                # Остальные строки - данные
                for line in lines[1:]:
                    values = line.split('\t')
                    if len(values) == len(headers):
                        row = dict(zip(headers, values))
                        all_rows.append(row)
                        
                logger.info(f"Успешно загружено {len(all_rows)} строк")
                return all_rows
                
            except requests.exceptions.RequestException as e:
                if attempt == max_attempts - 1:
                    raise
                logger.warning(f"Попытка {attempt + 1} не удалась, повтор через {RETRY_DELAY} секунд")
                time.sleep(RETRY_DELAY)
                
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {str(e)}")
        raise

# Удаление запроса после загрузки
def clean_up_request(token, counter_id, request_id):
    url = f"https://api-metrika.yandex.net/management/v1/counter/{counter_id}/logrequest/{request_id}"
    headers = {
        "Authorization": f"OAuth {token}",
    }
    response = session.delete(url, headers=headers)
    if response.status_code != 204:
        logger.warning("Не удалось удалить запрос.")

# Основная функция
def fetch_report(login, token, counter_id, report_type, metrics, date1="7daysAgo", date2="today", attribution="last"):
    try:
        if not validate_token(token):
            raise ValueError("Invalid token")
            
        # Validate fields and get actual report type
        actual_report_type = validate_fields(metrics, report_type)
        if actual_report_type != report_type:
            logger.info(f"Report type adjusted from {report_type} to {actual_report_type} based on metrics")
            report_type = actual_report_type
        
        # Проверяем формат дат
        validate_dates(date1, date2)
        
        fields = metrics
        logger.info(f"Начало выгрузки отчета для counter_id: {counter_id}")
        logger.info(f"Период: с {date1} по {date2}")
        logger.info(f"Выбранные метрики: {', '.join(fields)}")
        
        try:
            request_id = create_log_request(
                token=token, 
                counter_id=counter_id, 
                fields=fields, 
                date1=date1, 
                date2=date2,
                attribution=attribution
            )
            logger.info(f"Создан запрос с ID: {request_id}, атрибуция: {ATTRIBUTION_TYPES[attribution]}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при создании запроса: {str(e)}")
            raise

        try:
            wait_for_request_ready(token, counter_id, request_id)
            logger.info("Данные готовы к загрузке")
        except Exception as e:
            logger.error(f"Ошибка при ожидании готовности данных: {str(e)}")
            raise

        try:
            data = download_parts(token, counter_id, request_id)
            logger.info(f"Загружено {len(data) if data else 0} строк данных")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных: {str(e)}")
            raise

        try:
            clean_up_request(token, counter_id, request_id)
            logger.info("Запрос успешно очищен")
        except Exception as e:
            logger.warning(f"Ошибка при очистке запроса: {str(e)}")

        return data

    except Exception as e:
        logger.error(f"Критическая ошибка в fetch_report: {str(e)}")
        raise

# Сохранение в CSV
def save_to_csv(data, filepath, attribution='last'):
    """Сохранение данных в CSV файл с очисткой заголовков и подстановкой атрибуции"""
    try:
        if not data:
            logger.warning("Нет данных для сохранения")
            return
            
        # Получаем заголовки из первой строки данных
        headers = list(data[0].keys())
        
        # Очищаем заголовки от префиксов и заменяем <attribution> на значение атрибуции
        clean_headers = []
        for header in headers:
            # Убираем префиксы ym:s: и ym:pv:
            header = header.replace('ym:s:', '').replace('ym:pv:', '')
            # Заменяем <attribution> на значение атрибуции
            header = header.replace('<attribution>', attribution)
            clean_headers.append(header)
        
        # Создаем новый список словарей с очищенными заголовками
        clean_data = []
        
        # Собираем все уникальные ID целей
        goals_ids = set()
        goals_field = 'ym:s:goalsID'  # Поле с ID целей
        
        for row in data:
            if goals_field in row and row[goals_field]:
                # Удаляем квадратные скобки и пробелы, разделяем по запятой
                ids = row[goals_field].strip('[]').replace(' ', '').split(',')
                # Добавляем только непустые ID
                goals_ids.update(id for id in ids if id)
        
        # Добавляем новые заголовки для каждого ID цели
        goals_headers = [f"goalsID_{goal_id}" for goal_id in sorted(goals_ids)]
        all_headers = clean_headers + goals_headers
        
        # Обрабатываем данные с учетом новых столбцов целей
        for row in data:
            clean_row = {}
            # Копируем основные данные
            for old_key, new_key in zip(headers, clean_headers):
                clean_row[new_key] = row[old_key]
            
            # Обрабатываем цели
            if goals_field in row and row[goals_field]:
                current_goals = row[goals_field].strip('[]').replace(' ', '').split(',')
                for goal_id in goals_ids:
                    field_name = f"goalsID_{goal_id}"
                    clean_row[field_name] = '1' if goal_id in current_goals else '--'
            else:
                # Если поле целей отсутствует или пустое, заполняем все столбцы целей значением '--'
                for goal_id in goals_ids:
                    field_name = f"goalsID_{goal_id}"
                    clean_row[field_name] = '--'
            
            clean_data.append(clean_row)
        
        # Очищаем данные от пустых значений
        clean_data_no_empty = []
        for row in clean_data:
            row_no_empty = {k: v for k, v in row.items() if v is not None and v != ''}
            clean_data_no_empty.append(row_no_empty)
        
        # Определяем только те заголовки, которые реально используются в данных
        used_headers = set()
        for row in clean_data_no_empty:
            used_headers.update(row.keys())
        used_headers = list(used_headers)
        
        # Сортируем заголовки, чтобы столбцы целей шли в конце
        used_headers.sort(key=lambda x: (x.startswith('goalsID_'), x))        # Записываем в CSV только используемые заголовки и непустые данные
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            # Используем другой диалект CSV для лучшей совместимости с DataLens
            writer = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            
            # Записываем заголовки в первую строку
            writer.writerow(used_headers)
            
            # Записываем данные построчно
            for row in clean_data_no_empty:
                # Для каждой строки записываем значения в том же порядке, что и заголовки
                row_values = [row.get(header, '') for header in used_headers]
                writer.writerow(row_values)
            
        logger.info(f"Данные успешно сохранены в {filepath}")
        logger.info(f"Количество столбцов в файле: {len(used_headers)}")
        logger.info(f"Добавлено столбцов целей: {len(goals_headers)}")
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении в CSV: {str(e)}")
        raise

# Валидация токена
def validate_token(token):
    try:
        url = "https://api-metrika.yandex.net/management/v1/counters"
        headers = {"Authorization": f"OAuth {token}"}
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info("Токен валиден")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка валидации токена: {str(e)}")
        return False

# Валидация дат
def validate_dates(date1, date2):
    """
    Проверяет корректность дат. Поддерживает форматы:
    - YYYY-MM-DD
    - today
    - yesterday
    - NdaysAgo (например, 7daysAgo)
    """
    def parse_date(date_str):
        if isinstance(date_str, datetime):
            return date_str.date()
        
        if date_str == "today":
            return datetime.now().date()
        elif date_str == "yesterday":
            return (datetime.now() - timedelta(days=1)).date()
        elif isinstance(date_str, str) and date_str.endswith("daysAgo"):
            try:
                days = int(date_str.replace("daysAgo", ""))
                return (datetime.now() - timedelta(days=days)).date()
            except ValueError:
                raise ValueError(f"Некорректный формат даты: {date_str}")
        else:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Некорректный формат даты: {date_str}. Используйте формат YYYY-MM-DD")

    try:
        start_date = parse_date(date1)
        end_date = parse_date(date2)
        
        if start_date > end_date:
            raise ValueError("Дата начала не может быть позже даты окончания")
            
        logger.info(f"Даты валидны: с {start_date} по {end_date}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка валидации дат: {str(e)}")
        raise ValueError(f"Ошибка валидации дат: {str(e)}")
