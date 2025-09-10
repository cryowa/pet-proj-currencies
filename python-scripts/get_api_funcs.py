import requests
import json
import datetime


start_url = "https://www.okx.com"
middle_part_url = "/api/v5/market/tickers"
params_url = "?instType=SPOT"
url = f"{start_url}{middle_part_url}{params_url}"

def get_okx_crypto_price_data():
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        #dir_name = params_url[params_url.find("=")+1:]
        #with open(f'CryptoCcys/{dir_name}/okx_{dir_name}_{int(datetime.datetime.now().timestamp()*1000)}.json', 'w', encoding='utf-8') as f:
        #    json.dump(data, f, ensure_ascii=False, indent=4)
    else:
        print(f"Ошибка: {response.status_code}")
        return response
    return data
