import requests
from datetime import datetime
import time
import pandas as pd
import numpy as np

seconds_in_day = 24 * 60 * 60

DELTA = 8640  # 1/10 of one day (UNIX)

START_TIME = int(time.time())

headers = {
    'content-type': 'application/json',
    'X-Api-App-Id': ''
}

params = {
    'catalogues': 33,  # категория IT, Интернет, связь,...
    'no_agreement': 1,  # без зп "по договоренности"
    'period': 0,  # показ активных вакансий за весь период
    'count': 100,
    'page': 0,
    # т.к api superjob не позволяет вытягивать одним запросом больше 500 сущностей даже с учетом пагинации,
    # то придется менять параметр времени в запросе
    # то есть, если резальтат запроса содержит 2800 результатов, вытянуть получится лишь 5 страниц по 100 сущностей
    # поэтому придется менять запрос по времени
    'date_published_from': START_TIME - DELTA,
    'date_published_to': START_TIME
}

URL = "https://api.superjob.ru/2.33/vacancies/"

data = []


def map_vacancy(v):
    min_salary = v['payment_from']
    max_salary = v['payment_to']

    if min_salary == 0 and max_salary == 0:
        raise Exception("Неверный формат данных!")
    if min_salary == 0:
        min_salary = max_salary
    if max_salary == 0:
        max_salary = min_salary

    return {
        'name': v['profession'],
        'town': v['town']['title'],
        'min_salary': min_salary,
        'max_salary': max_salary,
        'company_name': v['firm_name'],
        'date_published': datetime.utcfromtimestamp(v['date_published']).strftime('%Y-%m-%d %H:%M:%S'),
        'date_delta': datetime.now() - datetime.utcfromtimestamp(v['date_published']),
        'experience': v['experience']['title'],
        'type_of_work': v['type_of_work']['title'],
        'description': v['vacancyRichText'],
        'duties': v['candidat'],  # обязаности и требования
        'conditions': v['compensation'],  # условия
        'key_skills': [y['title'] for x in v['catalogues'] for y in x['positions'] if x['id'] == 33]
    }


def analyze(df):
    print('max salary count: \n\n', df['max_salary'].value_counts())
    print()
    print('min salary count: \n\n', df['min_salary'].value_counts())
    print()
    print('names count: \n\n', df['name'].value_counts())
    print()
    print('max days published: ', df['date_delta'].max())
    print()
    print('min days published: ', df['date_delta'].min())
    print()
    print('mean days published: ', df['date_delta'].mean())
    print()
    print('experience count: \n\n', df['experience'].value_counts())
    print()
    print('type of work count: \n\n', df['type_of_work'].value_counts())
    print()
    print('skills: \n\n', df['key_skills'].value_counts())
    print()


while True:
    response = requests.get(url=URL, headers=headers, params=params)
    data_1 = response.json()
    data.extend(map(map_vacancy, data_1['objects']))
    print(datetime.utcfromtimestamp(params['date_published_to']).strftime('%Y-%m-%d %H:%M:%S'))
    print(datetime.utcfromtimestamp(params['date_published_from']).strftime('%Y-%m-%d %H:%M:%S'))
    print(len(data_1['objects']))
    print(len(data))
    params['page'] += 1
    if not data_1['more']:
        params['page'] = 0
        params['date_published_from'] -= DELTA
        params['date_published_to'] -= DELTA
        if params['date_published_to'] < START_TIME - DELTA * 10 * 10:
            break

data_frame = pd.DataFrame.from_records(data)

data_frame.to_csv('lab03.csv', encoding='utf-8-sig')

# сортировка по максимальной и минимальной
data_frame = data_frame.sort_values(['max_salary', 'min_salary'])

# разбиение на 10 групп по масимальной зп (группа 1: от 0% до 10% от макс, группа 2: от 10% до 20% от макс и т.д.)
target_percentiles = range(10, 100, 10)

cut_points = [np.percentile(data_frame['max_salary'], i) for i in target_percentiles]

data_frame['salary_group'] = 1
for i in range(len(target_percentiles)):
    data_frame['salary_group'] = data_frame['salary_group'] + (data_frame['max_salary'] < cut_points[i])

# группировка по группам зп
df_dict_1 = {}
for name, grouped_data_frame in data_frame.groupby('salary_group'):
    df_dict_1[name] = grouped_data_frame
    analyze(grouped_data_frame)

# группировка по названию вкансии
df_dict_2 = {}
for name, grouped_data_frame in data_frame.groupby('name'):
    df_dict_2[name] = grouped_data_frame
    analyze(grouped_data_frame)
