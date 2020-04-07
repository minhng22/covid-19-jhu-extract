import csv
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient 
from datetime import datetime
from datetime import timedelta  
from dotenv import load_dotenv
from pathlib import Path  # python3 only
import os
from data_export_jhu import find_state_id, find_country_id
import matplotlib.pyplot as plt

def main():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    MONGODB_CONNECT_STR = os.getenv('MONGODB_CONNECT_STR')

    client = AsyncIOMotorClient(MONGODB_CONNECT_STR)
    db = client.get_database("covid19")
    
    asyncio.get_event_loop().run_until_complete(analyze(db))


async def analyze(db):
    data = await load_data(db)
    current_cases, current_countries, current_states, current_counties = data['current_cases'], data['current_countries'], data['current_states'], data['current_counties']
    current_d_cases, current_d_countries, current_d_states, current_d_counties = data['current_d_cases'], data['current_d_countries'], data['current_d_states'], data['current_d_counties']
    print('Start analyzing: ')
    
    show_all_cases(current_d_cases, [
        {'territory_type': 'COUNTY', 'territory_id': '5e8b6f87802382a820edf6b1'},
        {'territory_type': 'COUNTRY', 'territory_id': '5e8b6f74802382a820edef1a'}
    ])


def show_all_counties(current_countries: any, current_states: any, current_counties: any, country: str, state: str):
    c_id = find_country_id(current_countries, country)
    s_id = find_state_id(current_states, state, c_id)

    print("Querrying state: ", state, " with id ", s_id ," of country: ", country, " of id ", c_id)
    for cc in current_counties:
        if cc['state_id'] == s_id:
            print(cc) 


def show_all_cases(current_cases, info):
    cases = []
    for inf in info:
        cases.append(get_cases(current_cases, inf['territory_type'], inf['territory_id']))
    line_chart(cases)


def get_cases(current_cases, t_type, t_id):
    cases = []
    for cc in current_cases:
        if cc['territory_type'] == t_type and cc['territory_id'] == t_id:
            cases.append(cc)
    cases = sorted(cases, key=lambda case: datetime.strptime(case['timestamp'], '%m/%d/%y').date())
    print("Cases are ")
    for c in cases:
        print(c)
    return cases


def show_new_case(current_cases, t_type, t_id):
    new_cases = []
    for cc in current_cases:
        if cc['territory_type'] == t_type and cc['territory_id'] == t_id:
            if len(new_cases) == 0:
                new_cases.append(cc)
            else:
                case_no = int(new_cases[len(new_cases) -1]['no'])
                cc['no'] = int(cc['no']) - case_no
            new_cases.append(cc)
    for nc in new_cases:
        print(new_cases)
    line_chart([new_cases])


def line_chart(cases_series):
    time_stamps, no = [], []
    for case in cases_series:
        time_stamps_temp, no_temp = [], []
        for c in case:
            time_stamps_temp.append(c['timestamp'])
            no_temp.append(c['no'])
        time_stamps.append(time_stamps_temp)
        no.append(no_temp)
    plt.ylabel('Case number')
    for i in range(len(cases_series)):
        plt.plot(time_stamps[i], no[i], color='green', marker='o', linestyle='dashed', linewidth=1, markersize=6)
    plt.show()


async def load_data(db) -> dict:
    print('Loading data')
    data = dict()
    data['current_cases'] = await db.get_collection('Case').find().to_list(length = 1000000)
    data['current_countries'] = await db.get_collection('Country').find().to_list(length = 500)
    data['current_states'] = await db.get_collection('State').find().to_list(length = 500)
    data['current_counties'] = await db.get_collection('County').find().to_list(length = 50000)

    data['current_d_cases'] = await db.get_collection('CaseDeath').find().to_list(length = 1000000)
    data['current_d_countries'] = await db.get_collection('CountryDeath').find().to_list(length = 500)
    data['current_d_states'] = await db.get_collection('StateDeath').find().to_list(length = 500)
    data['current_d_counties'] = await db.get_collection('CountyDeath').find().to_list(length = 50000)

    return data


if __name__ == "__main__":
    # execute only if run as a script
    main()