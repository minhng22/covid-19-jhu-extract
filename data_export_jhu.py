import csv
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient 
from datetime import datetime
from datetime import timedelta  
from dotenv import load_dotenv
from pathlib import Path  # python3 only
import os


def main():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    MONGODB_CONNECT_STR = os.getenv('MONGODB_CONNECT_STR')

    client = AsyncIOMotorClient(MONGODB_CONNECT_STR)
    db = client.get_database("covid19")
    PATH = [
        '../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
        '../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
    ]
    asyncio.get_event_loop().run_until_complete(update_territory(db, PATH))
    asyncio.get_event_loop().run_until_complete(update_cases(db, PATH))


async def update_territory(db: any, PATH: list):
    await update_country(db, PATH)
    await update_state(db, PATH)
    await update_county(db, PATH)


async def update_cases(db, PATH):
    current_cases_col = db.get_collection('Case')
    current_cases = await current_cases_col.find().to_list(length = 1000000)
    print('Current cases: ', current_cases)
    new_cases = await update_live_data_cases(db, PATH, current_cases)
    print('New case count: ', len(new_cases))
    # for c in new_cases:
    #     current_cases_col.insert_one({
    #         'no': c['no'],
    #         'timestamp': c['timestamp'],
    #         'territory_type': c['territory_type'],
    #         'territory_id': c['territory_id']
    #     })


async def update_live_data_cases(db, paths, current_cases):
    current_counties = await db.get_collection('County').find().to_list(length = 20000)
    current_countries = await db.get_collection('Country').find().to_list(length = 500)
    current_states = await db.get_collection('State').find().to_list(length = 500)
    new_cases = []

    for i,p in enumerate(paths):
        with open( p, 'r' ) as the_file:
            reader = csv.DictReader(the_file)
            for line in reader:
                territory_type, territory_id, ter_name = '', '', ''

                in_line_state = line['Province/State'] if i == 0 else line['Province_State']
                in_line_country = line['Country/Region'] if i == 0 else line['Country_Region']
                c_id = find_country_id(current_countries, in_line_country)

                if is_counties(line):
                    territory_type = "COUNTY"
                    ter_name = line['Admin2']
                    territory_id = find_county_id(current_counties, line['Admin2'], find_state_id(current_states, in_line_state, c_id))
                elif is_state(line, p):
                    territory_type = "STATE"
                    ter_name = in_line_state
                    territory_id = find_state_id(current_states, in_line_state, c_id)
                else:
                    territory_type = "COUNTRY"
                    territory_id = c_id
                    ter_name = in_line_country
                print('name: ', ter_name)
                c = get_cases_in_line(territory_id, territory_type, line, current_cases)
                print('Adding: ', c)
                new_cases.extend(c)
    return new_cases    


async def update_country(db: any, PATH: list):
    country_data_col = db.get_collection('Country')
    prev_countries = await country_data_col.find().to_list(length = 500)
    print('Prev countries count: ', len(prev_countries))
    new_countries = update_live_data_countries(prev_countries, PATH[0])
    print('New countries: ', new_countries)
    for c in new_countries:
        country_data_col.insert_one({
            'name': c
        })


async def update_state(db, PATH):
    state_data_col = db.get_collection('State')
    prev_states = await state_data_col.find().to_list(length = 500)
    current_countries = await db.get_collection('Country').find().to_list(length = 500)
    new_states = update_live_data_states(current_countries, prev_states, PATH)
    print('New states count: ', len(new_states))
    print('New states: ', new_states)
    for s in new_states:
        state_data_col.insert_one({
            'country_id': s['country_id'],
            'name': s['name']
        })


async def update_county(db, PATH):
    county_data_col = db.get_collection('County')
    prev_counties = await county_data_col.find().to_list(length = 20000)
    current_countries = await db.get_collection('Country').find().to_list(length = 500)
    current_states = await db.get_collection('State').find().to_list(length = 500)

    print('Previous counties count: ', len(prev_counties))
    new_counties = update_live_data_counties(current_countries, current_states, prev_counties, PATH[1])
    print('New counties count: ', len(new_counties))
    for s in new_counties:
        county_data_col.insert_one({
            'state_id': s['state_id'],
            'name': s['name']
        })


def update_live_data_countries(prev_countries: list, PATH: str) -> list:
    countries_update = []
    with open( PATH, 'r' ) as the_file:
        reader = csv.DictReader(the_file)
        for line in reader:
            if new_country(prev_countries, line['Country/Region']) and country_not_added(countries_update,line['Country/Region']):
                countries_update.append(line['Country/Region'])
    return countries_update


def country_not_added (countries: list, c: str) -> bool:
    for cs in countries:
        if cs == c:
            return False
    return True


def update_live_data_states(current_countries, prev_states, paths):
    state_update = []
    for i,p in enumerate(paths):
        with open( p, 'r' ) as the_file:
            reader = csv.DictReader(the_file)
            for line in reader:
                if is_state(line, p):
                    in_line_state = line['Province/State'] if i == 0 else line['Province_State']
                    in_line_country = line['Country/Region'] if i == 0 else line['Country_Region']

                    if new_state(current_countries, prev_states, in_line_state, in_line_country) and not_added_state(state_update, find_country_id(current_countries, in_line_country),in_line_state):
                        state_update.append({
                            'country_id': find_country_id(current_countries, in_line_country),
                            'name': in_line_state
                        })
    return state_update


def not_added_state(current_states, c_id, in_line_state):
    for cs in current_states:
        if cs['country_id'] == c_id and cs['name'] == in_line_state:
            return False
    return True


def not_added_county(current_counties, state_id, county):
    for cc in current_counties:
        if cc['name'] == county and cc['state_id'] == state_id:
            return False
    return True


def update_live_data_counties(current_countries, current_states, prev_counties, PATH):
    county_update = []
    with open( PATH, 'r' ) as the_file:
            reader = csv.DictReader(the_file)
            for line in reader:
                if is_counties(line):
                    s, cty, c_id = line['Province_State'], line['Admin2'], find_country_id(current_countries, line['Country_Region'])
                    s_id = find_state_id(current_states,s, c_id)
                    if new_county(current_states, prev_counties, s, line['Admin2'], c_id) and not_added_county(county_update, s_id, cty):
                        county_update.append({
                            'state_id': find_state_id(current_states, s, find_country_id(current_countries, line['Country_Region'])),
                            'name': cty
                        })  
    return county_update


def find_country_id(current_countries, name):
    for c in current_countries:
        if c['name'] == name:
            return str(c['_id'])


def get_cases_in_line(territory_id, territory_type, line, update_cases):
    x, cases = datetime(2020, 1, 22), []
    print('Now is: ', datetime.now())

    while x < datetime.now() and x.strftime("%-m/%-d/%y") in line.keys():
        f_date = x.strftime("%-m/%-d/%y")
        print('date: ', f_date)
        print('data: ', line[f_date])
        c_dict = dict()
        c_dict['no'] = line[f_date]
        c_dict['timestamp'] = f_date
        c_dict['territory_type'] = territory_type
        c_dict['territory_id'] = territory_id
        print('CDict: ', c_dict)
        if new_cases(update_cases, c_dict):
            cases.append(c_dict)

        x += timedelta(days=1)  
        print(x.strftime("%-m/%-d/%y"))

    return cases


def new_cases(update_cases, case):
    for uc in update_cases:
        if uc['no'] == case['no'] and uc['timestamp'] == case['timestamp'] and uc['territory_type'] == case['territory_type'] and uc['territory_id'] == case['territory_id']:
            return False
    return True


def new_country(prev_countries: list, country: str) -> bool:
    for p in prev_countries:
        if p['name'] == country:
            return False
    return True


def new_state(current_countries, prev_states, s_name, c_name):
    c_id = find_country_id(current_countries, c_name)
    for s in prev_states:
        if s['name'] == s_name and s['country_id'] == c_id:
            return False
    return True


def new_county(current_states, prev_counties, s_name, cty_name, c_id):
    s_id = find_state_id(current_states, s_name, c_id)
    for cty in prev_counties:
        if cty['name'] == cty_name and cty['state_id'] == s_id:
            return False
    return True


def find_state_id(current_states, s_name, c_id):
    for s in current_states:
        if s['name'] == s_name and s['country_id'] == c_id:
            return str(s['_id'])


def find_county_id(current_counties, cty_name, s_id):
    for s in current_counties:
        if s['name'] == cty_name and s['state_id'] == s_id:
            return str(s['_id'])


def is_counties(line):
    return 'Admin2' in line.keys() and line['Admin2'] != ""


def is_state(line, path):
    if path == '../COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv':
        return 'Province/State' in line.keys() and line['Province/State'] != ""
    else:
        return 'Province_State' in line.keys() and line['Province_State'] != ""

if __name__ == "__main__":
    # execute only if run as a script
    main()