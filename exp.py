import csv
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient 
from datetime import datetime
from datetime import timedelta  
from dotenv import load_dotenv
from pathlib import Path  # python3 only
import os

def main():
    case_type = ['POSITIVE', 'DEATH']
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    MONGODB_CONNECT_STR = os.getenv('MONGODB_CONNECT_STR')

    print(COUNTY_LENGTH)

    client = AsyncIOMotorClient(MONGODB_CONNECT_STR)
    db = client.get_database("covid19")

    for c_type in case_type:
        export_data(c_type, db)

COUNTY_LENGTH = 10000000
CASE_LENGTH = 100000000
COUNTRY_LENGTH = 1000
STATE_LENGTH = 100000

def export_data(c_type: any, db: any):
    PATH = [
        os.getenv('PATH_GLOBAL_POSITIVE') if (c_type == "POSITIVE") else os.getenv('PATH_GLOBAL_DEATH'),
        os.getenv('PATH_US_POSITIVE') if (c_type == "POSITIVE") else os.getenv('PATH_US_DEATH')
    ]
    asyncio.get_event_loop().run_until_complete(clean_db(c_type, db))
    asyncio.get_event_loop().run_until_complete(update_territory(c_type, db, PATH))
    asyncio.get_event_loop().run_until_complete(update_cases(c_type, db, PATH))


async def clean_db(c_type, db):
    await db.drop_collection('County' if (c_type == "POSITIVE") else 'CountyDeath')
    await db.drop_collection('State' if (c_type == "POSITIVE") else 'StateDeath')
    await db.drop_collection('Country' if (c_type == "POSITIVE") else 'CountryDeath')
    await db.drop_collection('Case' if (c_type == "POSITIVE") else 'CaseDeath')

    
async def update_territory(c_type: any, db: any, PATH: list):
    await update_country(c_type, db, PATH)
    await update_state(c_type, db, PATH)
    await update_county(c_type, db, PATH)


async def update_cases(c_type, db, PATH):
    current_cases_col = db.get_collection('Case' if (c_type == "POSITIVE") else 'CaseDeath')
    current_cases = await current_cases_col.find().to_list(length = CASE_LENGTH)
    print('Current ' , c_type, ' cases: ', current_cases)
    new_cases = await update_live_data_cases(c_type, db, PATH, current_cases)
    print('New ' , c_type, ' case count: ', len(new_cases))
    for c in new_cases:
        current_cases_col.insert_one({
            'no': c['no'],
            'timestamp': c['timestamp'],
            'territory_type': c['territory_type'],
            'territory_id': c['territory_id']
        })


async def update_live_data_cases(c_type, db, paths, current_cases):
    current_counties = await db.get_collection('County' if (c_type == "POSITIVE") else 'CountyDeath').find().to_list(length = COUNTY_LENGTH)
    current_countries = await db.get_collection('Country' if (c_type == "POSITIVE") else 'CountryDeath').find().to_list(length = COUNTRY_LENGTH)
    current_states = await db.get_collection('State' if (c_type == "POSITIVE") else 'StateDeath').find().to_list(length = STATE_LENGTH)
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
                elif is_state(c_type, line, p):
                    territory_type = "STATE"
                    ter_name = in_line_state
                    territory_id = find_state_id(current_states, in_line_state, c_id)
                else:
                    territory_type = "COUNTRY"
                    territory_id = c_id
                    ter_name = in_line_country
                c = get_cases_in_line(territory_id, territory_type, line, current_cases)
                new_cases.extend(c)
    return new_cases    


async def update_country(c_type: any, db: any, PATH: list):
    country_data_col = db.get_collection('Country' if (c_type == "POSITIVE") else "CountryDeath")
    prev_countries = await country_data_col.find().to_list(length = COUNTRY_LENGTH)
    print('Prev ' , c_type, ' countries count: ', len(prev_countries))
    new_countries = update_live_data_countries(c_type, prev_countries, PATH[0])
    print('New ' , c_type, ' countries: ', new_countries)
    for c in new_countries:
        country_data_col.insert_one({
            'name': c
        })


async def update_state(c_type, db, PATH):
    state_data_col = db.get_collection('State' if (c_type == "POSITIVE") else 'StateDeath')
    prev_states = await state_data_col.find().to_list(length = STATE_LENGTH)
    current_countries = await db.get_collection('Country' if (c_type == "POSITIVE") else 'CountryDeath').find().to_list(length = COUNTRY_LENGTH)
    new_states = update_live_data_states(c_type, current_countries, prev_states, PATH)
    print('New ' , c_type, ' states count: ', len(new_states))
    print('New ' , c_type, ' states: ', new_states)
    for s in new_states:
        state_data_col.insert_one({
            'country_id': s['country_id'],
            'name': s['name']
        })


async def update_county(c_type, db, PATH):
    county_data_col = db.get_collection('County' if (c_type == "POSITIVE") else 'CountyDeath')
    prev_counties = await county_data_col.find().to_list(length = COUNTY_LENGTH)
    current_countries = await db.get_collection('Country' if (c_type == "POSITIVE") else 'CountryDeath').find().to_list(length = COUNTRY_LENGTH)
    current_states = await db.get_collection('State' if (c_type == "POSITIVE") else 'StateDeath').find().to_list(length = STATE_LENGTH)

    print('Previous ' , c_type, ' counties count: ', len(prev_counties))
    new_counties = update_live_data_counties(c_type, current_countries, current_states, prev_counties, PATH[1])
    print('New ' , c_type, ' counties count: ', len(new_counties))
    for s in new_counties:
        county_data_col.insert_one({
            'state_id': s['state_id'],
            'name': s['name']
        })


def update_live_data_countries(c_type: any, prev_countries: list, PATH: str) -> list:
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


def update_live_data_states(c_type: any,current_countries: any, prev_states: any, paths: any):
    state_update = []
    for i,p in enumerate(paths):
        with open( p, 'r' ) as the_file:
            reader = csv.DictReader(the_file)
            for line in reader:
                if is_state(c_type, line, p):
                    in_line_state = line['Province/State'] if i == 0 else line['Province_State']
                    in_line_country = line['Country/Region'] if i == 0 else line['Country_Region']
                    in_line_country_id = find_country_id(current_countries, in_line_country)

                    if new_state(current_countries, prev_states, in_line_state, in_line_country) and not_added_state(state_update, in_line_country_id,in_line_state):
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


def update_live_data_counties(c_type: any, current_countries, current_states, prev_counties, PATH):
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

    while x < datetime.now() and x.strftime("%-m/%-d/%y") in line.keys():
        f_date = x.strftime("%-m/%-d/%y")
        c_dict = dict()
        c_dict['no'] = line[f_date]
        c_dict['timestamp'] = f_date
        c_dict['territory_type'] = territory_type
        c_dict['territory_id'] = territory_id
        if new_cases(update_cases, c_dict):
            cases.append(c_dict)

        x += timedelta(days=1)  

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


def is_state(c_type, line, path):
    if path == (os.getenv('PATH_GLOBAL_POSITIVE') if (c_type == "POSITIVE") else os.getenv('PATH_GLOBAL_DEATH')):
        return 'Province/State' in line.keys() and line['Province/State'] != ""
    else:
        return 'Province_State' in line.keys() and line['Province_State'] != ""

if __name__ == "__main__":
    # execute only if run as a script
    main()