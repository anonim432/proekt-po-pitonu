import requests
import sqlite3
import time

CITY = 'Ярославль'
DB_NAME = 'organizations.db'


def create_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS organizations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            address TEXT,
            lat REAL,
            lon REAL,
            type TEXT,
            UNIQUE(name, address, lat, lon, type)
        )
    ''')
    conn.commit()
    conn.close()


def search_organizations_overpass(city, category):
    category_mapping = {
        'кафе': 'amenity=cafe',
        'магазин': 'shop=supermarket',
        'аптека': 'amenity=pharmacy',
        'школа': 'amenity=school',
        'музей': 'tourism=museum'
    }
    tag = category_mapping.get(category, 'amenity=yes')
    overpass_query = f"""
    [out:json][timeout:45];
    area[name="{city}"]->.searchArea;
    (
      node[{tag}](area.searchArea);
      way[{tag}](area.searchArea);
    );
    out center;
    """
    url = "https://overpass-api.de/api/interpreter"
    try:
        print(f"Ищем {category} в {city}...")
        response = requests.post(url, data={'data': overpass_query}, timeout=120)
        response.raise_for_status()

        data = response.json()
        organizations = []
        for element in data.get('elements', []):
            element_tags = element.get('tags', {})
            name = element_tags.get('name', '').strip()
            if not name:
                continue
            if 'lat' in element and 'lon' in element:
                lat, lon = element['lat'], element['lon']
            elif 'center' in element:
                lat, lon = element['center']['lat'], element['center']['lon']
            else:
                continue
            address_parts = []
            if element_tags.get('addr:street'):
                street = element_tags.get('addr:street', '')
                housenumber = element_tags.get('addr:housenumber', '')
                address_parts.append(f"{street} {housenumber}".strip())
            address = ', '.join(address_parts) if address_parts else city
            organizations.append({
                'name': name,
                'address': address,
                'lat': lat,
                'lon': lon,
                'type': category
            })
        print(f"Найдено {len(organizations)} {category} с названиями")
        return organizations
    except Exception as e:
        print(f"Ошибка для {category}: {e}")
        return []

def save_to_database(organizations):
    if not organizations:
        print("Нет данных для сохранения")
        return
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    saved_count = 0
    for org in organizations:
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO organizations (name, address, lat, lon, type)
                VALUES (?, ?, ?, ?, ?)
            ''', (org['name'], org['address'], org['lat'], org['lon'], org['type']))
            saved_count += 1
        except Exception as e:
            print(f"Ошибка при сохранении {org['name']}: {e}")
    conn.commit()
    conn.close()

def main():
    create_database()
    categories = ['кафе', 'магазин', 'музей', 'школа', 'аптека']
    all_organizations = []
    for category in categories:
        orgs = search_organizations_overpass(CITY, category)
        all_organizations.extend(orgs)
        time.sleep(3)
    print(f"\nВсего найдено организаций: {len(all_organizations)}")
    save_to_database(all_organizations)

if __name__ == "__main__":
    main()