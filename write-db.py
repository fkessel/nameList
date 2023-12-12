import csv, os, time, uuid
import sqlite3

def parseNames(file: str) -> list:
    try:
        with open(file , newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            data = []
            for row in reader:
                data.append({
                    'vorname': row['vorname'],
                    'anzahl': int(row['anzahl']),
                    'geschlecht': row['geschlecht'],
                    'position': int(row['position'])
                })
    except csv.Error:
        with open(file , newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=';')
            data = []
            for row in reader:
                data.append({
                    'vorname': row['vorname'],
                    'anzahl': int(row['anzahl']),
                    'geschlecht': row['geschlecht'],
                    'position': int(row['position'])
                })
    return data

def parseDirectory(directory: str) -> list:
    with os.scandir(directory) as files:
        data = []
        for file in files:
            if file.name.endswith('.csv'):
                data.append(parseNames(str(file.path)))
        return data

def writeToDatabase(data: list, path: str) -> None:
    createDatabase(path)
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    start = time.time()

    for sublist in data:
        cursor.execute('BEGIN TRANSACTION;')

        for entry in sublist:
            if entry['anzahl'] >= 3:
                name = entry['vorname']
                amount = entry['anzahl']
                position = entry['position']
                gender = entry['geschlecht']
                name_type = 'first_name'

                cursor.execute("SELECT uuid FROM names WHERE name = ? AND name_type = ?", (name, name_type))
                existing_entry = cursor.fetchone()

                if existing_entry:
                    name_uuid = existing_entry[0]

                    cursor.execute("SELECT id FROM name_positions WHERE name_uuid = ? AND position = ?", (name_uuid, position))
                    existing_position_entry = cursor.fetchone()

                    if existing_position_entry:
                        cursor.execute("UPDATE name_positions SET amount = amount + ? WHERE id = ?",
                                        (amount, existing_position_entry[0]))
                    else:
                        cursor.execute("INSERT INTO name_positions (name_uuid, position, amount) \
                                        VALUES (?, ?, ?)", (name_uuid, position, amount))
                else:
                    random_uuid = str(uuid.uuid4())
                    cursor.execute("INSERT INTO names (uuid, name, name_type, gender) \
                                    VALUES (?, ?, ?, ?)", (random_uuid, name, name_type, gender))

                    cursor.execute("INSERT INTO name_positions (name_uuid, position, amount) \
                                    VALUES (?, ?, ?)", (random_uuid, position, amount))

        cursor.execute('COMMIT TRANSACTION;')
    
    ende = time.time()
    minuten = int(((ende - start) // 60)).__floor__
    print(f"Die Ausführung dauerte: {ende-start} Sekunden bzw. {minuten}:{int((ende - start) % 60).__floor__} Minuten")

    conn.commit()
    conn.close()

def createDatabase(path: str) -> None:
    if not os.path.exists(path):
        conn = sqlite3.connect(path)
        cursor = conn.cursor()
                
        cursor.execute('PRAGMA foreign_keys = off;')
        cursor.execute('BEGIN TRANSACTION;')
        cursor.execute('''
            CREATE TABLE names (
                uuid TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                name_type TEXT NOT NULL,
                gender TEXT
            );
        ''')
        cursor.execute('''
            CREATE TABLE name_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name_uuid TEXT NOT NULL,
                position INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                FOREIGN KEY (name_uuid) REFERENCES names (uuid)
            );
        ''')
        cursor.execute('COMMIT TRANSACTION;')
        cursor.execute('PRAGMA foreign_keys = on;')

        conn.commit()
        conn.close()

def main() -> int:
    result = parseDirectory(r'C:\Users\...\names\Berlin\2022')
    path = r'C:\Users\...\names\names2.db'.replace('\\', '/')
    writeToDatabase(result, path)
    return 0


# Execute when the module is not initialized from an import statement.
if __name__ == '__main__':
    main()
