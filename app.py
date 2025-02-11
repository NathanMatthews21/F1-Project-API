from flask import Flask, jsonify, request
import mysql.connector

app = Flask(__name__)

# Database connection function
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        user='f1user',             # Update if necessary
        password='sWorddfg£@215g', # Update if necessary
        database='f1data'          # Ensure this is correct
    )

# 🔹 1. Get available seasons
@app.route('/api/f1/seasons.json')
def get_seasons():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT DISTINCT year AS season FROM races ORDER BY season DESC;")
    seasons = [{'season': str(row['season']), 'url': f'/api/f1/{row["season"]}.json'} for row in cursor.fetchall()]
    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "SeasonTable": {"Seasons": seasons}
        }
    })

# 🔹 2. Get constructors per season
@app.route('/api/f1/<int:season>/constructors.json')
def get_constructors(season):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT constructors.constructorId, constructors.name
        FROM constructors
        JOIN results ON constructors.constructorId = results.constructorId
        JOIN races ON results.raceId = races.raceId
        WHERE races.year = %s;
    """, (season,))
    
    constructors = [{'constructorId': row['constructorId'], 'name': row['name']} for row in cursor.fetchall()]
    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "ConstructorTable": {"Constructors": constructors}
        }
    })

# 🔹 3. Get drivers per season
@app.route('/api/f1/<int:season>/drivers.json')
def get_drivers(season):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("""
        SELECT DISTINCT drivers.driverId, drivers.forename AS givenName, drivers.surname AS familyName
        FROM drivers
        JOIN results ON drivers.driverId = results.driverId
        JOIN races ON results.raceId = races.raceId
        WHERE races.year = %s;
    """, (season,))

    drivers = [{'driverId': row['driverId'], 'givenName': row['givenName'], 'familyName': row['familyName']} for row in cursor.fetchall()]
    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "DriverTable": {"Drivers": drivers}
        }
    })

# 🔹 4. Get race results per season and round
@app.route('/api/f1/<int:season>/<int:round>/results.json')
def get_race_results(season, round):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT 
            races.name AS raceName, 
            races.round AS raceRound, 
            circuits.name AS circuitName, 
            results.position, 
            results.points, 
            COALESCE(results.time, 'N/A') AS raceTime,  
            COALESCE(status.status, 'Unknown') AS status,  
            drivers.driverId,  
            drivers.forename AS givenName, 
            drivers.surname AS familyName, 
            constructors.name AS constructorName 
        FROM results
        JOIN races ON results.raceId = races.raceId
        JOIN circuits ON races.circuitId = circuits.circuitId
        JOIN drivers ON results.driverId = drivers.driverId
        JOIN constructors ON results.constructorId = constructors.constructorId
        LEFT JOIN status ON results.statusId = status.statusId  
        WHERE races.year = %s AND races.round = %s;
    """, (season, round))

    results = []
    race_name = "Unknown"
    race_round = "Unknown"

    for row in cursor.fetchall():
        race_name = row["raceName"]
        race_round = row["raceRound"]

        results.append({
            "position": row["position"],
            "points": row["points"],
            "raceTime": row["raceTime"], 
            "status": row["status"],  
            "Driver": {
                "driverId": row["driverId"],
                "givenName": row["givenName"],
                "familyName": row["familyName"]
            },
            "Constructor": {
                "name": row["constructorName"]
            }
        })

    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "RaceTable": {
                "season": str(season),
                "round": str(race_round),
                "raceName": race_name,  # Ensure race name is included
                "Races": [{
                    "raceName": race_name,
                    "Circuit": {"circuitName": row["circuitName"] if results else "Unknown"},
                    "Results": results
                }]
            }
        }
    })

# 🔹 5. Get constructor standings per season and round
@app.route('/api/f1/<int:season>/<int:round>/constructorStandings.json')
def get_constructor_standings(season, round):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("""
        SELECT constructorstandings.constructorId, constructors.name, constructorstandings.points
        FROM constructorstandings
        JOIN constructors ON constructorstandings.constructorId = constructors.constructorId
        JOIN races ON constructorstandings.raceId = races.raceId
        WHERE races.year = %s AND races.round = %s;
    """, (season, round))

    standings = [{'Constructor': {"constructorId": row["constructorId"], "name": row["name"]}, "points": row["points"]} for row in cursor.fetchall()]
    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "StandingsTable": {
                "season": str(season),
                "round": str(round),
                "StandingsLists": [{"ConstructorStandings": standings}]
            }
        }
    })

# 🔹 6. Get all races in a season
@app.route('/api/f1/<int:season>.json')
def get_season_races(season):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("""
        SELECT round, name 
        FROM races 
        WHERE year = %s ORDER BY round ASC;
    """, (season,))

    races = [{"round": row["round"], "raceName": row["name"]} for row in cursor.fetchall()]
    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "RaceTable": {
                "season": str(season),
                "Races": races
            }
        }
    })

@app.route('/api/f1/<int:season>/<int:round>/driverStandings.json')
def get_driver_standings(season, round):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT drivers.driverId, drivers.forename AS givenName, drivers.surname AS familyName, 
               COALESCE(ds.points, 0) AS totalPoints
        FROM drivers
        JOIN driverstandings ds ON ds.driverId = drivers.driverId
        JOIN races ON ds.raceId = races.raceId
        WHERE races.year = %s AND races.round = %s
        ORDER BY ds.points DESC;
    """, (season, round))

    standings = [
        {
            "Driver": {
                "driverId": row["driverId"],
                "givenName": row["givenName"],
                "familyName": row["familyName"]
            },
            "points": row["totalPoints"]
        } for row in cursor.fetchall()
    ]

    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "StandingsTable": {
                "season": str(season),
                "round": str(round),
                "StandingsLists": [{"DriverStandings": standings}]
            }
        }
    })

if __name__ == '__main__':
    app.run(debug=True, port=8000)