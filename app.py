import os
import fastf1
from dotenv import load_dotenv
from flask import Flask, jsonify, request
import mysql.connector
import unicodedata

load_dotenv(".env")

app = Flask(__name__)

# Database connection function
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")       
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
            races.raceId          AS db_race_id,
            races.name            AS raceName,
            races.round           AS raceRound,
            circuits.name         AS circuitName,
            results.position,
            results.points,
            COALESCE(status.status, 'Unknown') AS status,
            drivers.driverId,
            drivers.forename      AS givenName,
            drivers.surname       AS familyName,
            constructors.name     AS constructorName
        FROM results
        JOIN races        ON results.raceId       = races.raceId
        JOIN circuits     ON races.circuitId      = circuits.circuitId
        JOIN drivers      ON results.driverId     = drivers.driverId
        JOIN constructors ON results.constructorId = constructors.constructorId
        LEFT JOIN status  ON results.statusId     = status.statusId
        WHERE races.year  = %s
          AND races.round = %s
    """, (season, round))

    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    results_list = []
    db_race_id = None
    race_name = "Unknown"
    race_round = "Unknown"

    if rows:
        db_race_id = rows[0]["db_race_id"]  # store the numeric raceId
        race_name = rows[0]["raceName"]
        race_round = rows[0]["raceRound"]

    for row in rows:
        results_list.append({
            "position": row["position"],
            "points":   row["points"],
            "status":   row["status"],
            "Driver": {
                "driverId":   row["driverId"],
                "givenName":  row["givenName"],
                "familyName": row["familyName"]
            },
            "Constructor": {
                "name": row["constructorName"]
            }
        })

    return jsonify({
        "MRData": {
            "series": "f1",
            "RaceTable": {
                "season": str(season),
                "round": str(race_round),
                "raceId": db_race_id,    # <--- include raceId in the JSON
                "raceName": race_name,
                "Races": [
                    {
                        "raceName": race_name,
                        "season": str(season),
                        "round": str(race_round),
                        "Circuit": {
                            "circuitName": rows[0]["circuitName"] if rows else "Unknown"
                        },
                        "Results": results_list
                    }
                ]
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

# 🔹 7. Get driver standings per season
@app.route('/api/f1/<int:season>/driverResultsTable.json')
def get_driver_results_table(season):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    # Fetch race rounds and names for the season
    cursor.execute("""
        SELECT round, name 
        FROM races 
        WHERE year = %s 
        ORDER BY round ASC;
    """, (season,))
    
    races = {row["round"]: row["name"] for row in cursor.fetchall()}

    # Fetch driver positions per race
    cursor.execute("""
        SELECT d.driverId, d.forename AS givenName, d.surname AS familyName, 
               r.round, COALESCE(res.position, 'Ret') AS position
        FROM results res
        JOIN drivers d ON res.driverId = d.driverId
        JOIN races r ON res.raceId = r.raceId
        WHERE r.year = %s
        ORDER BY r.round ASC;
    """, (season,))

    driver_data = {}
    
    for row in cursor.fetchall():
        driver_id = row["driverId"]
        if driver_id not in driver_data:
            driver_data[driver_id] = {
                "Driver": {
                    "driverId": driver_id,
                    "givenName": row["givenName"],
                    "familyName": row["familyName"]
                },
                "Races": {race_round: "" for race_round in races.keys()},
                "TotalPoints": 0
            }
        driver_data[driver_id]["Races"][row["round"]] = row["position"]

    # Fetch latest recorded cumulative points per driver
    cursor.execute("""
        SELECT ds.driverId, ds.points
        FROM driverstandings ds
        JOIN races r ON ds.raceId = r.raceId
        WHERE r.year = %s AND r.round = (
            SELECT MAX(round) FROM races WHERE year = %s
        )
        ORDER BY ds.points DESC;
    """, (season, season))

    sorted_driver_results = []
    for row in cursor.fetchall():
        driver_id = row["driverId"]
        if driver_id in driver_data:
            driver_data[driver_id]["TotalPoints"] = row["points"]
            sorted_driver_results.append(driver_data[driver_id])
    
    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "StandingsTable": {
                "season": str(season),
                "Races": races,
                "DriverResults": sorted_driver_results
            }
        }
    })

# 🔹 8. Get driver standings per season and round
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

# 🔹 9. Get constructor standings per season
@app.route('/api/f1/<int:season>/constructorResultsTable.json')
def get_constructor_results_table(season):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    # 1) Gather all rounds for that season
    cursor.execute("""
        SELECT round, name
        FROM races
        WHERE year = %s
        ORDER BY round ASC;
    """, (season,))
    races = {row["round"]: row["name"] for row in cursor.fetchall()}

    # 2) Fetch ALL results for each constructor, for each round (one row per driver)
    cursor.execute("""
        SELECT c.constructorId,
               c.name AS constructorName,
               r.round,
               COALESCE(res.position, 'Ret') AS position,
               res.points
        FROM results res
        JOIN constructors c ON res.constructorId = c.constructorId
        JOIN races r        ON res.raceId       = r.raceId
        WHERE r.year = %s
        ORDER BY r.round ASC;
    """, (season,))

    # Build a data structure so that for each constructor:
    #   constructor_data[constructorId]["Races"][round] is a list of positions from each driver
    constructor_data = {}
    for row in cursor.fetchall():
        constructor_id = row["constructorId"]
        round_num      = row["round"]
        if constructor_id not in constructor_data:
            constructor_data[constructor_id] = {
                "Constructor": {
                    "constructorId": constructor_id,
                    "name": row["constructorName"]
                },
                # Make each round an empty list so we can store multiple positions
                "Races": {rnd: [] for rnd in races.keys()},
                "TotalPoints": 0
            }
        constructor_data[constructor_id]["Races"][round_num].append(row["position"])

    # 3) Now pull official final points from constructorStandings for sorting
    cursor.execute("""
        SELECT cs.constructorId, cs.points
        FROM constructorstandings cs
        JOIN races r ON cs.raceId = r.raceId
        WHERE r.year = %s
          AND r.round = (
              SELECT MAX(round)
              FROM races
              WHERE year = %s
          )
        ORDER BY cs.points DESC;
    """, (season, season))

    sorted_constructor_results = []
    for row in cursor.fetchall():
        cid = row["constructorId"]
        if cid in constructor_data:
            constructor_data[cid]["TotalPoints"] = row["points"]
            sorted_constructor_results.append(constructor_data[cid])

    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "StandingsTable": {
                "season": str(season),
                "Races": races,  # e.g. {1: "Bahrain GP", 2: "Saudi Arabian GP", ...}
                "ConstructorResults": sorted_constructor_results
            }
        }
    })

# 🔹 10. Get all drivers
@app.route('/api/f1/drivers/all.json')
def get_all_drivers():
    """
    Returns a list of all drivers in the database, 
    e.g., [ { 'driverId': 'hamilton', 'fullName': 'Lewis Hamilton'}, ... ]
    """
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT driverId, CONCAT(forename, ' ', surname) AS fullName
        FROM drivers
        ORDER BY surname ASC;
    """)
    drivers = cursor.fetchall()

    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "DriverTable": drivers
        }
    })

# 🔹 11. Get all constructors
@app.route('/api/f1/constructors/all.json')
def get_all_constructors():
    """
    Returns a list of all constructors in the database, 
    e.g., [ { 'constructorId': 'mercedes', 'name': 'Mercedes'}, ... ]
    """
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT constructorId, name
        FROM constructors
        ORDER BY name ASC;
    """)
    constructors = cursor.fetchall()

    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "ConstructorTable": constructors
        }
    })

# 🔹 12. Get drivers who participated in a given year range
@app.route('/api/f1/drivers/range')
def get_drivers_in_year_range():
    start_year = int(request.args.get('startYear', 1950))
    end_year = int(request.args.get('endYear', 2025))

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT DISTINCT 
            d.driverRef AS driverId,
            d.forename,
            d.surname,
            CONCAT(d.forename, ' ', d.surname) AS fullName
        FROM drivers d
        JOIN results r ON d.driverId = r.driverId
        JOIN races ra ON r.raceId = ra.raceId
        WHERE ra.year BETWEEN %s AND %s
        ORDER BY d.surname ASC, d.forename ASC;
    """, (start_year, end_year))

    drivers = cursor.fetchall()
    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "DriverTable": drivers
        }
    })

# 🔹 13. Get constructors who participated in a given year range
@app.route('/api/f1/constructors/range')
def get_constructors_in_year_range():
    """
    Example: /api/f1/constructors/range?startYear=2018&endYear=2020
    Returns a list of constructors who participated in any race between 2018 and 2020.
    """
    start_year = int(request.args.get('startYear', 1958))
    end_year = int(request.args.get('endYear', 2025))

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT DISTINCT c.constructorId, c.name
        FROM constructors c
        JOIN results r ON c.constructorId = r.constructorId
        JOIN races ra ON r.raceId = ra.raceId
        WHERE ra.year BETWEEN %s AND %s
        ORDER BY c.name ASC;
    """, (start_year, end_year))
    constructors = cursor.fetchall()

    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "ConstructorTable": constructors
        }
    })

# 🔹 14. Multi-year Driver Comparison
@app.route('/api/f1/multiYearDriverComparison')
def multi_year_driver_comparison():
    """
    Usage:
      /api/f1/multiYearDriverComparison?drivers=hamilton,alonso&startYear=2018&endYear=2020&metric=avgFinish
    """
    drivers_param = request.args.get('drivers')
    start_year = int(request.args.get('startYear', 1950))
    end_year = int(request.args.get('endYear', 2050))
    metric = request.args.get('metric', 'totalPoints')

    if not drivers_param:
        return jsonify({"error": "No drivers provided"}), 400

    driver_ids = [d.strip() for d in drivers_param.split(',') if d.strip()]
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    results = {}
    # For each driver, gather year range
    cursor.execute("""
        SELECT DISTINCT year
        FROM races
        WHERE year BETWEEN %s AND %s
        ORDER BY year ASC;
    """, (start_year, end_year))
    all_years = [row["year"] for row in cursor.fetchall()]

    for driver in driver_ids:
        results[driver] = {
            "driverId": driver,
            "yearlyPoints": [],
            "totalPoints": 0
        }
        for y in all_years:
            val = compute_driver_metric(cursor, driver, y, metric)
            results[driver]["yearlyPoints"].append({"year": y, "points": val})
            results[driver]["totalPoints"] += val

    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "startYear": start_year,
            "endYear": end_year,
            "MultiYearDriverComparison": results
        }
    })

def compute_driver_metric(cursor, driverRefOrId, year, metric):
    """Compute the chosen metric for one driver in one year."""
    if metric == "totalPoints":
        # Summation of points from driverstandings
        cursor.execute("""
            SELECT MAX(ds.points) AS val
            FROM driverstandings ds
            JOIN races r ON ds.raceId = r.raceId
            JOIN drivers d ON ds.driverId = d.driverId
            WHERE (d.driverRef = %s OR d.driverId = %s)
              AND r.year = %s
        """, (driverRefOrId, driverRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "avgFinish":
        # average finishing position (exclude non-numeric positions, i.e. 'Ret')
        cursor.execute("""
            SELECT AVG(CASE WHEN res.position REGEXP '^[0-9]+$' THEN res.position+0 END) AS val
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN drivers d ON res.driverId = d.driverId
            WHERE (d.driverRef = %s OR d.driverId = %s)
              AND r.year = %s
        """, (driverRefOrId, driverRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "dnfs":
        # number of DNFs -> check status or position
        cursor.execute("""
            SELECT COUNT(*) AS val
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN drivers d ON res.driverId = d.driverId
            LEFT JOIN status s ON res.statusId = s.statusId
            WHERE (d.driverRef = %s OR d.driverId = %s)
              AND r.year = %s
              AND (
                s.status LIKE 'Ret%' OR s.status IN ('Crash','Engine','Accident')
                OR res.position = 'Ret'
                OR res.position REGEXP '[^0-9]+'
              )
        """, (driverRefOrId, driverRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"])

    elif metric == "avgQual":
        # average grid position
        cursor.execute("""
            SELECT AVG(NULLIF(res.grid, 0)) AS val
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN drivers d ON res.driverId = d.driverId
            WHERE (d.driverRef = %s OR d.driverId = %s)
              AND r.year = %s
        """, (driverRefOrId, driverRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "wins":
        # finishing position = 1
        cursor.execute("""
            SELECT COUNT(*) AS val
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN drivers d ON res.driverId = d.driverId
            WHERE (d.driverRef = %s OR d.driverId = %s)
              AND r.year = %s
              AND res.position = '1'
        """, (driverRefOrId, driverRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "avgPointsPerRace":
        cursor.execute("""
            SELECT SUM(res.points) AS totalPts, COUNT(*) AS raceCount
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN drivers d ON res.driverId = d.driverId
            WHERE (d.driverRef = %s OR d.driverId = %s)
              AND r.year = %s
        """, (driverRefOrId, driverRefOrId, year))
        row = cursor.fetchone()
        if row["raceCount"] == 0:
            return 0
        return float(row["totalPts"] or 0) / float(row["raceCount"])

    return 0

#🔹 15. Multi-year Constructor Comparison
@app.route('/api/f1/multiYearConstructorComparison')
def multi_year_constructor_comparison():
    """
    /api/f1/multiYearConstructorComparison?teams=mercedes,ferrari&startYear=2018&endYear=2020&metric=dnfs
    """
    teams_param = request.args.get('teams')
    start_year = int(request.args.get('startYear', 1958))
    end_year = int(request.args.get('endYear', 2050))
    metric = request.args.get('metric', 'totalPoints')

    if not teams_param:
        return jsonify({"error": "No teams provided"}), 400

    team_ids = [t.strip() for t in teams_param.split(',') if t.strip()]

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    # gather all relevant years in [start_year, end_year]
    cursor.execute("""
        SELECT DISTINCT year
        FROM races
        WHERE year BETWEEN %s AND %s
        ORDER BY year ASC;
    """, (start_year, end_year))
    all_years = [row["year"] for row in cursor.fetchall()]

    results = {}
    for team in team_ids:
        results[team] = {
            "constructorId": team,
            "yearlyPoints": [],
            "totalPoints": 0
        }
        for y in all_years:
            val = compute_constructor_metric(cursor, team, y, metric)
            results[team]["yearlyPoints"].append({"year": y, "points": val})
            results[team]["totalPoints"] += val

    cursor.close()
    connection.close()

    return jsonify({
        "MRData": {
            "series": "f1",
            "startYear": start_year,
            "endYear": end_year,
            "MultiYearConstructorComparison": results
        }
    })


def compute_constructor_metric(cursor, constructorRefOrId, year, metric):
    """
    Compute the chosen metric for a single constructor in a single year.
    """
    if metric == "totalPoints":
        # sum from constructorstandings for that year
        cursor.execute("""
            SELECT MAX(cs.points) AS val
            FROM constructorstandings cs
            JOIN races r ON cs.raceId = r.raceId
            JOIN constructors c ON cs.constructorId = c.constructorId
            WHERE (c.constructorRef = %s OR c.constructorId = %s)
              AND r.year = %s
        """, (constructorRefOrId, constructorRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "avgFinish":
        # average finishing position for all drivers in this constructor for each race
        # we'll assume results has a numeric 'position' or 'Ret'
        cursor.execute("""
            SELECT AVG(CASE WHEN res.position REGEXP '^[0-9]+$' THEN res.position+0 END) AS val
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN constructors c ON res.constructorId = c.constructorId
            WHERE (c.constructorRef = %s OR c.constructorId = %s)
              AND r.year = %s
        """, (constructorRefOrId, constructorRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "dnfs":
        # number of DNFs among all drivers in that constructor
        cursor.execute("""
            SELECT COUNT(*) AS val
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN constructors c ON res.constructorId = c.constructorId
            LEFT JOIN status s ON res.statusId = s.statusId
            WHERE (c.constructorRef = %s OR c.constructorId = %s)
              AND r.year = %s
              AND (
                s.status LIKE 'Ret%'
                OR s.status IN ('Crash','Engine','Accident')
                OR res.position = 'Ret'
                OR res.position REGEXP '[^0-9]+'
              )
        """, (constructorRefOrId, constructorRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "avgQual":
        # average grid for all constructor's cars
        cursor.execute("""
            SELECT AVG(NULLIF(res.grid, 0)) AS val
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN constructors c ON res.constructorId = c.constructorId
            WHERE (c.constructorRef = %s OR c.constructorId = %s)
              AND r.year = %s
        """, (constructorRefOrId, constructorRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "wins":
        # count how many times any driver for this constructor finished 1st
        cursor.execute("""
            SELECT COUNT(*) AS val
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN constructors c ON res.constructorId = c.constructorId
            WHERE (c.constructorRef = %s OR c.constructorId = %s)
              AND r.year = %s
              AND res.position = '1'
        """, (constructorRefOrId, constructorRefOrId, year))
        row = cursor.fetchone()
        return float(row["val"] or 0)

    elif metric == "avgPointsPerRace":
        # total points / total races for that constructor
        cursor.execute("""
            SELECT SUM(res.points) AS totalPts, COUNT(*) AS raceCount
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN constructors c ON res.constructorId = c.constructorId
            WHERE (c.constructorRef = %s OR c.constructorId = %s)
              AND r.year = %s
        """, (constructorRefOrId, constructorRefOrId, year))
        row = cursor.fetchone()
        if not row["raceCount"]:
            return 0
        return float(row["totalPts"] or 0) / float(row["raceCount"])

    return 0

# 🔹 16. Get qualifying results for a specific season and round
@app.route('/api/f1/<int:season>/<int:round>/qualifying.json')
def get_qualifying_results(season, round):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("""
        SELECT
            races.name       AS raceName,
            races.round      AS raceRound,
            circuits.name    AS circuitName,
            qualifying.position AS qualPosition,
            qualifying.q1,
            qualifying.q2,
            qualifying.q3,
            drivers.driverId,         -- Return numeric driverId
            drivers.forename  AS givenName,
            drivers.surname   AS familyName,
            constructors.name AS constructorName
        FROM qualifying
        JOIN races        ON qualifying.raceId       = races.raceId
        JOIN circuits     ON races.circuitId         = circuits.circuitId
        JOIN drivers      ON qualifying.driverId     = drivers.driverId
        JOIN constructors ON qualifying.constructorId = constructors.constructorId
        WHERE races.year  = %s
          AND races.round = %s
        ORDER BY qualifying.position
    """, (season, round))

    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    results = []
    race_name = "Unknown"
    race_round = "Unknown"

    for row in rows:
        race_name = row["raceName"]
        race_round = row["raceRound"]
        results.append({
            "position": row["qualPosition"],
            "q1":       row["q1"] or "N/A",
            "q2":       row["q2"] or "N/A",
            "q3":       row["q3"] or "N/A",
            "Driver": {
                "driverId":   row["driverId"],   # Use driverId
                "givenName":  row["givenName"],
                "familyName": row["familyName"]
            },
            "Constructor": {
                "name": row["constructorName"]
            }
        })

    return jsonify({
        "MRData": {
            "series": "f1",
            "QualifyingTable": {
                "season": str(season),
                "round":  str(race_round),
                "raceName": race_name,
                "Races": [
                    {
                        "raceName": race_name,
                        "season":   str(season),
                        "round":    str(race_round),
                        "Circuit": {
                            "circuitName": rows[0]["circuitName"] if rows else "Unknown"
                        },
                        "QualifyingResults": results
                    }
                ]
            }
        }
    })

# 🔹 17. Get sprint results for a specific season and round
@app.route('/api/f1/<int:season>/<int:round>/sprint.json')
def get_sprint_results(season, round):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            races.name        AS raceName,
            races.round       AS raceRound,
            circuits.name     AS circuitName,
            sr.position,
            sr.points,
            COALESCE(status.status, 'Unknown') AS sprintStatus,
            drivers.driverId,       -- numeric driverId
            drivers.forename  AS givenName,
            drivers.surname   AS familyName,
            constructors.name AS constructorName
        FROM sprintresults sr
        JOIN races        ON sr.raceId       = races.raceId
        JOIN circuits     ON races.circuitId = circuits.circuitId
        JOIN drivers      ON sr.driverId     = drivers.driverId
        JOIN constructors ON sr.constructorId = constructors.constructorId
        LEFT JOIN status  ON sr.statusId     = status.statusId
        WHERE races.year  = %s
          AND races.round = %s
        ORDER BY sr.position
    """, (season, round))

    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    results = []
    race_name = "Unknown"
    race_round = "Unknown"

    for row in rows:
        race_name = row["raceName"]
        race_round = row["raceRound"]
        results.append({
            "position": row["position"],
            "points":   row["points"],
            "status":   row["sprintStatus"],
            "Driver": {
                "driverId":   row["driverId"],   # Use driverId
                "givenName":  row["givenName"],
                "familyName": row["familyName"]
            },
            "Constructor": {
                "name": row["constructorName"]
            }
        })

    return jsonify({
        "MRData": {
            "series": "f1",
            "SprintTable": {
                "season": str(season),
                "round":  str(race_round),
                "raceName": race_name,
                "Races": [
                    {
                        "raceName": race_name,
                        "season":   str(season),
                        "round":    str(race_round),
                        "Circuit": {
                            "circuitName": rows[0]["circuitName"] if rows else "Unknown"
                        },
                        "SprintResults": results
                    }
                ]
            }
        }
    })

# 🔹 18. Get driver results for a specific season and round
@app.route('/api/f1/<int:season>/<int:round>/driverResults.json')
def get_driver_results_for_round(season, round):
    session_type = request.args.get('session', 'race')
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    if session_type == 'race':
        table_name   = 'results'
        position_col = 'results.position'
        points_col   = 'results.points'
        status_col   = 'COALESCE(status.status, "Unknown") AS sessionStatus'
        status_join  = 'LEFT JOIN status ON results.statusId = status.statusId'
    elif session_type == 'qualifying':
        table_name   = 'qualifying'
        position_col = 'qualifying.position'
        points_col   = '0 AS points'
        status_col   = '"Qualifying" AS sessionStatus'
        status_join  = ''
    elif session_type == 'sprint':
        table_name   = 'sprintresults'
        position_col = 'sprintresults.position'
        points_col   = 'sprintresults.points'
        status_col   = 'COALESCE(status.status, "Unknown") AS sessionStatus'
        status_join  = 'LEFT JOIN status ON sprintresults.statusId = status.statusId'
    else:
        cursor.close()
        connection.close()
        return jsonify({"error": "Unknown session type"}), 400

    query = f"""
        SELECT
            races.name       AS raceName,
            races.round      AS raceRound,
            circuits.name    AS circuitName,
            {position_col}   AS position,
            {points_col}     AS points,
            {status_col},
            drivers.driverId,  -- numeric driverId
            drivers.forename  AS givenName,
            drivers.surname   AS familyName,
            constructors.name AS constructorName
        FROM {table_name}
        JOIN races        ON {table_name}.raceId       = races.raceId
        JOIN circuits     ON races.circuitId           = circuits.circuitId
        JOIN drivers      ON {table_name}.driverId     = drivers.driverId
        JOIN constructors ON {table_name}.constructorId = constructors.constructorId
        {status_join}
        WHERE races.year  = %s
          AND races.round = %s
        ORDER BY {table_name}.position
    """

    cursor.execute(query, (season, round))
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    results = []
    if rows:
        for row in rows:
            results.append({
                "position": row["position"],
                "points":   row["points"],
                "status":   row["sessionStatus"],
                "Driver": {
                    "driverId":   row["driverId"],
                    "givenName":  row["givenName"],
                    "familyName": row["familyName"]
                },
                "Constructor": {
                    "name": row["constructorName"]
                }
            })

    return jsonify({
        "MRData": {
            "series": "f1",
            "DriverResults": results
        }
    })

# 🔹 19. Get all constructor standings for a specific season
@app.route('/api/f1/<int:season>/allConstructorStandings.json')
def get_all_constructor_standings(season):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT r.round, cs.constructorId, c.name AS constructorName, cs.points
        FROM constructorstandings cs
        JOIN races r ON cs.raceId = r.raceId
        JOIN constructors c ON cs.constructorId = c.constructorId
        WHERE r.year = %s
        ORDER BY r.round ASC;
    """, (season,))

    data = cursor.fetchall()
    cursor.close()
    connection.close()

    standings_by_round = {}
    for row in data:
        round_num = row['round']
        standings_by_round.setdefault(round_num, []).append({
            "constructorId": row["constructorId"],
            "constructorName": row["constructorName"],
            "points": float(row["points"])
        })

    return jsonify({"season": season, "standings": standings_by_round})

# 🔹 20. Get all driver standings for a specific season
@app.route('/api/f1/<int:season>/allDriverStandings.json')
def get_all_driver_standings(season):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT r.round, ds.driverId, d.forename, d.surname, ds.points
        FROM driverstandings ds
        JOIN races r ON ds.raceId = r.raceId
        JOIN drivers d ON ds.driverId = d.driverId
        WHERE r.year = %s
        ORDER BY r.round ASC;
    """, (season,))

    data = cursor.fetchall()
    cursor.close()
    connection.close()

    standings_by_round = {}
    for row in data:
        round_num = row['round']
        standings_by_round.setdefault(round_num, []).append({
            "driverId": row["driverId"],
            "givenName": row["forename"],
            "familyName": row["surname"],
            "points": float(row["points"])
        })

    return jsonify({"season": season, "standings": standings_by_round})


# 🔹 21. Get all laptimes for a specific season and round
@app.route('/api/f1/<int:season>/<int:round>/laptimes.json')
def get_laptimes_for_round(season, round):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    query = """
        SELECT
            lt.driverId,
            drivers.forename,
            drivers.surname,
            lt.lap,
            lt.position,
            lt.time,
            lt.milliseconds
        FROM laptimes lt
        JOIN races r      ON lt.raceId   = r.raceId
        JOIN drivers      ON lt.driverId = drivers.driverId
        WHERE r.year  = %s
          AND r.round = %s
        ORDER BY lt.lap ASC, lt.position ASC
    """
    cursor.execute(query, (season, round))
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    # Group lap times by driver
    lapData = {}
    for row in rows:
        fullName = f"{row['forename']} {row['surname']}"
        if fullName not in lapData:
            lapData[fullName] = {
                "driverName": fullName,
                "laps": []
            }
        lapData[fullName]["laps"].append({
            "lap": row["lap"],
            "time": row["time"]
        })
    for driver in lapData:
        lapData[driver]["laps"].sort(key=lambda x: x["lap"])

    return jsonify({
        "season": season,
        "round": round,
        "lapData": list(lapData.values())
    })

# 🔹 22. Get start/finish positions for a specific season and round
@app.route('/api/f1/<int:season>/<int:round>/startFinish.json')
def get_start_finish_positions(season, round):

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    query = """
        SELECT
            d.forename AS givenName,
            d.surname  AS familyName,
            results.grid AS startPos,
            results.position AS finishPos
        FROM results
        JOIN drivers d ON results.driverId = d.driverId
        JOIN races r   ON results.raceId   = r.raceId
        WHERE r.year   = %s
          AND r.round  = %s
        ORDER BY results.position;  -- optional: order by finishing position
    """
    cursor.execute(query, (season, round))
    rows = cursor.fetchall()

    data = []
    for row in rows:
        # handle finishing pos if numeric
        finish_str = row["finishPos"]
        start_pos = row["startPos"] or 0  # grid=0 sometimes means "pit lane start"
        if finish_str is None:
            # skip if no finishing data
            continue

        # check if finishing position is numeric, skip if not
        if not str(finish_str).isdigit():
            continue

        finish_pos = int(finish_str)
        start_pos = int(start_pos)

        position_change = start_pos - finish_pos
        driver_name = f"{row['givenName']} {row['familyName']}"

        data.append({
            "driverName": driver_name,
            "startPosition": start_pos,
            "finishPosition": finish_pos,
            "positionChange": position_change
        })

    cursor.close()
    connection.close()

    return jsonify(data)

# 🔹 23. Get head-to-head results for two drivers in a specific season
@app.route('/api/f1/<int:season>/headToHeadDrivers.json')
def head_to_head_drivers(season):
    driverA = request.args.get('driverA')
    driverB = request.args.get('driverB')

    try:
        driverA_id = int(driverA)
        driverB_id = int(driverB)
    except (TypeError, ValueError):
        return jsonify({"error": "Please provide valid numeric driver IDs"}), 400

    if not driverA_id or not driverB_id:
        return jsonify({"error": "Please provide driverA and driverB"}), 400

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT round, name
        FROM races
        WHERE year = %s
        ORDER BY round ASC
    """, (season,))
    rounds = cursor.fetchall()
    if not rounds:
        cursor.close()
        connection.close()
        return jsonify([])

    h2h_data = {}
    for rnd_info in rounds:
        rnd = rnd_info['round']
        race_name = rnd_info['name']
        h2h_data[rnd] = {
            "round": rnd,
            "raceName": race_name,
            "driverA": {"position": None, "points": None},
            "driverB": {"position": None, "points": None},
            "winner": None
        }

    query = """
        SELECT r.round, r.name AS raceName,
               d.driverId, d.forename, d.surname,
               results.position, results.points
        FROM results
        JOIN races r      ON results.raceId   = r.raceId
        JOIN drivers d    ON results.driverId = d.driverId
        WHERE r.year = %s
          AND d.driverId IN (%s, %s)
        ORDER BY r.round ASC
    """
    cursor.execute(query, (season, driverA_id, driverB_id))
    rows = cursor.fetchall()

    # Populate the data
    for row in rows:
        rnd = row["round"]
        if rnd not in h2h_data:
            continue
        pos = row["position"]
        pts = float(row["points"] or 0)
        this_driver_id = row["driverId"]

        if this_driver_id == driverA_id:
            h2h_data[rnd]["driverA"] = {"position": pos, "points": pts}
        elif this_driver_id == driverB_id:
            h2h_data[rnd]["driverB"] = {"position": pos, "points": pts}

    for rnd in h2h_data:
        da_pos = h2h_data[rnd]["driverA"]["position"]
        db_pos = h2h_data[rnd]["driverB"]["position"]
        if da_pos is not None and db_pos is not None:
            try:
                da_num = int(da_pos)
                db_num = int(db_pos)
                if da_num < db_num:
                    h2h_data[rnd]["winner"] = "driverA"
                elif db_num < da_num:
                    h2h_data[rnd]["winner"] = "driverB"
                else:
                    h2h_data[rnd]["winner"] = "tie"
            except ValueError:
                pass

    cursor.close()
    connection.close()

    result_array = [h2h_data[k] for k in sorted(h2h_data.keys())]
    return jsonify(result_array)

# 🔹 24. Get head-to-head results for two constructors in a specific season
@app.route('/api/f1/<int:season>/headToHeadConstructors.json')
def head_to_head_constructors(season):

    teamA = request.args.get('teamA')
    teamB = request.args.get('teamB')

    try:
        teamA_id = int(teamA)
        teamB_id = int(teamB)
    except (TypeError, ValueError):
        return jsonify({"error": "Please provide valid numeric constructor IDs (teamA / teamB)"}), 400

    if not teamA_id or not teamB_id:
        return jsonify({"error": "Please provide teamA and teamB"}), 400

    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)

    cursor.execute("""
        SELECT round, name
        FROM races
        WHERE year = %s
        ORDER BY round ASC
    """, (season,))
    rounds = cursor.fetchall()
    if not rounds:
        cursor.close()
        connection.close()
        return jsonify([])

    h2h_data = {}
    for rnd_info in rounds:
        rnd = rnd_info['round']
        race_name = rnd_info['name']
        h2h_data[rnd] = {
            "round": rnd,
            "raceName": race_name,
            "teamA": {"position": None, "points": None},
            "teamB": {"position": None, "points": None},
            "winner": None
        }

    query = """
        SELECT r.round, r.name AS raceName,
               c.constructorId, c.name AS constructorName,
               cs.position, cs.points
        FROM constructorstandings cs
        JOIN races r         ON cs.raceId       = r.raceId
        JOIN constructors c  ON cs.constructorId = c.constructorId
        WHERE r.year = %s
          AND c.constructorId IN (%s, %s)
        ORDER BY r.round ASC
    """
    cursor.execute(query, (season, teamA_id, teamB_id))
    rows = cursor.fetchall()

    for row in rows:
        rnd = row["round"]
        if rnd not in h2h_data:
            continue  # skip if not in known rounds
        pos = row["position"]
        pts = float(row["points"] or 0)
        constructor_id = row["constructorId"]

        if constructor_id == teamA_id:
            h2h_data[rnd]["teamA"] = {"position": pos, "points": pts}
        elif constructor_id == teamB_id:
            h2h_data[rnd]["teamB"] = {"position": pos, "points": pts}

    for rnd in h2h_data:
        a_pos = h2h_data[rnd]["teamA"]["position"]
        b_pos = h2h_data[rnd]["teamB"]["position"]
        if a_pos is not None and b_pos is not None:
            try:
                a_num = int(a_pos)
                b_num = int(b_pos)
                if a_num < b_num:
                    h2h_data[rnd]["winner"] = "teamA"
                elif b_num < a_num:
                    h2h_data[rnd]["winner"] = "teamB"
                else:
                    h2h_data[rnd]["winner"] = "tie"
            except ValueError:
                pass

    cursor.close()
    connection.close()

    result_array = [h2h_data[k] for k in sorted(h2h_data.keys())]
    return jsonify(result_array)


#  WHAT IF FEATURES (SAME TABLE (f1data))
# =====================================================================

# 1) Create new scenario
@app.route('/api/f1/whatif/newScenario', methods=['POST'])
def create_scenario():

    data = request.json
    scenario_name = data.get("scenarioName")
    season = data.get("season")
    if not scenario_name or not season:
        return jsonify({"error": "scenarioName and season are required"}), 400

    connection = get_db_connection()
    cursor = connection.cursor()

    cursor.execute("""
        DELETE FROM whatif_scenarios
        WHERE created_at < NOW() - INTERVAL 7 DAY
    """)

    cursor.execute("""
        INSERT INTO whatif_scenarios (scenario_name, season)
        VALUES (%s, %s)
    """, (scenario_name, season))
    scenario_id = cursor.lastrowid
    connection.commit()

    cursor.close()
    connection.close()

    return jsonify({"scenarioId": scenario_id})

# 2) Update race results for a scenario
@app.route('/api/f1/whatif/scenario/<int:scenario_id>/updateRaceResults', methods=['POST'])
def update_scenario_race_results(scenario_id):

    data = request.json
    race_id = data.get("raceId")
    results = data.get("results", [])

    if not race_id or not isinstance(results, list):
        return jsonify({"error": "raceId and an array of results are required"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    # Remove old overrides for this scenario+race
    cur.execute("""
        DELETE FROM whatif_results
        WHERE scenario_id = %s
          AND raceId = %s
    """, (scenario_id, race_id))

    # Insert new overrides
    for row in results:
        driver_id = row["driverId"]
        position  = row["position"]
        points    = row["points"]
        cur.execute("""
            INSERT INTO whatif_results (scenario_id, raceId, driverId, position, points)
            VALUES (%s, %s, %s, %s, %s)
        """, (scenario_id, race_id, driver_id, position, points))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"status": "ok"})

# 3) Get scenario info (optional convenience route)
@app.route('/api/f1/whatif/scenario/<int:scenario_id>', methods=['GET'])
def get_scenario_info(scenario_id):

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    # scenario info
    cur.execute("""
        SELECT * 
        FROM whatif_scenarios
        WHERE scenario_id = %s
    """, (scenario_id,))
    scenario = cur.fetchone()
    if not scenario:
        cur.close()
        conn.close()
        return jsonify({"error": "Scenario not found"}), 404

    # races overridden
    cur.execute("""
        SELECT DISTINCT raceId 
        FROM whatif_results
        WHERE scenario_id = %s
    """, (scenario_id,))
    race_ids = [row["raceId"] for row in cur.fetchall()]

    cur.close()
    conn.close()

    scenario["overriddenRaces"] = race_ids
    return jsonify(scenario)

# 4) Compute scenario-based driver standings
@app.route('/api/f1/whatif/scenario/<int:scenario_id>/driverStandings', methods=['GET'])
def get_scenario_driver_standings(scenario_id):

    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT * FROM whatif_scenarios WHERE scenario_id = %s", (scenario_id,))
    scenario = cur.fetchone()
    if not scenario:
        cur.close()
        conn.close()
        return jsonify({"error": "Scenario not found"}), 404

    season = scenario["season"]

    cur.execute("""
        SELECT raceId, year, round, name
        FROM races
        WHERE year = %s
        ORDER BY round ASC
    """, (season,))
    races = cur.fetchall()

    # Keep track of driver total points
    driver_points = {}
    driver_names  = {}

    for race in races:
        r_id    = race["raceId"]
        r_round = race["round"]

        # check overrides
        cur.execute("""
            SELECT driverId, position, points
            FROM whatif_results
            WHERE scenario_id = %s
              AND raceId = %s
            ORDER BY position
        """, (scenario_id, r_id))
        overridden = cur.fetchall()

        if overridden:
            # Scenario overrides in effect
            for row in overridden:
                d_id = row["driverId"]
                pts  = float(row["points"] or 0)
                driver_points[d_id] = driver_points.get(d_id, 0) + pts
                # We'll fetch driver name once for convenience
                if d_id not in driver_names:
                    # get the driver's name
                    name_sql = "SELECT forename, surname FROM drivers WHERE driverId = %s"
                    cur.execute(name_sql, (d_id,))
                    d_info = cur.fetchone()
                    if d_info:
                        driver_names[d_id] = f"{d_info['forename']} {d_info['surname']}"
        else:
            # Use real results
            cur.execute("""
                SELECT results.driverId, results.points, drivers.forename, drivers.surname
                FROM results
                JOIN drivers ON results.driverId = drivers.driverId
                WHERE results.raceId = %s
            """, (r_id,))
            real_rows = cur.fetchall()
            for row in real_rows:
                d_id = row["driverId"]
                pts  = float(row["points"] or 0)
                driver_points[d_id] = driver_points.get(d_id, 0) + pts
                if d_id not in driver_names:
                    driver_names[d_id] = f"{row['forename']} {row['surname']}"

    standings_array = []
    for d_id, pts in driver_points.items():
        standings_array.append({
            "driverId": d_id,
            "driverName": driver_names[d_id],
            "points": pts
        })

    # sort descending by points
    standings_array.sort(key=lambda x: x["points"], reverse=True)

    cur.close()
    conn.close()

    return jsonify({
        "scenarioId": scenario_id,
        "season": season,
        "driverStandings": standings_array
    })

# 5) Compute scenario-based constructor standings (similar logic)
@app.route('/api/f1/whatif/scenario/<int:scenario_id>/constructorStandings', methods=['GET'])
def get_scenario_constructor_standings(scenario_id):
    conn = get_db_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT * FROM whatif_scenarios WHERE scenario_id = %s", (scenario_id,))
    scenario = cur.fetchone()
    if not scenario:
        cur.close()
        conn.close()
        return jsonify({"error": "Scenario not found"}), 404

    season = scenario["season"]
    cur.execute("""
        SELECT raceId, year, round, name
        FROM races
        WHERE year = %s
        ORDER BY round ASC
    """, (season,))
    races = cur.fetchall()

    constructor_points = {}
    constructor_names  = {}

    for race in races:
        r_id = race["raceId"]

        # Check override
        cur.execute("""
            SELECT driverId, position, points
            FROM whatif_results
            WHERE scenario_id = %s
              AND raceId = %s
        """, (scenario_id, r_id))
        overridden = cur.fetchall()

        if overridden:
            for row in overridden:
                d_id = row["driverId"]
                pts = float(row["points"] or 0)
                c_sql = """
                    SELECT constructors.constructorId, constructors.name
                    FROM results
                    JOIN constructors ON results.constructorId = constructors.constructorId
                    WHERE results.raceId = %s
                      AND results.driverId = %s
                    LIMIT 1
                """
                cur.execute(c_sql, (r_id, d_id))
                c_info = cur.fetchone()
                if not c_info:
                    continue
                c_id = c_info["constructorId"]
                c_name = c_info["name"]
                constructor_points[c_id] = constructor_points.get(c_id, 0) + pts
                constructor_names[c_id] = c_name

        else:
            # Use real results
            cur.execute("""
                SELECT results.driverId, results.points, constructors.constructorId, constructors.name
                FROM results
                JOIN constructors ON results.constructorId = constructors.constructorId
                WHERE results.raceId = %s
            """, (r_id,))
            real_rows = cur.fetchall()
            for row in real_rows:
                c_id = row["constructorId"]
                pts = float(row["points"] or 0)
                constructor_points[c_id] = constructor_points.get(c_id, 0) + pts
                if c_id not in constructor_names:
                    constructor_names[c_id] = row["name"]

    # Build final array
    standings_array = []
    for c_id, pts in constructor_points.items():
        standings_array.append({
            "constructorId": c_id,
            "constructorName": constructor_names[c_id],
            "points": pts
        })
    standings_array.sort(key=lambda x: x["points"], reverse=True)

    cur.close()
    conn.close()
    return jsonify({
        "scenarioId": scenario_id,
        "season": season,
        "constructorStandings": standings_array
    })


if __name__ == '__main__':
    app.run(debug=True, port=8000)