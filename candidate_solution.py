# candidate_solution.py
import sqlite3
import os
from fastapi import FastAPI, HTTPException
from typing import List, Optional
import uvicorn
import json
import http.client

# --- Constants ---
DB_NAME = "pokemon_assessment.db"


# --- Database Connection ---
def connect_db() -> Optional[sqlite3.Connection]:
    """
    Task 1: Connect to the SQLite database.
    Implement the connection logic and return the connection object.
    Return None if connection fails.
    """
    if not os.path.exists(DB_NAME):
        print(f"Error: Database file '{DB_NAME}' not found.")
        return None

    connection = None
    try:
        # --- Implement Here ---
        connection = sqlite3.connect(DB_NAME)
        # --- End Implementation ---
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

    return connection


# --- Data Cleaning ---
def clean_database(conn: sqlite3.Connection):
    """
    Task 2: Clean up the database using the provided connection object.
    Implement logic to:
    - Remove duplicate entries in tables (pokemon, types, abilities, trainers).
      Choose a consistent strategy (e.g., keep the first encountered/lowest ID).
    - Correct known misspellings (e.g., 'Pikuchu' -> 'Pikachu', 'gras' -> 'Grass', etc.).
    - Standardize casing (e.g., 'fire' -> 'Fire' or all lowercase for names/types/abilities).
    """
    if not conn:
        print("Error: Invalid database connection provided for cleaning.")
        return

    cursor = conn.cursor()
    print("Starting database cleaning...")

    try:
        # --- Implement Here ---                

        sql = """
--fix & standardise spelling 
update pokemon set name = 'Pikachu' where id = 18;
update pokemon set name = 'Charmander' where id = 19;
update pokemon set name = 'Bulbasaur' where id = 20;
update pokemon set name = 'Rattata' where id = 22;

update types set name = 'Fire' where id = 18;
update types set name = 'Grass' where id = 22;
update types set name = 'Water' where id = 19;
update types set name = 'Poison' where id = 23;

update trainers set name = 'Misty' where id = 6;

update abilities set name = 'Static' where id = 11;
update abilities set name = 'Overgrow' where id = 12;

--update links
update trainer_pokemon_abilities set trainer_id = 1 where trainer_id = 5;
update trainer_pokemon_abilities set ability_id = 1 where ability_id = 12;
update trainer_pokemon_abilities set pokemon_id = 11 where pokemon_id = 21;
update trainer_pokemon_abilities set pokemon_id = 11 where pokemon_id = 21;
update trainer_pokemon_abilities set pokemon_id = 9 where pokemon_id in (17,18);

update pokemon set type1_id = 1 where type1_id = 24;
update pokemon set type1_id = 1 where type1_id = 24;

--delete messy data
delete from abilities where id = 14;
delete from types where id in (17,20,21);
"""

        tables = ['abilities', 'pokemon', 'trainers', 'types']        
        
        for table in tables:
            sql += f"""
            --delete duplicate records from {table}
            WITH {table}_tmp AS 
            (
                SELECT *, ROW_NUMBER() 
                OVER (PARTITION BY name ORDER BY name) AS Row_Number
                FROM {table}
            )
            DELETE FROM {table} where id IN (SELECT id FROM {table}_tmp WHERE Row_Number <> 1); \r\n"""  
            
        #print(sql);
            
        cursor.executescript(sql)
        
        # --- End Implementation ---
        conn.commit()
        print("Database cleaning finished and changes committed.")

    except sqlite3.Error as e:
        print(f"An error occurred during database cleaning: {e}")
        conn.rollback()  # Roll back changes on error

# --- FastAPI Application ---
def create_fastapi_app() -> FastAPI:
    """
    FastAPI application instance.
    Define the FastAPI app and include all the required endpoints below.
    """
    print("Creating FastAPI app and defining endpoints...")
    app = FastAPI(title="Pokemon Assessment API")

    # --- Define Endpoints Here ---
    @app.get("/")
    def read_root():
        """
        Task 3: Basic root response message
        Return a simple JSON response object that contains a `message` key with any corresponding value.
        """
        # --- Implement here ---

        return { "message": "read_root" }

        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str):
        """
        Task 4: Retrieve all Pokémon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here --- 

        try:
            db = connect_db();

            sql = f"""select p.Name from pokemon p
                    inner join trainer_pokemon_abilities tpa on p.id = tpa.pokemon_id
                    inner join abilities a on a.id = tpa.ability_id
                    where a.name = ?
                    """
            
            cursor = db.cursor();
            cursor.execute(sql, (ability_name,));
            res = cursor.fetchall();
            mapped = [row[0] for row in res]

            return mapped;
        except Exception as e:
            print(f"Unexpected error: {e}")
            return [];

        # --- End Implementation ---

    @app.get("/pokemon/type/{type_name}", response_model=List[str])
    def get_pokemon_by_type(type_name: str):
        """
        Task 5: Retrieve all Pokémon names of a specific type (considers type1 and type2).
        Query the cleaned database. Handle cases where the type doesn't exist.
        """
        # --- Implement here ---

        try:
            db = connect_db();

            sql = f"""select p.Name from pokemon p
                    inner join types t on t.id = p.type1_id or t.id = p.type2_id
					where t.name = ?
                    """
            
            cursor = db.cursor();
            cursor.execute(sql, (type_name,));
            res = cursor.fetchall();
            mapped = [row[0] for row in res]

            return mapped;
        except Exception as e:
            print(f"Unexpected error: {e}")
            return [];
    
        # --- End Implementation ---

    @app.get("/trainers/pokemon/{pokemon_name}", response_model=List[str])
    def get_trainers_by_pokemon(pokemon_name: str):
        """
        Task 6: Retrieve all trainer names who have a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist or has no trainer.
        """
        # --- Implement here ---

        try:
            db = connect_db();

            sql = f"""select t.Name from trainers t
                    inner join trainer_pokemon_abilities tpa on t.id = tpa.trainer_id
                    inner join pokemon p on p.id = tpa.pokemon_id
                    where p.name = ?
                    """
            
            cursor = db.cursor();
            cursor.execute(sql, (pokemon_name,));
            res = cursor.fetchall();
            mapped = [row[0] for row in res]

            return mapped;
        except Exception as e:
            print(f"Unexpected error: {e}")
            return [];

        # --- End Implementation ---

    @app.get("/abilities/pokemon/{pokemon_name}", response_model=List[str])
    def get_abilities_by_pokemon(pokemon_name: str):
        """
        Task 7: Retrieve all ability names of a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist.
        """
        # --- Implement here ---
        try:
            db = connect_db();

            sql = f"""select a.Name from abilities a
                    inner join trainer_pokemon_abilities tpa on a.id = tpa.ability_id
                    inner join pokemon p on p.id = tpa.pokemon_id
                    where p.name = ?
                    """
            
            cursor = db.cursor();
            cursor.execute(sql, (pokemon_name,));
            res = cursor.fetchall();
            mapped = [row[0] for row in res]

            return mapped;
        except Exception as e:
            print(f"Unexpected error: {e}")
            return [];
        # --- End Implementation ---

    # --- Implement Task 8 here ---
    @app.get("/pokemon/create/{pokemon_name}", response_model=int)
    def create_pokemon(pokemon_name: str):       
        
        try:
            baseUrl = "pokeapi.co"
            conn = http.client.HTTPSConnection(f"{baseUrl}")
            conn.request("GET", f"/api/v2/pokemon/{pokemon_name}")

            response = conn.getresponse()
            if(response.status == 404): raise HTTPException(status_code=400, message="Pokemon not found");

            data = response.read().decode('utf-8')
            jsonData = json.loads(data);
            conn.close();

            db = connect_db()
            cursor = db.cursor();

            sql = "select id from pokemon where name = ?"
            cursor.execute(sql, (pokemon_name,));
            pokemonid = cursor.fetchone();

            if(pokemonid): raise HTTPException(status_code=400, message="Pokemon already created");
            
            # handle types
            typeids = [];

            for typeData in jsonData["types"]:
                typeName = typeData["type"]["name"].replace("-", " ");
                sql = "select id from types where name = ?"
                cursor.execute(sql, (typeName,))
                typeid:int = cursor.fetchone()

                if(typeid != ()): typeids.append(typeid[0]);
                else:
                    sql = "insert into types (name) values(?);"
                    cursor.execute(sql, (typeName,))                    
                    typeids.append(cursor.lastrowid);
            
            # create pokemon
            if(len(typeids) == 1):
                sql = "insert into pokemon (name, type1_id) values(?,?);"
                params = (pokemon_name.title(),typeids[0]);
            
            if(len(typeids) > 1):
                sql = "insert into pokemon (name, type1_id, type2_id) values(?,?,?);"
                params = (pokemon_name.title(),typeids[0],typeids[1])

            cursor.execute(sql, params);
            pokemonid = cursor.lastrowid;            

            # handle ablities
            for abilityData in jsonData["abilities"]:
                abilityName = abilityData["ability"]["name"].replace("-", " ").title();
                sql = "select id from abilities where name = ?";
                cursor.execute(sql, (abilityName,))
                abilityid = cursor.fetchone();

                if(abilityid): print(abilityid)
                else:                     
                    sql = "insert into abilities (name) values(?);"
                    cursor.execute(sql, (abilityName,))
                    abilityid = cursor.lastrowid;

                # link to trainer
                sql = f"""insert into trainer_pokemon_abilities (pokemon_id, trainer_id, ability_id) values(?, (select id FROM trainers ORDER BY RANDOM() LIMIT 1),?);"""
                cursor.execute(sql, (pokemonid, abilityid));
            
            db.commit();

            return pokemonid;
        except Exception as e:
            db.rollback();
            print(f"Unexpected error: {e}")
            raise e
    # --- End Implementation ---

    print("FastAPI app created successfully.")
    return app


# --- Main execution / Uvicorn setup (Optional - for candidate to run locally) ---
if __name__ == "__main__":
    # Ensure data is cleaned before running the app for testing
    temp_conn = connect_db()
    if temp_conn:
        clean_database(temp_conn)
        temp_conn.close()

    app_instance = create_fastapi_app()
    uvicorn.run(app_instance, host="127.0.0.1", port=8000)
