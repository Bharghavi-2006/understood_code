import os #python module to interact with the operating system
import json #python module which provides functionalities for working with JSON data
from sqlalchemy import create_engine, text #ORM 
import dotenv #loads environment variables from a .env file

dotenv.load_dotenv()
"""
.get_key() is a function from the python-dotenv package. It opens the .env file.
Extracts the value associated with the DB_USERNAME key and assigns that value to the variable username. 
"""
username = dotenv.get_key(".env", "DB_USERNAME")
password = dotenv.get_key(".env", "DB_PASSWORD")
name = dotenv.get_key(".env", "DB_NAME")

"""
This function uses a formatted string(f-string) to plug in database credentials and form a connection URL.
create_engine() is a function from sqlalchemy that creates a reusable connection object. 
What this means is it gives a connection from the connection pool, let's me run a query and return it safely instead of making a new connection every time and closing it after use.
What the connection URL means is connect to the PostgreSQL database using the psycopg2 driver, log in with {username}:{password}, connect to the database at host IP address and use the database named {db_name}
"""
engine = create_engine(f"postgresql+psycopg2://{username}:{password}@35.238.0.91:5432/{name}")
OUTPUT_DIR = "output"

with engine.connect() as conn:
    for root, dirs, files in os.walk(OUTPUT_DIR): #os.walk() generates the file names in a directory tree by walking the tree. Each directory in the tree yields a 3-tuple(dirpath, dirnames, filenames)
        for file in files:
            if not file.endswith(".json"): #if filenmae does not end with .json
                continue

            print(f"\n Fixing chapter info for file: {file}")
            parts = root.split(os.sep) #splitting the full directory path into a list of folder names ['output', 'Class_5', 'science']
            if len(parts) < 3:
                print("Skipping due to bad path:", root)
                continue

            class_name = parts[1] #second element in the array assigned value of the variable class_name
            subject = parts[2] #third element in the array assigned value of the variable subject
            grade = class_name.split("_")[1] #splitting class name based on where "_" is and the second element of the array the number of class is assigned as the value to grade variable
            chapter_name = file.replace(".json", "") #replacing ".json" part of the filename with an empty string
            file_path = os.path.join(root, file) #constructing the full absolute/relative path to the file

            try:
                with open(file_path, 'r', encoding='utf-8') as f: #opening file in read-only mode while making sure special characters are read properly too
                    questions = json.load(f) #parse a JSON document from a file and convert it into a Python object like a dictionary or a list
            except Exception as e:
                print(f"Failed to read file {file}: {e}")
                continue

            for q in questions:
                question_text = q.get("question", "").strip() #iterates over the list of dictionaries. Each q is a single question dictionary from the JSON file, q.get() gets the question key from the dictionary

                try:
                    result = conn.execute(text("""
                        UPDATE "public"."Extracted_Questions" 
                        SET "Chapter" = :chapter_name, "Topic" = :chapter_name
                        WHERE TRIM("Question_text") = :question_text
                            AND "Grade" = :grade
                            AND "Subject" = :subject;
                    """), {
                        "chapter_name": chapter_name,
                        "question_text": question_text,
                        "grade": grade,
                        "subject": subject
                    })
                    if result.rowcount:
                        print(f"Updated: {question_text[:60]}...")  #result.rowcount tells how many rows were updated
                    else:
                        print(f"Not found: {question_text[:60]}...")
                except Exception as e:
                    print(f"Update failed: {e}")

