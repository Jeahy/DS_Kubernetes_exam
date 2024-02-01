from fastapi import FastAPI, HTTPException
from typing import Optional
from pydantic import BaseModel
import pandas as pd

# Dictionary for user credentials
users_db = {
    "alice": "wonderland",
    "bob": "builder",
    "clementine": "mandarine",
    "admin": "4dm1N"
}

api = FastAPI(
    title='My API',
    description="API to update and create multiple choice question catalogues",
    version="1.0.1")

# Load excel into dataframe
file_path = 'questions_en.xlsx'
df = pd.read_excel(file_path)

# Drop rows with missing values in the "correct" column
df = df.dropna(subset=['correct'])


# Number of questions
def validate_num_questions(num_questions):
    valid_values = [5, 10, 20]
    if num_questions not in valid_values:
        raise HTTPException(status_code=400, detail="Invalid number of questions. Choose from 5, 10, or 20.")

# Function to verify user credentials
def verify_user(username: str, password: str):
    if username not in users_db or users_db[username] != password:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return username

# Create a new question (admin only)
class NQ(BaseModel):
    """structure of individual questions contained in database
    """
    question: str
    subject: str
    use: str
    correct: str
    responseA: str
    responseB: str
    responseC: Optional[str] = None
    responseD: Optional[str] = None
    remark: Optional[str] = None


# Verify that the API is functional
@api.get("/status")
def get_status():
    """checks whether API is functional
    """
    return {"status": "API is functional"}


# Admin adds new question to the database
@api.post("/questions/admin")
def create_question( username: str, password: str, new_question: NQ):
    """admin can add new questions to database
    """
    global df  # Declare df as a global variable
    # Verify user credentials
    user = verify_user(username, password)
    if user != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    # Append the new question to the dataframe
    df = df._append(new_question.dict(), ignore_index=True)
    
    return {"message": "Question created successfully"}



# Get random questions
@api.get("/questions/")
def get_questions(
    username: str, password: str, use: str, subjects: str, num_questions: int = 5
):
    """creates questionnaire with multiple choice questions
    """
    # Verify user credentials
    verify_user(username, password)

    # Validate the number of questions
    validate_num_questions(num_questions)
    
    # Filter questions based on user preferences
    filtered_df = df[(df['use'] == use) & df['subject'].isin(subjects.split(','))]

    # Check if there are enough questions
    if len(filtered_df) < num_questions:
        raise HTTPException(status_code=400, detail="Not enough questions available.")

    # Randomly select questions
    random_questions = filtered_df.sample(n=num_questions, random_state=1)

    # Format the response
    response_data = []
    for _, question in random_questions.iterrows():
        answers = [
            question["responseA"],
            question["responseB"],
        ]
        if pd.notna(question["responseC"]) and question["responseC"] != "":
            answers.append(question["responseC"])
        if pd.notna(question["responseD"]) and question["responseD"] != "":
            answers.append(question["responseD"])

        response_data.append({
            "subject": question["subject"],
            "question": question["question"],
            "answers": answers,
            "correct": question["correct"],
        })

    return {"questions": response_data}
