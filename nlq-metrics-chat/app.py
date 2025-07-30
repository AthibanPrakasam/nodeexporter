from fastapi import FastAPI, Query
from pydantic import BaseModel
from promql_helper import query_victoriametrics, summarize_response
import os
import openai
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
openai.api_key = os.getenv("OPENAI_API_KEY")

class QueryInput(BaseModel):
    question: str

@app.post("/nlq/")
async def natural_language_query(input: QueryInput):
    user_question = input.question

    # Step 1: Use OpenAI to convert to PromQL
    prompt = f"""You are a Prometheus expert. Convert this natural language question to PromQL:
    
    Question: "{user_question}"
    PromQL:"""

    try:
        completion = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        promql = completion.choices[0].message.content.strip()

        # Step 2: Run the query
        raw_response = query_victoriametrics(promql)

        # Step 3: Summarize
        summary = summarize_response(raw_response)

        return {
            "question": user_question,
            "promql": promql,
            "summary": summary,
            "raw": raw_response
        }

    except Exception as e:
        return {"error": str(e)}
