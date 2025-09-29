from typing import Type
from pydantic import BaseModel, Field
import requests
from bs4 import BeautifulSoup
import json
import argparse

#Created by Syed

class UserParameters(BaseModel):
    pass

class ToolParameters(BaseModel):
    website: str = Field(description="The website URL to scrape banking product info from")

def run_tool(config: UserParameters, args: ToolParameters):
    website = args.website

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
    }

    try:
        response = requests.get(website, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        data = []
        for section in soup.find_all(['h1', 'h2', 'h3', 'p', 'li']):
            text = section.get_text(strip=True)
            if any(keyword in text.lower() for keyword in ['loan', 'credit card', 'interest', '%', 'offer', 'cashback']):
                data.append(text)

        unique_data = list(dict.fromkeys(data))
        return unique_data

    except requests.exceptions.RequestException as e:
        return [f"An error occurred while scraping: {e}"]

OUTPUT_KEY = "tool_output"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user-params", required=True, help="JSON string for user params")
    parser.add_argument("--tool-params", required=True, help="JSON string for tool arguments")
    args = parser.parse_args()

    config = UserParameters(**json.loads(args.user_params))
    params = ToolParameters(**json.loads(args.tool_params))
    output = run_tool(config, params)
    print(OUTPUT_KEY, output)
