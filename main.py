import pickle

import requests
from concurrent.futures import ThreadPoolExecutor
import re
import pandas as pd

from tqdm import tqdm

__API_BASE_URL = "https://api.hh.ru/vacancies/"
__DICT_KEYS = (
    "Ids",
    "Employer",
    "Name",
    "Salary",
    "From",
    "To",
    "Experience",
    "Schedule",
    "Keys",
    "Description",
)
_rates = {"USD": 0.01187, "EUR": 0.01067, "RUR": 1.00000}


def __convert_gross(is_gross: bool) -> float:
    return 0.87 if is_gross else 1


def clean_tags(html_text: str) -> str:
    pattern = re.compile("<.*?>")
    return re.sub(pattern, "", html_text)


def get_vacancy(vacancy_id: str):
    # Get data from URL
    url = f"{__API_BASE_URL}{vacancy_id}"
    vacancy = requests.get(url).json()

    # Extract salary
    salary = vacancy.get("salary")

    # Calculate salary:
    # Get salary into {RUB, USD, EUR} with {Gross} parameter and
    # return a new salary in RUB.
    from_to = {"from": None, "to": None}
    if salary and salary["currency"] in _rates:
        is_gross = vacancy["salary"].get("gross")
        for k, v in from_to.items():
            if vacancy["salary"][k] is not None:
                _value = __convert_gross(is_gross)
                from_to[k] = int(_value * salary[k] / _rates[salary["currency"]])

    # Create pages tuple
    return (
        vacancy_id,
        vacancy.get("name", ""),
        vacancy.get("employer", {}).get("name", ""),
        salary is not None,
        from_to["from"],
        from_to["to"],
        vacancy.get("experience", {}).get("name", ""),
        vacancy.get("schedule", {}).get("name", ""),
        [el["name"] for el in vacancy.get("key_skills", [])],
        clean_tags(vacancy.get("description", "")),
    )


def collect_vacancies(page_limit=None):
    target_url = __API_BASE_URL

    if page_limit:
        num_pages = page_limit
    else:
        num_pages = requests.get(target_url).json()["pages"]

    ids = []
    for idx in tqdm(range(num_pages + 1), desc="Collecting vacancy IDs", ncols=100):
        response = requests.get(target_url, {"page": idx})
        data = response.json()
        if "items" not in data:
            break
        ids.extend(x["id"] for x in data["items"])

    jobs_list = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for vacancy in tqdm(
                executor.map(get_vacancy, ids),
                desc="Get data via HH API",
                ncols=100,
                total=len(ids),
        ):
            jobs_list.append(vacancy)

    unzipped_list = list(zip(*jobs_list))

    result = {}
    for idx, key in enumerate(__DICT_KEYS):
        result[key] = unzipped_list[idx]

    return result


if __name__ == '__main__':
    vacancies = collect_vacancies(30)
    df = pd.DataFrame(vacancies)
    df.to_csv('vacancies.csv')
    print(vacancies)
