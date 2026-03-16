import asyncio
import json
import aiohttp
import pandas as pd

API_KEYS = []
MODEL = "mistral-large-latest"
API_URL = "https://api.mistral.ai/v1/chat/completions"
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 3
PER_KEY_DELAY_SECONDS = 0.5


def build_payload(row_dict):
    brand = row_dict["бренд"]
    q = row_dict["запрос"]
    a = row_dict["текст_ответа"]
    sources = row_dict.get("сайты_из_блока_источников", "")

    prompt = f"Вопрос: {q}\nОтвет: {a}\nСайты-источники: {sources}"

    system_instr = f"""Ты - аналитик LLM-выдачи, работающий на стороне P&G. Твоя задача - оценить, как Яндекс Нейро отвечает на вопросы, связанные с брендом {brand}

ВАЖНО: P&G - это холдинг. Все бренды из P&G и НЕ являются конкурентами друг другу

Верни строго JSON объект без markdown-разметки

Поля JSON:

ВОПРОС
q_brand_direct (int): 1 если название бренда {brand} явно упомянуто в тексте вопроса, 0 если вопрос общий
q_comp_direct (int): 1 если в вопросе упомянут конкурент не из P&G, 0 иначе

ПОЗИЦИЯ БРЕНДА В ОТВЕТЕ
a_pg_rank (int): позиция, на которой бренд {brand} ВПЕРВЫЕ упоминается в ответе. 0 если бренд не упомянут вообще
a_is_winner (int): 1 если {brand} назван лучшим выбором, победителем или рекомендованным вариантом, 0 иначе
a_pg_portfolio_count (int): сколько различных суббрендов/линеек P&G из портфеля упомянуто в ответе

КОНКУРЕНТЫ (только НЕ P&G бренды)
a_comp_count (int): количество уникальных брендов-конкурентов (не P&G) упомянутых в ответе
a_main_competitor (str): название самого заметного конкурента не из P&G (упоминается первым или чаще всего). "none" если конкурентов нет
a_comp_is_winner (int): 1 если конкурент (не P&G) назван лучшим вариантом в ответе, 0 иначе

КАЧЕСТВО И ТОН ОТВЕТА
a_sentiment (int): тональность ответа по отношению к {brand}: 1 = позитивный (хвалят, рекомендуют), 0 = нейтральный (факты без оценки), -1 = негативный (критика, предупреждения)
a_has_tech (int): 1 если в ответе описаны конкретные технологии, материалы или состав продукта {brand}, 0 иначе
a_has_price (int): 1 если упомянуты цены, скидки, ценовой сегмент или конкретные места продаж, 0 иначе
a_buy_cta (int): 1 если ответ содержит призыв к покупке или явную рекомендацию купить конкретный продукт, 0 иначе
a_comparison_type (str): тип сравнения в ответе — "внутри_pg" (только суббренды P&G), "с_конкурентом" (с внешними брендами), "смешанное" (и те и те), "нет_сравнения" (продукт рассматривается один)

ИСТОЧНИКИ
a_source_trust (int): доминирующий тип источников в блоке источников: 1 = маркетплейсы, 2 = официальные сайты брендов, 3 = обзоры/статьи/блоги, 4 = форумы/Q&A, 5 = аптеки, 6 = смешанные
"""

    return {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_instr},
            {"role": "user", "content": prompt},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
    }


async def call_mistral_async(session, payload, api_key):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.post(
                    API_URL,
                    headers={"Authorization": f"Bearer {api_key}"},
                    json=payload,
            ) as response:
                response_text = await response.text()
                if response.status in {429, 500, 502, 503, 504}:
                    raise RuntimeError(f"Transient HTTP {response.status}: {response_text[:200]}")
                response.raise_for_status()
                return json.loads(response_text)
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, RuntimeError):
            if attempt == MAX_RETRIES:
                raise
            await asyncio.sleep(2 ** (attempt - 1))


async def analyze_row_async(row_dict, api_key, session):
    brand = row_dict["бренд"]
    payload = build_payload(row_dict)

    try:
        data = await call_mistral_async(session, payload, api_key)
        raw = data["choices"][0]["message"]["content"]
        parsed = json.loads(raw)
        flat = {}
        for k, v in parsed.items():
            if isinstance(v, dict):
                flat.update(v)
            else:
                flat[k] = v
        return flat
    except Exception as e:
        print(f"Error on brand {brand}: {e}")
        return None


async def process_one_row(position, row_idx, row_dict, session, key_locks):
    key_idx = position % len(API_KEYS)
    api_key = API_KEYS[key_idx]

    async with key_locks[key_idx]:
        query_preview = str(row_dict.get("запрос", ""))[:60]
        print(f"[key {key_idx + 1}] Processing {position + 1}: {query_preview}...")
        analysis = await analyze_row_async(row_dict, api_key, session)
        await asyncio.sleep(PER_KEY_DELAY_SECONDS)

    if analysis:
        return row_idx, {**row_dict, **analysis}
    return row_idx, row_dict


async def run_batch_async(df):
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    key_locks = [asyncio.Lock() for _ in API_KEYS]

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [
            asyncio.create_task(process_one_row(position, row_idx, row.to_dict(), session, key_locks))
            for position, (row_idx, row) in enumerate(df.iterrows())
        ]

        results_by_idx = {}
        for task in asyncio.as_completed(tasks):
            row_idx, result_row = await task
            results_by_idx[row_idx] = result_row

    return [results_by_idx[row_idx] for row_idx in df.index]


def main():
    df = pd.read_csv("out/result.csv")
    ordered_results = asyncio.run(run_batch_async(df))

    final_df = pd.DataFrame(ordered_results)
    final_df.to_csv("out/llm_result.csv", index=False)
    print(f"\nDone: {len(final_df)} rows -> out/llm_result.csv")


if __name__ == "__main__":
    main()
