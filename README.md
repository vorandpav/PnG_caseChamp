# PnG_caseChamp

Скрипт `data_to_sheet.py` парсит txt-файлы, которые сохраняет bookmarklet из `page_parser.js`, и собирает таблицу с полями:

- `категория_товара`
- `бренд`
- `категория_вопроса`
- `запрос`
- `текст_ответа`
- `ссылки_в_тексте`
- `сайты_в_тексте` 
- `ссылки_из_блока_источников` 
- `сайты_из_блока_источников` 

Имя папки-источника метаданных: `категория_бренд_категорияВопроса`.

Пример структуры:

```text
in/
  гигиена_Pampers_брендовые/
    q1.txt
    q2.txt
  уход_OralB_инфо/
    q3.txt
```

## Установка

```powershell
py -m pip install -r requirements.txt
```

## Быстрый запуск

CSV:

```powershell
py data_to_sheet.py --input in --output out/result.csv
```

XLSX:

```powershell
py data_to_sheet.py --input in --output out/result.xlsx
```

JSON:

```powershell
py data_to_sheet.py --input in --output out/result.json
```
