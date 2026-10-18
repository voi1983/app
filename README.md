# Google Drive Excel Price Parser

Python-скрипт, який автоматично збирає прайс-листи постачальників з папки Google Drive,
розбирає їх і зводить у одну уніфіковану Excel-таблицю.

## Що робить

1. Авторизується в Google Drive та Google Sheets API через сервісний акаунт.
2. Знаходить усі `.xlsx`-файли у вказаній папці Google Drive.
3. Завантажує кожен файл і розбирає його вміст:
   - знаходить блоки товарів за рядком `Модель:` / `Назва моделі`;
   - автоматично визначає таблицю за заголовками (`Розмір`, `Оптова ціна`, `Роздрібна ціна`,
     акційні ціни тощо), навіть якщо порядок і назви стовпців відрізняються між файлами;
   - пропускає порожні та описові рядки;
   - якщо в заголовку акційної ціни вказана знижка у відсотках, розраховує акційну ціну автоматично.
4. Зберігає зведені дані у файл `result.xls` з форматуванням цін.

### Формат результату

| Model_rez | Size_rez | Price_Opt | Price_Opt_Akciya | Price_Rozn | Price_Rozn_Akcia |
|-----------|----------|-----------|------------------|------------|------------------|

Приклад вихідного файлу — [`result.xls`](result.xls).

## Технології

- Python 3
- Google Drive API, Google Sheets API (`google-api-python-client`, `gspread`, `oauth2client`)
- `openpyxl`, `pandas`

## Запуск

```bash
pip install -r requirements.txt
```

1. Створіть сервісний акаунт у Google Cloud Console, увімкніть Google Drive API та Google Sheets API,
   завантажте ключ і збережіть його як `google_excel/credentials.json`.
2. Надайте сервісному акаунту доступ до папки Google Drive з прайсами.
3. Створіть `secret_info.json` за зразком [`secret_info.example.json`](secret_info.example.json).
4. Запустіть скрипт з кореня репозиторію:

```bash
python google_excel/main.py
```

Файли `credentials.json` і `secret_info.json` додані в `.gitignore` і не потрапляють у репозиторій.

## Структура

```
.
├── google_excel/
│   └── main.py               # основний скрипт
├── result.xls                # приклад результату
├── requirements.txt
└── secret_info.example.json  # шаблон налаштувань
```
