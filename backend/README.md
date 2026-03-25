# CONEXIAI (prototype)

## Что делает
Мини-приложение на FastAPI:
1) регистрация/логин через Supabase Auth  
2) создание `companies` и `employees` в Supabase  
3) кнопка `Найти риски` -> создание `risk_runs` в Supabase -> POST в webhook n8n  
4) дэшборд читает результаты из Supabase (что записывает n8n)
5) кнопка `Сообщить CEO (email)` -> POST в webhook n8n

## Подготовка Supabase
Выполни SQL: `backend/supabase/schema.sql`

## Переменные окружения
Создай файл: `backend/.env`
Пример:
```bash
APP_NAME="CONEXIAI"

SUPABASE_URL="https://<project-ref>.supabase.co"
SUPABASE_ANON_KEY="<anon key>"

N8N_FIND_RISKS_WEBHOOK_URL="https://n8n.maxinum.kz/webhook/<find-risks-webhook>"
N8N_CEO_EMAIL_WEBHOOK_URL="https://n8n.maxinum.kz/webhook/<ceo-email-webhook>"

SESSION_COOKIE_SECURE="false"
SESSION_ACCESS_COOKIE_NAME="sb_access_token"
SESSION_REFRESH_COOKIE_NAME="sb_refresh_token"
```

Важно: `SUPABASE_ANON_KEY` можно хранить в клиентской части/сервере.
`service_role` ключ не хранится в этом приложении, он нужен только внутри n8n для записи результатов анализа в `risk_runs`.

## Запуск
```bash
# внутри backend/
./../.venv/bin/python -m uvicorn app.main:app --reload --port 8000
```

Открой: `http://localhost:8000`

