import streamlit as st
import sys
import os
import json
import pandas as pd
from pathlib import Path

# Подключение модулей из корня проекта
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.db.db_manager import DBManager
from src.modes.crawler import CategoryCrawler
from src.modes.scraper import DataScraper
from src.utils.exporter import ExcelExporter

st.set_page_config(page_title="AI Data Miner", page_icon="⛏️", layout="wide")

@st.cache_resource
def get_db():
    return DBManager()

db = get_db()

def load_stats():
    with db.get_connection() as conn:
        tasks = conn.execute("SELECT status, COUNT(*) as cnt FROM crawler_tasks GROUP BY status").fetchall()
        items = conn.execute("SELECT status, COUNT(*) as cnt FROM scraper_items GROUP BY status").fetchall()
        
        stats = {
            'crawler': {'pending': 0, 'done': 0, 'error': 0},
            'scraper': {'pending': 0, 'processing': 0, 'done': 0, 'error': 0}
        }
        for row in tasks:
            stats['crawler'][row['status']] = row['cnt']
        for row in items:
            stats['scraper'][row['status']] = row['cnt']
            
        return stats

# --- UI Layout ---

st.title("⛏️ AI Data Miner Dashboard")

stats = load_stats()
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Очередь Категорий", stats['crawler']['pending'])
with col2:
    st.metric("Очередь Карточек", stats['scraper']['pending'])
with col3:
    st.metric("Успешно собрано", stats['scraper']['done'])
with col4:
    st.metric("Ошибок", stats['scraper']['error'])

st.divider()

# --- САЙДБАР ---
with st.sidebar:
    st.header("⚙️ Управление задачами")
    
    st.subheader("📥 Импорт ссылок")
    links_input = st.text_area("Вставь ссылки (по одной в строке):", height=100)
    col_import1, col_import2 = st.columns(2)
    with col_import1:
        if st.button("В Категории", width='stretch'):
            if links_input.strip():
                links = [line.strip() for line in links_input.split('\n') if line.strip()]
                for link in links: db.add_category_task(link)
                st.success(f"Добавлено {len(links)} шт.")
                st.rerun()
    with col_import2:
        if st.button("В Карточки", width='stretch', type="primary"):
            if links_input.strip():
                links = [line.strip() for line in links_input.split('\n') if line.strip()]
                db.add_scraper_items(source_url="manual_import", data_urls=links)
                st.success(f"Добавлено {len(links)} шт.")
                st.rerun()

    st.divider()
    
    st.subheader("Настройки парсинга")
    custom_fields_input = st.text_area("Кастомные поля (через запятую)", value="ofertant_name, ofertant_cif, autoritate_name, autoritate_cif")
    use_only_custom = st.checkbox("Искать ТОЛЬКО кастомные поля", value=True)
    
    # НОВЫЙ ЧЕКБОКС
    is_headless = st.checkbox("Скрытый режим (Headless)", value=True, help="Ускоряет работу, но повышает риск блокировки капчей.")
    
    st.divider()
    
    st.subheader("💾 Экспорт в Excel")
    export_name = st.text_input("Префикс файла", value="dataset")
    if st.button("📥 Выгрузить", width='stretch'):
        exporter = ExcelExporter()
        path = exporter.export_done_items(export_name)
        if path: st.success(f"Сохранено: {path.name}")
        else: st.warning("Нет данных.")

    st.divider()

    # === НОВЫЙ БЛОК: РЕЗЕРВНОЕ КОПИРОВАНИЕ И ОЧИСТКА ===
    st.subheader("🗄️ Резервная копия БД")
    
    # 1. Скачивание .db файла
    with open(db.db_path, "rb") as f:
        st.download_button(
            label="💾 Скачать всю базу (.db)",
            data=f,
            file_name="mining_state_backup.db",
            mime="application/octet-stream",
            width='stretch'
        )
    
    # 2. Загрузка и восстановление
    uploaded_db = st.file_uploader("📂 Восстановить базу", type=["db"])
    if uploaded_db is not None:
        if st.button("⚠️ Перезаписать текущую базу", type="primary", width='stretch'):
            with open(db.db_path, "wb") as f:
                f.write(uploaded_db.getbuffer())
            st.success("База успешно восстановлена!")
            st.rerun()

    # 3. Полная очистка
    if st.button("🗑 Очистить все очереди", width='stretch'):
        db.clear_all_queues()
        st.success("Все данные из очередей удалены!")
        st.rerun()
    # =====================================================


# --- ОСНОВНАЯ ПАНЕЛЬ ---
tab1, tab2, tab3 = st.tabs(["🚀 Запуск", "🧠 Кэш AI", "📋 Управление данными"])

with tab1:
    col_run1, col_run2 = st.columns(2)
    
    with col_run1:
        st.subheader("Режим 1: Crawler")
        pages_to_crawl = st.number_input("Страниц пагинации за запуск?", 1, 1000, 5)
        if st.button("▶️ Запустить Crawler", type="primary", use_container_width=True):
            
            # Динамические контейнеры для лайв-апдейта
            st.markdown("### 🔄 Лайв статус работы")
            live_stats = st.empty()
            live_logs = st.empty()
            log_history = []

            def crawler_callback(msg):
                log_history.append(msg)
                live_logs.code("\n".join(log_history[-15:]), language="text") # Храним только 15 последних строк
                
                # Перерисовка статистики базы данных
                s = load_stats()
                with live_stats.container():
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Очередь Категорий", s['crawler']['pending'])
                    c2.metric("Очередь Карточек", s['scraper']['pending'])
                    c3.metric("Успешно собрано", s['scraper']['done'])
                    c4.metric("Ошибок", s['scraper']['error'])

            with st.spinner("Работает... Для экстренной остановки нажми 'Stop' в правом верхнем углу экрана."):
                CategoryCrawler().run(max_pages_to_test=pages_to_crawl, headless=is_headless, ui_callback=crawler_callback)
            st.rerun()

    with col_run2:
        st.subheader("Режим 2: Scraper")
        items_to_scrape = st.number_input("Карточек за запуск?", 1, 1000, 10)
        if st.button("▶️ Запустить Scraper", type="primary", use_container_width=True):
            
            st.markdown("### 🔄 Лайв статус работы")
            live_stats = st.empty()
            live_logs = st.empty()
            log_history = []

            def scraper_callback(msg):
                log_history.append(msg)
                live_logs.code("\n".join(log_history[-15:]), language="json")
                
                s = load_stats()
                with live_stats.container():
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Очередь Категорий", s['crawler']['pending'])
                    c2.metric("Очередь Карточек", s['scraper']['pending'])
                    c3.metric("Успешно собрано", s['scraper']['done'])
                    c4.metric("Ошибок", s['scraper']['error'])

            with st.spinner("Работает... Для экстренной остановки нажми 'Stop' в правом верхнем углу экрана."):
                custom_fields = [f.strip() for f in custom_fields_input.split(",") if f.strip()]
                DataScraper().run(
                    custom_fields=custom_fields, 
                    max_items_to_test=items_to_scrape, 
                    use_only_custom=use_only_custom,
                    headless=is_headless,
                    ui_callback=scraper_callback
                )
            st.rerun()
            
with tab2:
    st.subheader("Управление селекторами (ai_configs)")
    st.info("Используй Raw JSON для массовой замены селекторов (копируй в VS Code, редактируй через Ctrl+D и возвращай обратно).")
    
    with db.get_connection() as conn:
        configs = pd.read_sql_query("SELECT domain, selectors_json FROM ai_configs", conn)
    
    if not configs.empty:
        for idx, row in configs.iterrows():
            domain = row['domain']
            with st.expander(f"🌐 Домен: {domain}", expanded=False):
                selectors_dict = json.loads(row['selectors_json'])
                
                # Создаем две вложенные вкладки внутри экспандера
                edit_tab1, edit_tab2 = st.tabs(["🗂️ Таблица", "📝 Raw JSON (Код)"])
                
                # --- Вкладка 1: Таблица ---
                with edit_tab1:
                    clean_dict = {k: (v if v is not None else "") for k, v in selectors_dict.items()}
                    df = pd.DataFrame(list(clean_dict.items()), columns=['Поле', 'Селектор'])
                    
                    edited_df = st.data_editor(
                        df, 
                        key=f"editor_tbl_{domain}", 
                        width='stretch',
                        disabled=["Поле"],
                        hide_index=True
                    )
                    
                    if st.button(f"💾 Сохранить таблицу", key=f"save_tbl_{domain}", type="primary"):
                        new_selectors = {}
                        for _, r in edited_df.iterrows():
                            val = r['Селектор']
                            # Безопасная проверка на пустые значения, чтобы избежать ошибок
                            if isinstance(val, str) and val.strip() != "":
                                new_selectors[r['Поле']] = val.strip()
                            else:
                                new_selectors[r['Поле']] = None
                                
                        db.update_ai_config(domain, new_selectors)
                        st.success("Таблица успешно сохранена!")
                        st.rerun()
                        
                # --- Вкладка 2: Raw JSON ---
                with edit_tab2:
                    # Форматируем JSON с отступами для удобного чтения и редактирования
                    raw_json_str = json.dumps(selectors_dict, indent=4, ensure_ascii=False)
                    edited_json_str = st.text_area(
                        "Редактор JSON", 
                        value=raw_json_str, 
                        height=400, 
                        key=f"editor_raw_{domain}"
                    )
                    
                    if st.button(f"💾 Сохранить JSON", key=f"save_raw_{domain}", type="primary"):
                        try:
                            # Проверяем, не нарушил ли ты синтаксис (запятые, кавычки)
                            parsed_json = json.loads(edited_json_str)
                            db.update_ai_config(domain, parsed_json)
                            st.success("JSON успешно сохранен!")
                            st.rerun()
                        except json.JSONDecodeError as e:
                            st.error(f"Ошибка синтаксиса JSON (проверь запятые и кавычки): {e}")

                st.divider()
                
                # Общая кнопка удаления кэша
                if st.button(f"🗑️ Удалить кэш домена", key=f"del_cache_{domain}"):
                    db.delete_ai_config(domain)
                    st.rerun()
    else:
        st.write("Кэш селекторов пока пуст.")
        
        
        
# === ОБНОВЛЕННАЯ ВКЛАДКА: УПРАВЛЕНИЕ ДАННЫМИ (УДАЛЕНИЕ) ===
with tab3:
    st.subheader("Редактирование записей (Scraper Items)")
    st.info("Поставь галочку в колонке 'Select' и нажми кнопку удаления снизу.")
    
    with db.get_connection() as conn:
        # Увеличили лимит до 5000 (или можешь вообще убрать LIMIT)
        latest_data = pd.read_sql_query(
            "SELECT id, data_url, status, extracted_data FROM scraper_items ORDER BY id DESC LIMIT 5000", 
            conn
        )
    
    if not latest_data.empty:
        expanded_rows = []
        # Собираем все возможные ключи из всех JSON, чтобы колонки не терялись
        all_keys = set()
        for _, row in latest_data.iterrows():
            if row['extracted_data']:
                try:
                    data = json.loads(row['extracted_data'])
                    all_keys.update(data.keys())
                except: pass

        for _, row in latest_data.iterrows():
            base = {'id': row['id'], 'status': row['status'], 'url': row['data_url']}
            # Инициализируем все колонки пустыми значениями
            for k in all_keys: base[k] = None
            
            if row['extracted_data']:
                try:
                    data = json.loads(row['extracted_data'])
                    base.update(data)
                except: pass
            expanded_rows.append(base)
            
        df = pd.DataFrame(expanded_rows)
        df.insert(0, "Select", False)
        
        edited_df = st.data_editor(
            df,
            hide_index=True,
            column_config={"Select": st.column_config.CheckboxColumn("Выбрать", default=False)},
            disabled=[col for col in df.columns if col != "Select"],
            use_container_width=True,
            height=600  # Сделали таблицу высокой
        )
        
        selected_rows = edited_df[edited_df["Select"] == True]
        selected_ids = selected_rows["id"].tolist()
        
        if selected_ids:
            st.warning(f"Выбрано строк для удаления: {len(selected_ids)}")
            if st.button("🗑️ Удалить выбранные строки", type="primary"):
                db.delete_scraper_items(selected_ids)
                st.success("Строки удалены!")
                st.rerun()
    else:
        st.write("База данных пока пуста.")